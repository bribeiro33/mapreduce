"""MapReduce framework Manager node."""
import os
from pathlib import Path
from queue import Queue
import shutil
import socket
import string
import threading
import tempfile
import logging
import json
import time
import click
import mapreduce.utils


# Configure logging
LOGGER = logging.getLogger(__name__)


class WTag:
    """A class representing a Worker node's identifying info."""

    # TODO: add job parameter later as needed
    def __init__(self, host_in, port_in, state_in, time_in):
        """Construct a WTag instance"""
        self.host = host_in
        self.port = port_in
        self.state = state_in
        self.last_checkin = time_in
        self.task = None

    def placeholder_func(self):
        """do nothing pretty much, placeholder func for styling"""
        return self.task


class Job:
    """A class representing a Job task"""

    # dont hate me im turning this into a dictionary

    def __init__(self, id_in, dict_in):
        """Construct a Job instance"""
        self.job_id = id_in
        self.job_dict = {}
        self.job_dict["input_dir"] = dict_in["input_directory"]
        self.job_dict["output_dir"] = dict_in["output_directory"]
        self.job_dict["mapper_exec"] = dict_in["mapper_executable"]
        self.job_dict["reducer_exec"] = dict_in["reducer_executable"]
        self.job_dict["num_mappers"] = dict_in["num_mappers"]
        self.job_dict["num_reducers"] = dict_in["num_reducers"]
        self.job_dict["is_completed"] = False
 

    def placeholder_func(self):
        """do nothing pretty much, placeholder func for styling"""
        return self.job_dict


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""

        LOGGER.info("Starting manager:%s", port,)
        LOGGER.info("PWD %s", os.getcwd())

        # Member vars
        self.options = {
            "host": host,
            "port": port
        }
        self.shutdown = False
        self.workers = {}

        self.curr_job_id = 0
        self.job_queue = Queue()
        self.is_executing_job = False
        self.running_process = "neither"  # or "mapping" of "reducing"
        self.tmpdir = None
        self.task_list = []

        self.job_thread = threading.Thread(target=self.run_job)
        self.tcp_thread = threading.Thread(target=self.tcp_listening)
        self.udp_thread = threading.Thread(target=self.udp_listening)
        self.hb_thread = threading.Thread(target=self.worker_death_check)

        self.job_thread.start()
        self.tcp_thread.start()
        self.udp_thread.start()
        self.hb_thread.start()

        self.job_thread.join()
        self.tcp_thread.join()
        self.udp_thread.join()
        self.hb_thread.join()

        self.shutdown_workers()

    def tcp_listening(self):
        """Wait on a message from a socket OR a shutdown signal."""
        # Create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.options["host"], self.options["port"]))
            sock.listen()
            sock.settimeout(1)

            while not self.shutdown:
                # Wait for a connection for 1s.  The socket library avoids
                # consuming CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue

                # Socket recv() will block for a maximum of 1 second.
                # If you omit this, it blocks indefinitely, waiting for packets.
                clientsocket.settimeout(1)

                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)

                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")

                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue

                # Handle messages
                if message_dict["message_type"] == "shutdown":
                    self.shutdown_manager(message_dict)

                elif message_dict["message_type"] == "register":
                    self.register_worker(message_dict)

                elif message_dict["message_type"] == "new_manager_job":
                    self.new_manager_job(message_dict)

                elif message_dict["message_type"] == "finished":
                    self.finished_worker(message_dict)

    def udp_listening(self):
        """Bind the socket and recieve UDP messages."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Bind the UDP socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.options['host'], self.options['port']))
            sock.settimeout(1)
            # No sock.listen() since UDP doesn't establish connections like TCP
            # Receive incoming UDP messages
            while not self.shutdown:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)

				# Update the worker's time when recieved new message
                curr_time = time.time()
                for worker in self.workers.values():
                    if (worker.port == message_dict["worker_port"] and
                        worker.host == message_dict["worker_host"]):
                        worker.last_checkin = curr_time


    def worker_death_check(self):
        """Checking for workers who haven't reported in >10 seconds."""
        while not self.shutdown:
            time.sleep(0.1)
            curr_time = time.time()
            for worker in self.workers.values():
                if worker.state != "dead" and \
                    curr_time - worker.last_checkin >= 10:
                    if worker.state == "busy":
                        self.task_list.append(worker.task)
                        worker.task = None
                    # All workers (busy, ready, dead) marked as dead here
                    worker.state = "dead"


    def shutdown_manager(self, message_dict):
        # if shutdown message has been received, will not go back into
        # while loop and will effectively, well, shutdown
        self.shutdown = True
        LOGGER.debug("recieved\n%s", json.dumps(message_dict, indent=2))
        LOGGER.info("shutting down")


    def register_worker(self, message_dict):
        # Worker is ready to listen
        LOGGER.debug(
            "recieved\n%s",
            json.dumps(message_dict, indent=2),
        )
        worker_host = message_dict["worker_host"]
        worker_port = message_dict["worker_port"]
        # If worker has already registered in the past, change to ready state
        # and check if had any tasks registered to it
        possible_worker = self.workers.get((worker_host, worker_port))
        if possible_worker:
            possible_worker.state = "ready"
            possible_worker.last_checkin = time.time()
            # Re-add the task to the task list if they died during execution
            if possible_worker.task:
                self.task_list.insert(0, possible_worker.task)
                possible_worker.task = None
        else:
            # Add new worker to worker dict
            new_worker = WTag(
                host_in=worker_host,
                port_in=worker_port,
                state_in="ready",
                time_in=time.time()
            )
            self.workers[(new_worker.host, new_worker.port)] = new_worker
        LOGGER.info("registered worker %s:%s",
            worker_host, worker_port,
        )
        # Send acknolegement to worker
        self.send_ack(worker_host, worker_port)


    def new_manager_job(self, message_dict):
        # Recieved new job conf
        LOGGER.debug("recieved\n%s", json.dumps(message_dict, indent=2))
        # Remove message_type from message_dict
        message_dict.pop("message_type", None)

        # Create a new Job instance using own tracker and json data
        new_job = Job(id_in=self.curr_job_id, dict_in=message_dict)
        # Add to queue
        self.job_queue.put(new_job)
        # Increment curr_job_id for next Job added
        self.curr_job_id += 1


    def finished_worker(self, message_dict):
        LOGGER.debug("recieved\n%s", json.dumps(message_dict, indent=2))
        this_worker = self.workers[(message_dict["worker_host"],
                                    message_dict["worker_port"])]
        this_worker.state = "ready"
        this_worker.task = None


    def send_ack(self, worker_host, worker_port):
        """Send the worker a registration ack."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect((worker_host, worker_port))
                reg_ack_message = json.dumps({
                    "message_type": "register_ack",
                    "worker_host": worker_host,
                    "worker_port": worker_port
                })
                sock.sendall(reg_ack_message.encode('utf-8'))
            except ConnectionRefusedError:
                LOGGER.debug("ConnectionRefusedError")
                self.workers[(worker_host, worker_port)].state = "dead"


    def shutdown_workers(self):
        """Forward the shutdown message to all the workers."""
        for worker in self.workers.values():
            if worker.state != "dead":
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    try:
                        sock.connect((worker.host, worker.port))
                        shutdown_message = json.dumps({
                            "message_type": "shutdown"
                        })
                        sock.sendall(shutdown_message.encode('utf-8'))
                    except ConnectionRefusedError:
                        LOGGER.debug("ConnectionRefusedError")
                        worker.state = "dead"


    def run_job(self):
        """Start executing a new job."""
		# If not currently exec job (and queue isn't empty), start new job
		# Manager runs each job to completion before starting a new one
        while not self.shutdown:
            if not self.is_executing_job and not self.job_queue.empty():
                self.is_executing_job = True
                curr_job = self.job_queue.get()

                # Mapping
                LOGGER.info("Begin Map Stage")
                # Delete output dir if it exists
                if os.path.exists(curr_job.job_dict["output_dir"]):
                    shutil.rmtree(curr_job.job_dict["output_dir"])

                # create new output directory
                os.makedirs(curr_job.job_dict["output_dir"])
                LOGGER.info("Created output")
                # Create a shared dir for temp intermediate files
                prefix = f"mapreduce-shared-job{curr_job.job_id:05d}-"
                self.tmpdir = tempfile.TemporaryDirectory(prefix=prefix)
                LOGGER.info("Created %s", self.tmpdir.name)

                # Execute mapping
                self.running_processes = "mapping"
                new_map_tasks = self.map_partioning(curr_job)
                self.task_list.extend(new_map_tasks)
				# Make sure not to move on to reduce when a worker is busy w/ a task
                while True:
                    self.distribute_new_tasks(curr_job)
                    if self.is_stage_complete():
                        break
                    else:
                        time.sleep(0.5)

                LOGGER.info("End Map Stage")
                # Reducing
                LOGGER.info("begin Reduce Stage")
                self.running_processes = "reducing"
                new_reduce_tasks = self.reduce_partitioning()
                self.task_list.extend(new_reduce_tasks)

                while True:
                    self.distribute_new_tasks(curr_job)
                    if self.is_stage_complete():
                        break
                    else:
                        time.sleep(0.5)
                LOGGER.info("end Reduce Stage")

                self.running_processes = "neither"
                # Clean up the temporary directory
                self.tmpdir.cleanup()
                LOGGER.info("Removed %s", self.tmpdir.name)

                self.is_executing_job = False
                LOGGER.info("Finished job. Output directory: %s", curr_job.job_dict["output_dir"])
            time.sleep(0.5)


    def is_stage_complete(self):
        """Double check that the stage is complete."""
        # If there are any busy workers, incomplete tasks
        # If there are any dead workers with tasks, incomplete tasks
        for worker in self.workers.values():
            if worker.state == "busy":
                return False
            elif worker.state == "dead" and worker.task != None:
                return False
        return True


    def map_partioning(self, curr_job):
        """Partition the input files into num_mappers partition"""
        # Each partition is a "new_map_task"
        # Get the input files from the job's input directory
        input_files = os.listdir(curr_job.job_dict["input_dir"])
        # Sort the input files by name
        input_files.sort()
        # Partition the input files into num_mappers partions using round robin
        # and assign each partition a task_id by order of generation (start 0)
        partitions = []
        task_id = 0
        # create num_mapper sublists with the files belonging to that partition
        for files in [input_files[i::curr_job.job_dict["num_mappers"]] \
                      for i in range(curr_job.job_dict["num_mappers"])]:
            partitions.append({"task_id": task_id, "files": files})
            task_id += 1

        return partitions


    def reduce_partitioning(self):
        """Put the files with the same parts in the same task."""
        partitions = []
        curr_task_id = 0
        dir_path = Path(self.tmpdir.name)
        # Get all the files in the tmp dir
        all_files = os.listdir(self.tmpdir.name)

        while all_files:
            curr_part = "*-" + all_files[curr_task_id].split("-")[1]
            one_group = sorted(dir_path.glob(curr_part))
            # Remove each file in one_group from all_files
            for file_name in one_group:
                all_files.remove(file_name.name)
            partitions.append({"task_id": curr_task_id, "files": one_group})
            # Increment the task ID
            curr_task_id += 1
        return partitions


    def distribute_new_tasks(self, curr_job):
        """Distribute the new_tasks to avaliable workers."""
        curr_task_id = 0
        while curr_task_id < len(self.task_list) and not self.shutdown:
            for worker in self.workers.values():
                if worker.state == "ready":
                    # Assign the task to the worker, change its state,
                    # increment the curr_task_id to get the next tasks's value
                    self.assign_task(worker, self.task_list[curr_task_id],
                                     curr_job)
                    worker.state = "busy"
                    curr_task_id += 1
                    break
                else:
                    time.sleep(0.5) # ???? TODO: check this, busy waiting?


    def assign_task(self, worker, task, curr_job):
        """Assign a task to the given worker --> send it to them"""
        ## Remove the task from the tasklist
        try:
            self.task_list.remove(task)
        except ValueError:
            LOGGER.debug("remove task from tasklist failed")
        # Preps input_paths by combining input_dir and file names
        input_paths_list = []
        worker.task = task
        if self.running_processes == "mapping":
            for curr_file in task.get("files"):
                full_path = curr_job.job_dict["input_dir"] + "/" + curr_file
                input_paths_list.append(full_path)

            task_message = {
                "message_type": "new_map_task",
                "task_id": task.get("task_id"),
                "input_paths": input_paths_list,
                "executable": curr_job.job_dict["mapper_exec"],
                "output_directory": self.tmpdir.name,
                "num_partitions": curr_job.job_dict["num_reducers"],
                "worker_host": worker.host,
                "worker_port": worker.port,
            }

        elif self.running_processes == "reducing":
            for curr_file in task.get("files"):
                full_path = os.path.join(self.tmpdir.name, curr_file)
                input_paths_list.append(full_path)

            task_message = {
                "message_type": "new_reduce_task",
                "task_id": task.get("task_id"),
                "input_paths": input_paths_list,
                "executable": curr_job.job_dict["reducer_exec"],
                "output_directory": curr_job.job_dict["output_dir"],
                "worker_host": worker.host,
                "worker_port": worker.port,
            }

        else:
            LOGGER.info("Error in assign_task")
            task_message = {"Error": "Error in assign_task"}

        # Send task message to worker
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect((worker.host, worker.port))
                assign_task_message = json.dumps(task_message)
                sock.sendall(assign_task_message.encode('utf-8'))
            except ConnectionRefusedError:
                LOGGER.debug("ConnectionRefusedError")
                worker.state = "dead"
                # task needs to be readded at the beginning of the list
                self.task_list.insert(0, worker.task)
                worker.task = None



@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
