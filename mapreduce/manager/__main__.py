"""MapReduce framework Manager node."""
import os
from queue import Queue
import shutil
import socket
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
    def __init__(self, host_in, port_in, state_in):
        """Construct a WTag instance"""
        self.host = host_in
        self.port = port_in
        self.state = state_in

class Job:
    """A class representing a Job task"""
    def __init__(self, id, input_directory, output_directory, mapper_executable, reducer_executable, num_mappers, num_reducers):
        """Construct a Job instance"""
        self.job_id = id
        self.input_dir = input_directory
        self.output_dir = output_directory
        self.mapper_exec = mapper_executable
        self.reducer_exec = reducer_executable
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.is_completed = False
    
class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        ## commented out pwd bc it's long
        LOGGER.info(
            "Starting manager:%s", port,
        )
        LOGGER.info(
            "PWD",
        )
        # Member vars
        self.options = {
            "host": host, 
            "port": port
        }
        self.shutdown = False
        self.workers = {}
        self.worker_queue = Queue()
        
        self.curr_job_id = 0
        self.job_queue = Queue()
        self.is_executing_job = False
        
        self.tcp_thread = threading.Thread(target=self.tcp_listening)
        self.tcp_thread.start()
        self.tcp_thread.join()
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

                # Socket recv() will block for a maximum of 1 second.  If you omit
                # this, it blocks indefinitely, waiting for packets.
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
                # Handle message 
                if message_dict["message_type"] == "shutdown":
                    # if shutdown message has been received, will not go back into 
                    # while loop and will effectively, well, shutdown
                    self.shutdown = True
                    LOGGER.debug("recieved\n%s", json.dumps(message_dict, indent=2))
                    LOGGER.info("shutting down")

                if message_dict["message_type"] == "register":
                    # Worker is ready to listen
                    LOGGER.debug(
                        "recieved\n%s",
                        json.dumps(message_dict, indent=2),
                    )
                    # Add worker to worker list
                    new_worker = WTag(
                        host_in=message_dict["worker_host"], 
                        port_in=message_dict["worker_port"], 
                        state_in="ready"
                        #job
                    )
                    self.workers[(new_worker.host, new_worker.port)] = new_worker
                    # Add worker to worker queue as well
                    self.worker_queue.put(new_worker)
                    LOGGER.info("registered worker %s:%s",
                        message_dict["worker_host"], message_dict["worker_port"],
                    )
                    # Send acknolegement to worker
                    self.send_ack(message_dict["worker_host"], message_dict["worker_port"])
                
                if message_dict["message_type"] == "new_manager_job":
                    # Recieved new job conf
                    LOGGER.debug("recieved\n%s", json.dumps(message_dict, indent=2))
                    # Remove message_type from message_dict
                    message_dict.pop("message_type", None)
                    
                    # Create a new Job instance using own tracker and json data
                    new_job = Job(id=self.curr_job_id, **message_dict)
                    # Add to queue
                    self.job_queue.put(new_job)
                    # Increment curr_job_id for next Job added
                    self.curr_job_id += 1

                    # If no currently executing job, run new one
                    if not self.is_executing_job:
                        self.run_job()

    def send_ack(self, worker_host, worker_port):
        """Send the worker a registration ack."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((worker_host, worker_port))
            reg_ack_message = json.dumps({
                "message_type": "register_ack",
                "worker_host": worker_host,
                "worker_port": worker_port
            })
            sock.sendall(reg_ack_message.encode('utf-8'))

    def shutdown_workers(self):
        """Forward the shutdown message to all the workers."""
        for worker in self.workers.values():
            if worker.state != "dead":
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((worker.host, worker.port))
                    shutdown_message = json.dumps({"message_type": "shutdown"})
                    sock.sendall(shutdown_message.encode('utf-8'))
    
    def run_job(self):
        """Start executing a new job"""
        # If not currently exec job (and queue isn't empty), start new job
        # Manager runs each job to completion before starting a new one
        if not self.is_executing_job and not self.job_queue.empty():
            curr_job = self.job_queue.get()

            # Delete output dir if it exists
            if os.path.exists(curr_job.output_dir):
                shutil.rmtree(curr_job.output_dir)
            
            # create new output directory
            os.makedirs(curr_job.output_dir)
            LOGGER.info("Created output")
            # Create a shared dir for temp intermediate files
            prefix = f"mapreduce-shared-job{curr_job.job_id:05d}-"
            self.tmpdir = tempfile.TemporaryDirectory(prefix=prefix)
            LOGGER.info("Created %s", self.tmpdir.name)
        
            job_completed = False
            while not job_completed:
                time.sleep(0.1)
                #Once everything is working TODO: change
                #job_completed = curr_job.is_completed 
                self.input_partioning(curr_job)
                job_completed = True
            # Clean up the temporary directory
            self.tmpdir.cleanup()
            LOGGER.info("Cleaned up tmpdir %s", self.tmpdir.name)

    def input_partioning(self, curr_job):
        """Partition the input files into num_mappers partition"""
        # Each partition is a "new_map_task"
        # Get the input files from the job's input directory 
        input_files = os.listdir(curr_job.input_dir)
        # Sort the input files by name
        input_files.sort()
        # Partition the input files into num_mappers partions using round robin
        # and sssign each partition a task_id by order of generation (start at 0)
        partitions = []
        task_id = 0
        # creating num_mapper sublists with the files belonging to that partition
        for files in [input_files[i::curr_job.num_mappers] for i in range(curr_job.num_mappers)]:
            partitions.append({"task_id": task_id, "files": files})
            task_id += 1

        # Distribute the new_map_tasks to avaliable workers
        self.distribute_new_map_tasks(partitions)

    def distribute_new_map_tasks(self, partitions):
        """Distribute the new_map_tasks to avaliable workers"""
        for worker in self.workers:
            if worker.state is "ready":





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
