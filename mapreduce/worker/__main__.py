"""MapReduce framework Worker node."""
from contextlib import ExitStack
import hashlib
import heapq
import os
import logging
import json
import shutil
import tempfile
import time
import threading
import subprocess
import socket
import click


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker:%s", port,
        )
        LOGGER.info("PWD",)

        # Member vars
        self.state = "ready"
        self.shutdown = False
        self.options = {
            "host": host,
            "port": port,
            "manager_host": manager_host,
            "manager_port": manager_port,
        }
        self.tmpdir = None
        self.hbthread = threading.Thread(target=self.send_heartbeat)

        self.register()
        self.tcp_listening()

    # function to access the worker's state variable from manager
    def get_state(self):
        """Access function."""
        return self.state

    def get_workerhost(self):
        """Access function."""
        return self.options["host"]

    def get_workerport(self):
        """Access function."""
        return self.options["port"]

    def register(self):
        """Send registration message when worker is ready to Manager."""
        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # connect to the server
            sock.connect((self.options["manager_host"],
                          self.options["manager_port"]))

            # send a message when worker is ready
            if self.state == "ready":
                # this is the message
                message_dict = {
                    "message_type": "register",
                    "worker_host": self.options["host"],
                    "worker_port": self.options["port"],
                }
                message = json.dumps(message_dict)
                # send the message
                sock.sendall(message.encode('utf-8'))

    def send_heartbeat(self):
        """Send heartbeat once worker has been registered with manager."""
        # check if shutdown
        while not self.shutdown:
            # Create an INET, DGRAM socket, this is UDP
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

                # Connect to the UDP socket on server
                sock.connect((self.options["manager_host"],
                              self.options["manager_port"]))

                # Send a hb message
                message_dict = {
                    "message_type": "heartbeat",
                    "worker_host": self.options["host"],
                    "worker_port": self.options["port"],
                }
                message = json.dumps(message_dict)
                # send the message
                sock.sendall(message.encode('utf-8'))
            time.sleep(2)

    def mapping(self, message_dict):
        """Carry out mapping."""
        # mark state as busy since a task is beginning
        self.state = "busy"

        # get all the json variables for directing the work
        # stored in a dictionary for styling
        map_dict = {
            "input_paths": message_dict["input_paths"],
            "executable": message_dict["executable"],
            "output_dir": message_dict["output_directory"],
            "num_partitions": message_dict["num_partitions"]
        }
        task_id = message_dict["task_id"]
        # input_paths = message_dict["input_paths"]
        # executable = message_dict["executable"]
        # output_dir = message_dict["output_directory"]
        # num_partitions = message_dict["num_partitions"]

        # 0.5 Create local temp dir
        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created %s", tmpdir)

            with ExitStack() as stack:
                # Open input_files and create a list of corresponding
                # file objects
                file_objs = []
                for filename in map_dict["input_paths"]:
                    file_objs.append(stack.enter_context(open(
                                                              filename,
                                                              encoding="utf8"
                                                              )))

                # Create all partition files and create a list of corr file obj
                partition_files = []
                for i in range(0, map_dict["num_partitions"]):
                    # The 'a' appends the line instead of rewriting
                    partition_files.append(stack.enter_context(
                        open(os.path.join(tmpdir,
                                          f"maptask{task_id:05d}-part{i:05d}"),
                                          'a', encoding="utf8")))

                # For every input file, run the executable and pipe result to
                # correct partition intermediate file
                for input_file in file_objs:
                    # 1. Run map executable in input files,
                    # returns key-value pairs
                    with subprocess.Popen(
                        [map_dict["executable"]],
                        stdin=input_file,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process:
                        LOGGER.info("Executed %s input=%s",
                                    map_dict["executable"], input_file)
                        # 2. partition the map output into several intermediate
                        # partition files
                        # Add line to correct partition output file
                        for line in map_process.stdout:
                            # Write the contents of line to the
                            # intermediate partition file
                            partition_files[int(hashlib.md5(
                                line.split('\t')[0]
                                .encode("utf-8")).hexdigest(), base=16)
                                % map_dict["num_partitions"]].write(line)

            # 3. Sort each output file by line
            # all_inter_files = os.listdir(tmpdir)
            self.sort_file_lines(os.listdir(tmpdir), tmpdir)

            # 4. Move each sorted output file to the shared
            # temporary directory specified by the Manager
            for file_name in os.listdir(tmpdir):
                # src_path = os.path.join(tmpdir, file_name)
                # dest_path = os.path.join(output_dir, file_name)
                shutil.move(os.path.join(tmpdir, file_name),
                            os.path.join(map_dict["output_dir"], file_name))
                LOGGER.info("Moved %s -> %s",
                            os.path.join(tmpdir, file_name),
                            os.path.join(map_dict["output_dir"], file_name))

            # Clean up the temporary directory
            LOGGER.info("Removed %s", tmpdir)

            # 5. Send TCP message to Manager
            self.send_completion_msg(task_id)

    def reducing(self, message_dict):
        """Carry out reduce stage."""
        self.state = "busy"

        # get all the json variables for directing the work
        task_id = message_dict["task_id"]
        input_files = message_dict["input_paths"]
        executable = message_dict["executable"]
        output_dir = message_dict["output_directory"]

        # 0.5 Create local temp dir
        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created %s", tmpdir)

            with ExitStack() as stack:
                # Open all input files
                open_input = []
                for file_name in input_files:
                    open_input.append(stack.enter_context(
                        open(file_name, encoding="utf8")
                    ))

                # Create temp output file in temp dir and open it
                file_name = f"part-{task_id:05d}"
                LOGGER.debug("in reducing, os.path.join %s", os.path.join(tmpdir, file_name))
                # tmp_path= os.path.join(tmpdir, file_name)
                outfile = stack.enter_context(open(
                    os.path.join(tmpdir, file_name), "x",
                    encoding="utf8"))
                LOGGER.info("Created %s", os.path.join(tmpdir, file_name))

                with subprocess.Popen(
                    [executable],
                    text=True,
                    stdin=subprocess.PIPE,
                    stdout=outfile,
                ) as reduce_process:
                    # 1. Merge input files into one sorted
                    # output stream. --> heapq.merge
                    # 2. Run the reduce executable on merged input,
                    # writing output to a single file.
                    for line in heapq.merge(*open_input):
                        reduce_process.stdin.write(line)
            LOGGER.info("Executed %s", executable)
            # 3. Move the output file to the final output
            # directory specified by the Manager.
            dest_path = os.path.join(output_dir, file_name)
            shutil.move(os.path.join(tmpdir, file_name), dest_path)
            LOGGER.info("Moved %s -> %s",
                        os.path.join(tmpdir, file_name), dest_path)

            LOGGER.info("Removed %s", tmpdir)

            # 4. Send TCP message to Manager
            self.send_completion_msg(task_id)

    def send_completion_msg(self, task_id):
        """Send TCP completion message to the manager."""
        message_dict = {
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": self.options['host'],
            "worker_port": self.options['port'],
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.options["manager_host"],
                          self.options["manager_port"]))
            completion_msg = json.dumps(message_dict)
            sock.sendall(completion_msg.encode('utf-8'))

    def sort_file_lines(self, all_files, tmpdir_name):
        """Sort the content of all the given files by line."""
        for file_name in all_files:
            # Open the file in read mode
            file_path = os.path.join(tmpdir_name, file_name)
            with open(file_path, 'r', encoding="utf8") as f_open:
                # Read the lines from the file and sort them
                lines = f_open.readlines()
                lines.sort()

            # Write the sorted lines back to the file
            with open(file_path, 'w', encoding="utf8") as f_open:
                f_open.writelines(lines)
            LOGGER.info("Sorted %s", file_path)

    def tcp_listening(self):
        """Listen for incoming messages from manager."""
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
                # If you omit
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

                # Handle message & threads
                # threads = []
                if message_dict["message_type"] == "shutdown":
                    # if shutdown message has been received,
                    # will not go back into while loop and will
                    # effectively, well, shutdown
                    self.shutdown = True

                    LOGGER.debug("recieved\n%s",
                                 json.dumps(message_dict, indent=2))
                    LOGGER.info("shutting down",)
                    # print("server() shutting down")
                if message_dict["message_type"] == "register_ack":
                    # manager has receieved worker registration
                    LOGGER.debug(
                        "recieved\n%s", json.dumps(message_dict, indent=2),
                    )
                    # create thread
                    # threads.append(thread)
                    self.hbthread.start()
                if message_dict["message_type"] == "new_map_task":
                    LOGGER.debug("recieved\n%s",
                                 json.dumps(message_dict, indent=2),)
                    self.mapping(message_dict)

                if message_dict["message_type"] == "new_reduce_task":
                    LOGGER.debug("recieved\n%s",
                                 json.dumps(message_dict, indent=2),)
                    self.reducing(message_dict)

            if self.hbthread.is_alive():
                self.hbthread.join()


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
