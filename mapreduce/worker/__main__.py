"""MapReduce framework Worker node."""
import hashlib
import os
import logging
import json
import shutil
import tempfile
import time
import click
import socket
import string
import threading
import mapreduce.utils
from pathlib import Path
import subprocess


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
		self.options= {
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
		return self.state
	
	def get_workerhost(self):
		return self.options["host"]
	
	def get_workerport(self):
		return self.options["port"]

	def register(self):
		"""Send registration message when worker is ready to Manager."""
		# create an INET, STREAMing socket, this is TCP
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

			# connect to the server
			sock.connect((self.options["manager_host"], self.options["manager_port"]))

			# send a message when worker is ready
			if (self.state == "ready"):
				# this is the message
				message_dict = {
					"message_type" : "register",
					"worker_host" : self.options["host"],
					"worker_port" : self.options["port"],
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
				sock.connect((self.options["manager_host"], self.options["manager_port"]))

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
		## TODO: Gol: this is done in manager as well, which one do you think is 
		## best to keep?
		self.state = "busy"

		# get all the json variables for directing the work
		task_id = message_dict["task_id"]
		input_paths = message_dict["input_paths"]
		executable = message_dict["executable"]
		output_dir = message_dict["output_directory"]
		num_partitions = message_dict["num_partitions"]

		# 0.5 Create local temp dir
		prefix = f"mapreduce-local-task{task_id:05d}-"
		tmpdir = tempfile.TemporaryDirectory(prefix=prefix)
		LOGGER.info("Created %s", tmpdir.name)
		
		for input_path in input_paths:
			# 1. Run map executable in input files, returns key-value pairs
			with open(input_path) as infile:
				with subprocess.Popen(
					[executable],
					stdin=infile,
					stdout=subprocess.PIPE,
					text=True,
				) as map_process:
					LOGGER.info("Executed %s input=%s", executable, input_path)
					# 2. partition the map output into several intermediate partition files
					# Add line to correct partition output file
					for line in map_process.stdout:
						# Split the line by the tab to access the key
						key = line.split('\t')[0]
						# Hash and mod the key to get partition number
						hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
						keyhash = int(hexdigest, base=16)
						partition_number = keyhash % num_partitions

						inter_file_name = f"maptask{task_id:05d}-part{partition_number:05d}"
						# Create the full path to the intermediate file
						inter_file_path = os.path.join(tmpdir.name, inter_file_name)
						# Write the contents of line to the intermediate file
						# The 'a' appends the line instead of rewriting
						with open(inter_file_path, 'a') as inter_file:
							inter_file.write(line)

		# 3. Sort each output file by line
		all_inter_files = os.listdir(tmpdir.name)
		self.sort_file_lines(all_inter_files, tmpdir.name)

		# 4. Move each sorted output file to the shared temporary directory specified by the Manager
		for file_name in all_inter_files:
			src_path = os.path.join(tmpdir.name, file_name)
			dest_path = os.path.join(output_dir, file_name)
			shutil.move(src_path, dest_path)
			LOGGER.info("Moved %s -> %s", src_path, dest_path)
		
		# Clean up the temporary directory
		tmpdir.cleanup()
		LOGGER.info("Removed %s", tmpdir.name) # might have to use path not name
		
		# 5. Send TCP message to Manager
		message_dict = {
			"message_type": "finished",
			"task_id": task_id,
			"worker_host": self.options['host'],
			"worker_port": self.options['port'],
		}
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.connect((self.options["manager_host"], self.options["manager_port"]))
			completion_msg = json.dumps(message_dict)
			sock.sendall(completion_msg.encode('utf-8'))
		# output_files = []
		# for path in input_paths:
		# 	input_filename = Path(path).name
		# 	output_directory = \
		# 		Path(message_dict["output_directory"]) / input_filename
		# 	with open(path, 'r', encoding='utf-8') as infile:
		# 		with open(output_directory, 'w', encoding='utf-8') as outfile:
		# 			subprocess.run(executable, stdin=infile, stdout=outfile, check=True)
		# 	output_files.append(str(output_directory))

	def sort_file_lines(self, all_files, tmpdir_name):
		"""Sort the content of all the given files by line."""
		for file_name in all_files:
			# Open the file in read mode
			file_path = os.path.join(tmpdir_name, file_name)
			with open(file_path, 'r') as f:
				# Read the lines from the file and sort them
				lines = f.readlines()
				lines.sort()

			# Write the sorted lines back to the file
			with open(file_path, 'w') as f:
				f.writelines(lines)
			LOGGER.info("Sorted %s", file_path)

	def tcp_listening(self):
		"""Listen for incoming messages from manager."""
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

				# Handle message & threads

				# threads = []
				if message_dict["message_type"] == "shutdown":
					# if shutdown message has been received, will not go back into 
					# while loop and will effectively, well, shutdown
					self.shutdown = True
					
					LOGGER.debug("recieved\n%s", json.dumps(message_dict, indent=2))
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
					LOGGER.debug("recieved\n%s", json.dumps(message_dict, indent=2),)
					self.mapping(message_dict)

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
