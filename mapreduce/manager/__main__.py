"""MapReduce framework Manager node."""
import os
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
		
		self.curr_job_id = 0
		self.job_queue = Queue()
		self.is_executing_job = False
		self.tmpdir = None
		
		self.job_thread = threading.Thread(target=self.run_job)
		self.tcp_thread = threading.Thread(target=self.tcp_listening)
		
		self.job_thread.start()
		self.tcp_thread.start()
		
		self.job_thread.join()
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
				
				# Handle messages
				if message_dict["message_type"] == "shutdown":
					self.shutdown_manager(message_dict)

				elif message_dict["message_type"] == "register":
					self.register_worker(message_dict)
				
				elif message_dict["message_type"] == "new_manager_job":
					self.new_manager_job(message_dict)
				
				elif message_dict["message_type"] == "finished":
					self.finished_worker(message_dict)


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
		# Add worker to worker list
		new_worker = WTag(
			host_in=message_dict["worker_host"], 
			port_in=message_dict["worker_port"], 
			state_in="ready"
			#job
		)
		self.workers[(new_worker.host, new_worker.port)] = new_worker
		LOGGER.info("registered worker %s:%s",
			message_dict["worker_host"], message_dict["worker_port"],
		)
		# Send acknolegement to worker
		self.send_ack(message_dict["worker_host"], message_dict["worker_port"])

	def new_manager_job(self, message_dict):
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

	def finished_worker(self, message_dict):
		LOGGER.debug("recieved\n%s", json.dumps(message_dict, indent=2))
		this_worker = self.workers[(message_dict["worker_host"], message_dict["worker_port"])]
		this_worker.state = "ready"


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
		while not self.shutdown:
			if not self.is_executing_job and not self.job_queue.empty():
				self.is_executing_job = True
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
					#Once everything is working TODO: change
					#job_completed = curr_job.is_completed 
					new_map_tasks = self.input_partioning(curr_job)
					self.distribute_new_map_tasks(new_map_tasks, curr_job)
					job_completed = True
				# Clean up the temporary directory
				self.tmpdir.cleanup()
				LOGGER.info("Cleaned up tmpdir %s", self.tmpdir.name)
			time.sleep(0.5)

	def input_partioning(self, curr_job):
		"""Partition the input files into num_mappers partition"""
		# Each partition is a "new_map_task"
		# Get the input files from the job's input directory 
		input_files = os.listdir(curr_job.input_dir)
		# Sort the input files by name
		input_files.sort()
		# Partition the input files into num_mappers partions using round robin
		# and assign each partition a task_id by order of generation (start at 0)
		partitions = []
		task_id = 0
		# creating num_mapper sublists with the files belonging to that partition
		for files in [input_files[i::curr_job.num_mappers] for i in range(curr_job.num_mappers)]:
			partitions.append({"task_id": task_id, "files": files})
			task_id += 1
		
		return partitions

	def distribute_new_map_tasks(self, new_map_tasks, curr_job):
		"""Distribute the new_map_tasks to avaliable workers"""
		LOGGER.info("Begin Map Stage")
		curr_task_id = 0
		while curr_task_id < len(new_map_tasks) and not self.shutdown:
			for worker in self.workers.values():
				if worker.state == "ready":
					# Assign the task to the worker, change its state, 
					# and increment the curr_task_id to get the next tasks's value
					self.assign_task(worker, new_map_tasks[curr_task_id], curr_job)
					worker.state = "busy"
					curr_task_id += 1
					break
				else: 
					time.sleep(0.5) # ???? TODO: check this, busy waiting?
		
		LOGGER.info("End Map Stage")

	def assign_task(self, worker, task, curr_job):
		"""Assign a task to the given worker --> send it to them"""
		# Preps input_paths by combining input_dir and file names
		input_paths_list = []
		for curr_file in task.get("files"):
			full_path = curr_job.input_dir + "/" + curr_file
			input_paths_list.append(full_path)

		task_message = {
			"message_type": "new_map_task",
			"task_id": task.get("task_id"),
			"input_paths": input_paths_list, 
			"executable": curr_job.mapper_exec,
			"output_directory": self.tmpdir.name, # don't know if this is right
			"num_partitions": curr_job.num_reducers,
			"worker_host": worker.host,
			"worker_port": worker.port,
		}

		# Send task message to worker
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.connect((worker.host, worker.port))
			assign_task_message = json.dumps(task_message)
			sock.sendall(assign_task_message.encode('utf-8'))




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
