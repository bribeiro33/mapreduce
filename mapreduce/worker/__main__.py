"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import socket
import string
import threading
import mapreduce.utils


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
