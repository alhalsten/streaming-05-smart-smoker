"""
    Angela Halsten 9/16/23
    This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.

    Author: Denise Case
    Date: January 15, 2023

"""

import pika
import sys
import webbrowser
import csv
import logging
import time

from util_logger import setup_logger
logger, logname = setup_logger(__file__)

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print() 

def send_message_01(host: str, queue_name_01: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name_01, durable=True)

       
                
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name_01, body=message_01)
        # print a message to the console for the user
        logger.info(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        logger.info(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
        
    finally:
        # close the connection to the server
        conn.close()

input_file_name = "smoker-temps.csv"

def prepare_message_from_row_01(row):
    """Prepare a binary message from a given row."""
    Time, Channel1, Channel2, Channel3 = row
    # use an fstring to create a message from our data
    # notice the f before the opening quote for our string?
    fstring_message_01 = f"[{Time}, {Channel1}]"

    # prepare a binary (1s and 0s) message to stream
    MESSAGE_01 = fstring_message_01.encode()
    logging.debug(f"Prepared message: {fstring_message_01}")
    return MESSAGE_01

def stream_row_01():
    """Read from input file and stream data."""
    #logging.info(f"Starting to stream data from {input_file_name}.")

    # Create a file object for input (r = read access)
with open(input_file_name, 'r') as input_file:
        #logging.info(f"Opened for reading: {"tasks.csv"}.")

        # Create a CSV reader object
    reader = csv.reader(input_file, delimiter= ",")
    header = next(reader)  # Skip header row
    logging.info(f"Skipped header row: {header}")


       
    
            
# Meat 1, Channel2
def send_message_02(host: str, queue_name_02: str, message_02: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name_02, durable=True)

       
                
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name_02, body=message_02)
        # print a message to the console for the user
        logger.info(f" [x] Sent {message_02}")
    except pika.exceptions.AMQPConnectionError as e:
        logger.info(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
        
    finally:
        # close the connection to the server
        conn.close()

input_file_name = "smoker-temps.csv"

def prepare_message_from_row_02(row):
    """Prepare a binary message from a given row."""
    Time, Channel1, Channel2, Channel3 = row
    # use an fstring to create a message from our data
    # notice the f before the opening quote for our string?
    fstring_message_02 = f"[{Time}, {Channel2}]"

    # prepare a binary (1s and 0s) message to stream
    MESSAGE_02 = fstring_message_02.encode()
    logging.debug(f"Prepared message: {fstring_message_02}")
    return MESSAGE_02

def stream_row_02():
    """Read from input file and stream data."""
    #logging.info(f"Starting to stream data from {input_file_name}.")

    # Create a file object for input (r = read access)
with open(input_file_name, 'r') as input_file:
        #logging.info(f"Opened for reading: {"tasks.csv"}.")

        # Create a CSV reader object
    reader = csv.reader(input_file, delimiter= ",")
    header = next(reader)  # Skip header row
    logging.info(f"Skipped header row: {header}")


# Meat 2, Channel3
def send_message_03(host: str, queue_name_03: str, message_03: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name_03, durable=True)

       
                
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name_03, body=message_03)
        # print a message to the console for the user
        logger.info(f" [x] Sent {message_03}")
    except pika.exceptions.AMQPConnectionError as e:
        logger.info(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
        
    finally:
        # close the connection to the server
        conn.close()

input_file_name = "smoker-temps.csv"

def prepare_message_from_row_03(row):
    """Prepare a binary message from a given row."""
    Time, Channel1, Channel2, Channel3 = row
    # use an fstring to create a message from our data
    # notice the f before the opening quote for our string?
    fstring_message_03 = f"[{Time}, {Channel3}]"

    # prepare a binary (1s and 0s) message to stream
    MESSAGE_03 = fstring_message_03.encode()
    logging.debug(f"Prepared message: {fstring_message_03}")
    return MESSAGE_03

def stream_row_03():
    """Read from input file and stream data."""
    #logging.info(f"Starting to stream data from {input_file_name}.")

    # Create a file object for input (r = read access)
with open(input_file_name, 'r') as input_file:
        #logging.info(f"Opened for reading: {"tasks.csv"}.")

        # Create a CSV reader object
    reader = csv.reader(input_file, delimiter= ",")
    header = next(reader)  # Skip header row
    logging.info(f"Skipped header row: {header}")
      

    for row in reader:
        MESSAGE_01 = prepare_message_from_row_01(row)
        MESSAGE_02 = prepare_message_from_row_02(row)
        MESSAGE_03 = prepare_message_from_row_03(row)
        #Time, Channel1, Channel2, Channel3 = row
        message_01 = MESSAGE_01.decode()
        message_02 = MESSAGE_02.decode()
        message_03 = MESSAGE_03.decode()
        send_message_01("localhost","01-smoker", message_01)
        send_message_02("localhost","02-food-A", message_02)
        send_message_03("localhost","03-food-B", message_03)
        time.sleep(30) #sleep for 30 seconds

        
# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()
    stream_row_01()
    stream_row_02() 
    stream_row_03()

    