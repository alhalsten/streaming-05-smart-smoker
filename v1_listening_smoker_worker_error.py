"""
   Angela Halsten 9/16/23
    This program listens for work messages contiously.
    Start multiple versions to add more workers.  

    Author: Denise Case
    Date: January 15, 2023

"""

import pika
import sys
import time

#from collections import deque


# define a callback function to be called when a message is received


from util_logger import setup_logger
logger, logname = setup_logger(__file__)



def callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    logger.info(f" [x] Received {body.decode()}")
    # simulate work by sleeping for the number of dots in the message
    time.sleep(body.count(b"."))
    # when done with task, tell the user
    logger.info(" [x] Done.")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

    #tuples = body.decode()
    #(timestamp, temp) = tuples
    
    #smoker_deque = deque(temp, maxlen=5)  # limited to 5 items (the 5 most recent readings)
    #smoker_deque.append(temp)
    #last = smoker_deque[-1]
    #first = smoker_deque[0]
    #warning_test = last - first
    #if warning_test > 15:
        #print("smoker alert!")
        
# define a main function to run the program
def main(hn: str = "localhost", qn: str = "task_queue"):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        logger.info()
        logger.info("ERROR: connection to RabbitMQ server failed.")
        logger.info(f"Verify the server is running on host={hn}.")
        logger.infot(f"The error says: {e}")
        logger.info()
        sys.exit(1)

    try:
        
        # use the connection to create a communication channel
        channel = connection.channel()
        #channel.queue_delete(queue=qn)
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        #channel.queue_delete(queue=qn)
        channel.queue_declare(queue=qn, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume(queue=qn, on_message_callback=callback, auto_ack=False)

        # print a message to the console for the user
        logger.info(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logger.info()
        logger.info("ERROR: something went wrong.")
        logger.info(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info()
        logger.info(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logger.info("\nClosing connection. Goodbye.\n")
        connection.close()
        




# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost", "01-smoker")
