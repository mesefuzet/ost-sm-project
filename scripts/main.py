# For the orchestration of all the files
#@EVERYONE: when you finish your files, add them here


import subprocess
import os
import sys
import time


# Get the path to the current Python executable (from the virtual environment)
python_executable = sys.executable

# Define the script paths
scripts_dir = os.path.dirname(os.path.abspath(__file__))
producer_file = os.path.join(scripts_dir, "0_1_kafka_producer.py")
consumer_file = os.path.join(scripts_dir, "0_2_kafka_consumer.py")
spark_file = os.path.join(scripts_dir, "1_Emese_spark_streaming_streaming_KMeans.py")

try:
    # Start producer
    producer_process = subprocess.Popen([python_executable, producer_file], 
                                        stdout=sys.stdout, #log the output to see anything
                                        stderr=sys.stderr) #log the errors for debug
    print(f"Started Producer with PID {producer_process.pid}")
    time.sleep(3)

    # Start the consumer
    consumer_process = subprocess.Popen([python_executable, consumer_file], 
                                        stdout=sys.stdout, 
                                        stderr=sys.stderr)
    print(f"Started Consumer with PID {consumer_process.pid}")
    time.sleep(3)

    #Start the Spark streaming process
    spark_process = subprocess.Popen([python_executable, spark_file], 
                                     stdout=sys.stdout, 
                                     stderr=sys.stderr)
    print(f"Started Spark Streaming with PID {spark_process.pid}")
    time.sleep(3)

    # Wait until finish
    producer_process.wait()
    consumer_process.wait()
    spark_process.wait()

except KeyboardInterrupt:
    print("Terminating all processes...")
    producer_process.terminate()
    consumer_process.terminate()
    spark_process.terminate()
finally:
    print("All processes terminated.")
