# Import the necessary modules
import subprocess

# Run the first script
subprocess.run(["python", "1_api_saved_locally.py"])

# Run the second script
subprocess.run(["python", "2_transfer_api_output_to_hdfs.py"])
