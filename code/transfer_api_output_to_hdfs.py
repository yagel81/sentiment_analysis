#############################################################
####### copy files from local machine into HDFS       #######
#############################################################

import os
import pyarrow as pa
import datetime

def upload_to_hdfs(local_path, hdfs_path):
    """
    Upload a file from the local machine to HDFS.

    Parameters:
    - local_path (str): Local path of the file.
    - hdfs_path (str): HDFS path where the file will be uploaded.
    """
    fs = pa.hdfs.connect(
        host='<host>',
        port=<port>,
        user='<user>',
        kerb_ticket=None,
        extra_conf=None
    )

    try:
        # Check if the local file exists
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")

        # Extract the filename from the local path
        local_filename = os.path.basename(local_path)

        # Create the directory structure in HDFS
        fs.mkdir(os.path.dirname(hdfs_path), create_parents=True)

        # Upload the file to HDFS
        fs.upload(hdfs_path, local_path)
        print(f"File uploaded successfully: {local_filename} -> {hdfs_path}")

    except Exception as e:
        print(f"Error uploading file: {e}")

if __name__ == "__main__":
    # Specify local and HDFS paths
    wanted_date = str(datetime.date.today())
    local_file_path = f'/tmp/staging/project/api_calls/{wanted_date}.json'
    hdfs_file_path = f'hdfs://cnt7-naya-cdh63:8020/tmp/staging/project/api_calls/{wanted_date}.json'

    # Upload the file to HDFS
    upload_to_hdfs(local_file_path, hdfs_file_path)
