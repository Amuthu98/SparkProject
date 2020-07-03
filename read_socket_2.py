#!/usr/bin/env python3

import socket
import boto3
from datetime import datetime
from botocore.exceptions import NoCredentialsError

# AWS CREDENTIALS
ACCESS_KEY = 'AKIA6FDQAJXFXEOCTDIW'
SECRET_KEY = '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'

s3 = boto3.resource(
    's3',
    region_name='eu-west-3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

# AWS function to upload
def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

HOST = '127.0.0.1'  # The server's hostname or IP address
PORT = 9998        # The port used by the server

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    while True:
    	data = s.recv(1024)
    	print(repr(data))
    	
    	# if empty data, close
    	if str(data) == "b\'\'":
    		break
    	try:
    		#if data
    		time = datetime.now().strftime("%Y%m%d%H%M%S")
    		s3.Object("projetspark4iabd2ana", "drone_violation/drone_violation_"+time+".txt").put(Body=data)
    		#uploaded = upload_to_aws('local_file', 'bucket_name', 's3_file_name')
    	except FileNotFoundError:
        	print("The file was not found")
    	except NoCredentialsError:
        	print("Credentials not available")

