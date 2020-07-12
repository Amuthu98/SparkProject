#!/usr/bin/env python3

import boto3
import pandas as pd
import uuid
from time import sleep
import math
from datetime import datetime
from io import StringIO
from botocore.exceptions import NoCredentialsError

# AWS CREDENTIALS
ACCESS_KEY = 'AKIASUAIN4F6CVZQZYSN'
SECRET_KEY = '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'

# AWS S3 link
s3 = boto3.resource(
    's3',
    region_name='eu-west-3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

# there aren't any infomations on drone + height of the drone 
# so we will use custom vars (drone_id, height, status)
drone_id = "00000000-0000-0000-0000-000000000001"
height = 0.0
status = "raw"
image_id = "00000000-0000-0000-0000-000000000042"

# read from csv until nothing is left
batch_count = 0

while batch_count < 1000:
    print("\nBEGIN ITER\n")
    #skiprows=batch_count*50000
    if batch_count == 0:
        historical_data = pd.read_csv("historical_2015.csv", nrows=50000,
         usecols=['Latitude', 'Longitude', 'Issue Date', 'Violation Time',
          'Violation Code'])
    else:
        historical_data = pd.read_csv("historical_2015.csv", nrows=50000,
         skiprows=range(1,50001 * batch_count), usecols=['Latitude', 'Longitude',
          'Issue Date', 'Violation Time', 'Violation Code'])
    
    print("Batch ", batch_count, " Batch len : ", len(historical_data.index))
    if len(historical_data) < 50000:
        print("ALL DATA SENT, EXIT")
        break

    print(historical_data.dtypes, "vio time type = ", type(historical_data["Violation Time"][0]))
    def change_vio_time(x):
        x = str(x)
        
        # replace whitespaces by 0
        x = x.replace(" ", "0")

        bad_chars = "+.`/*-"

        for char in bad_chars:
            x = x.replace(char, "0")

        # replace nan and val != [0 - 23] values by midnight
        if x == "nan" or int(x[0:2]) < 0 or int(x[0:2]) > 23:
            x = "1200A"    
        
        # len = 4 put A letter
        if len(x) == 4:
            x += "A" 

        # if there is a letter (A) instead of number, replace by number (0)
        if "A" in x[0:4] :
            x = x[0:4].replace("A", "0") + x[4]

        try :
            # ifs after exeptions
            if x[0:2] == "12" and x[4] == "A":
                #print("Midnight changes")
                x = "00"+":"+x[2:4] + ":00"
            elif x[0:2] != "12" and x[4] == "P":
                if int(x[0:2]) > 12:
                    print("\n+1 strange hour dont change")
                    x = str(int(x[0:2])) + ":" + x[2:4] + ":00"
                else:
                    x = str(int(x[0:2]) + 12) + ":" + x[2:4] + ":00"
                #print("x after ", x, "\n")
            else:
                x = x[0:2] + ":" + x[2:4] + ":00"
        except Exception as e:
            print("X before exception ", x)
            print("EXCEPTION IN IFS ", str(e))

        if "+" in x:
            print("WTF ", x)

        return x

    historical_data['Violation Time2'] = historical_data['Violation Time'] \
    .apply(change_vio_time)

    historical_data['Violation Time2'] = historical_data['Violation Time2'].astype(str)
    historical_data['Issue Date'] = historical_data['Issue Date'].astype(str)

    historical_data['Issue Date'] = historical_data['Issue Date'] + " "+ historical_data['Violation Time2']

    # now use another function to cast string in datetime then timestamp
    def string_to_timestamp(x):
        x = datetime.strptime(x, "%m/%d/%Y %H:%M:%S")
        x = str(int(x.timestamp() + 3600))
        return x

    historical_data['Issue Date'] = historical_data['Issue Date'].apply(string_to_timestamp)
    historical_data['Height'] = height
    historical_data['Latitude'] = 0.0
    historical_data['Longitude'] = 0.0
    historical_data['Id Drone'] = drone_id
    historical_data['Status'] = status
    historical_data['Image Id'] = image_id

    # final dataset, ready to be pushed in S3
    historical_data = historical_data[['Latitude', 'Longitude', 'Height', 'Issue Date', 'Id Drone',
    'Status', 'Violation Code', 'Image Id']]
    
    # Create buffer
    csv_buffer = StringIO()

    # Write dataframe to buffer
    historical_data.to_csv(csv_buffer, sep=";", index=False, header=None)

    # Write buffer to S3 object
    time = datetime.now().strftime("%Y%m%d%H%M%S")
    
    try:
        s3.Object("projetspark4iabd2ana2", "raw_historical_data_2/part_"+str(batch_count)+"_"+time+".txt").put(Body=csv_buffer.getvalue())
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")

    print("START")
    print(historical_data.head())
    print("END")
    print(historical_data.tail())

    batch_count += 1