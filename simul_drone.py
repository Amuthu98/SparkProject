#!/usr/bin/env python3

import socket
import random
import uuid
from time import sleep
import datetime
import base64
import os

HOST = '127.0.0.1' #Standard loopback interface address(localhost)
PORT = 9997 # Port to listen on(non - privileged ports are > 1023)

# all caractresitics of the drone
id_drone = str(uuid.uuid4())
status_drone = "raw" #usefull field to check if a violation code have been modified by a police officer

# init drone with random value, will move later each X seconds(depends on sleep)
latitude_drone = round(random.uniform(-360, 360), 2)
longitude_drone = round(random.uniform(-360, 360), 2)
height_drone = round(random.uniform(8, 42), 2)

def update_drone_position(latitude_drone, longitude_drone, height_drone):
	update_step = random.uniform(-0.2, 0.2)

	# update latitude
	latitude_drone = latitude_drone + update_step

	if latitude_drone > 360:
	  latitude_drone = 360
	elif latitude_drone < -360:
	  latitude_drone = -360

	latitude_drone = round(latitude_drone, 2)

	# update longitude
	longitude_drone = longitude_drone + update_step

	if longitude_drone > 360:
	  longitude_drone = 360
	elif longitude_drone < -360:
	  longitude_drone = -360

	longitude_drone = round(longitude_drone, 2)

	# update height
	height_drone = height_drone + update_step

	if height_drone > 42:
	  height_drone = 42
	elif height_drone < 8:
	  height_drone = 8

	height_drone = round(height_drone, 2)

	return latitude_drone, longitude_drone, height_drone


# sockets to create basic logs and violation event
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s, \
socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s2, \
socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s3 :
	# prepare all sockets
	s.bind((HOST, PORT))
	s.listen()
	s2.bind((HOST, PORT + 1))
	s2.listen()
	s3.bind((HOST, PORT + 2))
	s3.listen()

	conn, addr = s.accept()
	conn2, addr = s2.accept()
	conn3, addr = s3.accept()
	with conn, conn2, conn3:
		while True:
			line = f"{latitude_drone};{longitude_drone};{height_drone};{datetime.datetime.now().timestamp()};{id_drone};{status_drone}\n" #send message
			# classic message
			conn.sendall(line.encode('UTF-8'))
			print("\nLINE : ",line)
			if random.random() < 0.17: # if violation detected, send specific message
				if random.random() <= 0.01: # violation code require human intervention ? ==> 1 %
					violation_code = 0
				else : 
					violation_code = random.randint(1, 50)

				# prepare everything for the violation stream
				image_id = str(uuid.uuid4())
				violation_line = f"{latitude_drone};{longitude_drone};{height_drone};{datetime.datetime.now().timestamp()};{id_drone};{status_drone};{violation_code};{image_id}\n"
				
				# prepare everything for the image stream
				with open("bad_parking_1.jpg", "rb") as img_file:
					my_base64_image = base64.b64encode(img_file.read())
				
				image_line = f"{datetime.datetime.now().timestamp()};{image_id};{my_base64_image}\n"
				# send on stream
				conn2.sendall(violation_line.encode('UTF-8'))
				print("\nVIOLATION LINE : ", violation_line)
				conn3.sendall(image_line.encode('UTF-8'))
				print("\nIMAGE LINE : ")#, image_line)

			sleep(4)
			latitude_drone, longitude_drone, height_drone =  update_drone_position(latitude_drone, longitude_drone, height_drone)