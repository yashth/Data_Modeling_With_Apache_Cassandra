# Part I. ETL Pipeline for Pre-Processing the Files

## Import Python Packages
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from prettytable import PrettyTable

## Creating list of filepaths to process original event csv data files
print(os.getcwd())

filepath = os.getcwd() + '/event_data'

for root, dirs, files in os.walk(filepath):
    file_path_list = glob.glob(os.path.join(root,'*'))


## Processing the files to create the data file csv that will be used for Apache Casssandra tables
full_data_rows_list = [] 
    
for f in file_path_list:

    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
    
        for line in csvreader:
            full_data_rows_list.append(line) 
            

csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))
	

## Creating a Cluster
from cassandra.cluster import Cluster
cluster = Cluster()

session = cluster.connect()


## Create Keyspace
try:
    session.execute("""CREATE KEYSPACE IF NOT EXISTS apcahe_cassandra_project WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")
except Exception as e:
    print(e)
	
try:
    session.set_keyspace('apcahe_cassandra_project')
except Exception as e:
    print(e)
	
	
# Part II. Using Apache Cassandra to create Data Models according to asked queires

## Query1:
## Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4


### Create table "song_length_session" for Query1

query1 = "CREATE TABLE IF NOT EXISTS song_length_session"
query1 = query1 + "( session_id         varchar,\
					 item_In_Session    varchar, \
					 artist             varchar, \
					 song_title         varchar, \
					 song_length        varchar, \
					 PRIMARY KEY(session_id,item_In_Session))"

try:
    session.execute(query1)
except Exception as e:
    print(e)
	

### INSERT data into "song_length_session table"

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "INSERT INTO song_length_session (session_id, item_In_Session, artist, song_title, song_length)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (line[8], line[3], line[0], line[9], line[5]))
		
		
### SELECT statement to verify the data was entered into the table "song_length_session"
query = "SELECT * FROM song_length_session LIMIT 5"

try:
    rows = session.execute(query)
    t = PrettyTable(['Session Id', 'Item In Session', 'Artist', 'Song Title', 'Song Length'])
except Exception as e:
    print(e)

for row in rows:
    t.add_row([row.session_id,row.item_in_session,row.artist,row.song_title,row.song_length])
print(t)


### SELECT statement for Query1

query = "SELECT artist,song_title,song_length FROM song_length_session where session_id = '338' AND item_In_Session = '4'"

try:
    rows = session.execute(query)
    t = PrettyTable(['Artist', 'Song Title', 'Song Length'])
except Exception as e:
    print(e)

for row in rows:
    t.add_row([row.artist,row.song_title,row.song_length])
    
print(t)


## Query 2: 
## Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182 


#### Create Table "song_playlist_session" for Query2

query2 = "CREATE TABLE IF NOT EXISTS song_playlist_session"
query2 = query2 + "( user_id          varchar, \
					 session_id       varchar, \
					 artist           varchar, \
					 song_title 	  varchar, \
					 item_In_Session  varchar, \
					 first_name       varchar, \
					 last_name        varchar, \
					 PRIMARY KEY((user_id,session_id),item_In_Session));"

try:
    session.execute(query2)
except Exception as e:
    print(e)

### Insert values into "song_playlist_session" table

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) 
    for line in csvreader:
        query = "INSERT INTO song_playlist_session (user_id, session_id, artist, song_title, item_In_Session, first_name, last_name)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (line[10], line[8], line[0], line[9], line[3], line[1], line[4]))
		
### SELECT statement to verify the data was entered into the table "song_playlist_session"

query = "SELECT * FROM song_playlist_session LIMIT 5"

try:
    rows = session.execute(query)
    t = PrettyTable(['User Id', 'Session Id', 'Artist', 'Song Title', 'Item In Session', 'First Name', 'Last Name'])
except Exception as e:
    print(e)

for row in rows:
    t.add_row([row.user_id,row.session_id,row.artist,row.song_title,row.item_in_session,row.first_name,row.last_name])
    
print(t)


### The SELECT statement for Query2
query = "SELECT artist,song_title,first_name,last_name FROM song_playlist_session WHERE user_id='10' AND session_id='182'"

try:
    rows = session.execute(query)
    t = PrettyTable(['Artist', 'Song Title', 'First Name', 'Last Name'])
except Exception as e:
    print(e)

for row in rows:
    t.add_row([row.artist,row.song_title,row.first_name,row.last_name])
    
print(t)


## Query3:
## Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

### Create Table "user_song_history"

query3 = "CREATE TABLE IF NOT EXISTS user_song_history"
query3 = query3 + "( user_id varchar, song_title varchar, first_name varchar, last_name varchar, PRIMARY KEY(song_title,user_id))"

try:
    session.execute(query3)
except Exception as e:
    print(e)

### Insert values into "user_song_history" table

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) 
    for line in csvreader:
        query = "INSERT INTO user_song_history (user_id, song_title, first_name, last_name)"
        query = query + "VALUES (%s, %s, %s, %s)"
        session.execute(query, (line[10], line[9], line[1], line[4]))
		
		
### SELECT statement to verify the data was entered into the table "user_song_history"

query = "SELECT * FROM user_song_history LIMIT 5"
try:
    rows = session.execute(query)
    t = PrettyTable(['User Id', 'Song Title', 'First Name', 'Last Name'])
except Exception as e:
    print(e)

for row in rows:
    t.add_row([row.user_id, row.song_title, row.first_name, row.last_name])
    
print(t)


### SELECT statement for Query3

query = "SELECT first_name, last_name FROM user_song_history WHERE song_title = 'All Hands Against His Own'"

try:
    rows = session.execute(query)
    t = PrettyTable(['First Name', 'Last Name'])
except Exception as e:
    print(e)

for row in rows:
    t.add_row([row.first_name, row.last_name])
    
print(t)


### Drop the tables before closing out the sessions

query = "DROP TABLE IF EXISTS song_length_session"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
	
query = "DROP TABLE IF EXISTS song_playlist_session"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
	
query = "DROP TABLE IF EXISTS user_song_history"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
	

### Close the session and cluster connection

session.shutdown()
cluster.shutdown()