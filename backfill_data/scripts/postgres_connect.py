#!/usr/bin/env python
# coding: utf-8

from sshtunnel import SSHTunnelForwarder
import pandas as pd
import psycopg2 as psy

def printthis(message):
    print(message)

def get_df_from_sql(SSH_requiered, query,key_path):   #for getting a datafarame as a result
    
    db='datawarehouse'
    DB_HOST='datawarehouse.cdgpvetprks3.ap-south-1.rds.amazonaws.com'
    conn = []
    if SSH_requiered == 'Yes':
        SSH_HOST='ec2-15-206-161-154.ap-south-1.compute.amazonaws.com'
        #LOCALHOST="0.0.0.0"
        ssh_tunnel= SSHTunnelForwarder(
                (SSH_HOST),
                ssh_username="ec2-user",
                ssh_private_key= key_path,
                ssh_private_key_password= "",
                remote_bind_address=(DB_HOST, 5432))
        #ssh_tunnel._server_list[0].block_on_close = False
        ssh_tunnel.start()
        conn = psy.connect(
            host=ssh_tunnel.local_bind_host,
            port=ssh_tunnel.local_bind_port,
            user='postgres',
            password= "Simply1234",
            database='postgres')
        df_results = pd.read_sql(query, conn)
        conn.close()
        ssh_tunnel.stop()
        return df_results
    else:
        conn = psy.connect(
            host = DB_HOST,
            port = 5432,
            user = 'postgres',
            password= "Simply1234",
            database='postgres')
        df_results = pd.read_sql(query, conn)
        conn.close()
        return df_results
        
def get_conn(SSH_requiered,key_path):   #for getting a conn as a result
    
    db='datawarehouse'
    DB_HOST='datawarehouse.cdgpvetprks3.ap-south-1.rds.amazonaws.com'
    conn = []
    if SSH_requiered == 'Yes':
        SSH_HOST='ec2-15-206-161-154.ap-south-1.compute.amazonaws.com'
        #LOCALHOST="0.0.0.0"
        ssh_tunnel= SSHTunnelForwarder(
                (SSH_HOST),
                ssh_username="ec2-user",
                ssh_private_key= key_path,
                ssh_private_key_password= "",
                remote_bind_address=(DB_HOST, 5432))
        print('Starting Tunnel Started')
        ssh_tunnel.start()
        print('Started, getting conn')
        conn = psy.connect(
            host=ssh_tunnel.local_bind_host,
            port=ssh_tunnel.local_bind_port,
            user='postgres',
            password= "Simply1234",
            database='postgres')
        print('Connection Made')
        return conn
    else:
        conn = psy.connect(
            host = DB_HOST,
            port = 5432,
            user = 'postgres',
            password= "Simply1234",
            database='postgres')
        print('Connection Made')
        return conn

def get_conn2(SSH_requiered,key_path,host,database):   #for getting a conn as a result
    DB_HOST = 0
    if host=='datawarehouse':
        DB_HOST='datawarehouse.cdgpvetprks3.ap-south-1.rds.amazonaws.com'
    if host=='read_replica':
        DB_HOST='gs-prod-new-db-read.cdgpvetprks3.ap-south-1.rds.amazonaws.com'
    if host=='prod':
        DB_HOST='gs-prod-new-db.cdgpvetprks3.ap-south-1.rds.amazonaws.com'
    conn = []
    if SSH_requiered == 'Yes':
        SSH_HOST='ec2-15-206-161-154.ap-south-1.compute.amazonaws.com'
        #LOCALHOST="0.0.0.0"
        ssh_tunnel= SSHTunnelForwarder(
                (SSH_HOST),
                ssh_username="ec2-user",
                ssh_private_key= key_path,
                ssh_private_key_password= "",
                remote_bind_address=(DB_HOST, 5432))
        print('Starting Tunnel Started')
        ssh_tunnel.start()
        print('Started, getting conn')
        conn = psy.connect(
            host=ssh_tunnel.local_bind_host,
            port=ssh_tunnel.local_bind_port,
            user='postgres',
            password= "Simply1234",
            database=database)
        print('Connection Made')
        return conn
    else:
        conn = psy.connect(
            host = DB_HOST,
            port = 5432,
            user = 'postgres',
            password= "Simply1234",
            database='postgres')
        print('Connection Made')
        return conn


