# data_pipeline.py
import os
import pandas as pd
import paramiko



SFTP_HOST = "localhost"  
SFTP_PORT = 22
SFTP_USER = "demo"
SFTP_PASS = "password"
SFTP_REMOTE_DIR = "/fake_sftp"
LOCAL_DATA_DIR = "data"

def connect_sftp():
    try:
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASS)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return sftp, transport
    except Exception as e:
        print(f"Error connecting to SFTP: {e}")
        return None, None

def download_sftp_files(sftp, remote_dir, local_dir):
    try:
        files = sftp.listdir(remote_dir)
        os.makedirs(local_dir, exist_ok=True)
        for file in files:
            remote_path = f"{remote_dir}/{file}"
            local_path = os.path.join(local_dir, file)
            sftp.get(remote_path, local_path)
            print(f"Downloaded: {file}")
    except Exception as e:
        print(f"Error downloading files: {e}")

def ingest():
    sftp, transport = connect_sftp()
    if sftp:
        download_sftp_files(sftp, SFTP_REMOTE_DIR, LOCAL_DATA_DIR)
        sftp.close()
        transport.close()

__all__ = ['ingest']
