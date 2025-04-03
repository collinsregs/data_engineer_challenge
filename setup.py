import subprocess
import os
import time
import sys
import venv

def create_venv_and_install_packages():
    """Creates a virtual environment and installs required packages."""

    venv_dir = "venv" 
    requirements_file = "requirements.txt"  

    if not os.path.exists(venv_dir):
        print("Creating virtual environment...")
        venv.create(venv_dir, with_pip=True)

    if sys.platform == "win32":
        venv_python = os.path.join(venv_dir, "Scripts", "python.exe")
    else:
        venv_python = os.path.join(venv_dir, "bin", "python")

    if os.path.exists(requirements_file):
        print("Installing packages from requirements.txt...")
        subprocess.run([venv_python, "-m", "pip", "install", "-r", requirements_file], check=True)

    return venv_python

def run_setup_and_api(venv_python):
    """Runs data generation, SFTP server, and API server with virtual environment.""" 

    print('generating fake data')
    subprocess.run([venv_python, os.path.join("sftp_setup", "generate_test_data.py")], check=True)

    print('Starting up sftp server')
    sftp_process = subprocess.Popen([venv_python, os.path.join("sftp_setup", "start_sftp.py")])
    time.sleep(2)

    print('Starting up flask api server')
    api_process = subprocess.Popen([venv_python, "api.py"])

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Terminating servers...")
        sftp_process.terminate()
        api_process.terminate()
        sftp_process.wait()
        api_process.wait()

if __name__ == "__main__":
    venv_python = create_venv_and_install_packages()
    run_setup_and_api(venv_python)