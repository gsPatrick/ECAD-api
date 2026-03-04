import paramiko
import os

hostname = "129.121.39.128"
port = 22022
username = "root"
password = "Senhanova#123"

def run_ssh_commands(commands):
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname, port=port, username=username, password=password)
        
        for command in commands:
            print(f"Executando: {command}")
            stdin, stdout, stderr = client.exec_command(command)
            print(stdout.read().decode())
            error = stderr.read().decode()
            if error:
                print(f"Erro: {error}")
                
        client.close()
    except Exception as e:
        print(f"Falha na conexao: {e}")

commands = [
    "cd /opt/apidois && git fetch origin main && git reset --hard origin/main",
    "rm -rf /opt/apidois/venv",
    "python3 -m venv /opt/apidois/venv",
    "/opt/apidois/venv/bin/pip install --upgrade pip",
    "/opt/apidois/venv/bin/pip install -r /opt/apidois/requirements.txt",
    "fuser -k 8000/tcp || true",
    "systemctl restart apidois",
    "systemctl status apidois | grep Active"
]

run_ssh_commands(commands)
