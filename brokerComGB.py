import socket
import json
import argparse
import pandas as pd

class CommandSender:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.host, self.port))

    def send_command(self, command_type):
        command = {
            "tipo": "COMANDO",
            "comando": command_type
        }
        self.client_socket.send(json.dumps(command).encode())
        response = self.client_socket.recv(1024).decode()
        return response

    def close_connection(self):
        self.client_socket.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Send commands to the server.')
    parser.add_argument('-c', '--command', help='Command to send to the server', required=True)
    args = parser.parse_args()
    command = args.command

    command_sender = CommandSender('localhost', 8888)

    try:
        # Enviar o comando para o servidor
        response = command_sender.send_command(command)
        data = json.loads(response)
        for key, value in data.items():
            formatted_addresses = ', '.join(value)
            print(f"{key}: {formatted_addresses}")
    finally:
        # Fechar a conexão com o servidor
        command_sender.close_connection()