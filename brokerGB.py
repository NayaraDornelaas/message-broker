import socket
import threading
import json
import time

class MessageBroker:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.subinscritos = {}  # Dicionário para armazenar os tópicos e os subinscritos associados
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Message Broker listening on {self.host}:{self.port}")

    def confirmacaoACK(self,cliente_socket):
        time.sleep(2)
        cliente_socket.send(json.dumps({"tipo":"ACK","sucesso":True,"content":{"mensagem":"Solicitação processada"}}).encode())

    def handle_client(self, client_socket, addr):
        print(f"Accepted connection from {addr}")
        while True:
            try:
                message = client_socket.recv(1024).decode()
                if not message:
                    print(f"Connection from {addr} closed")
                    break

                try:
                    respostaJson = json.loads(message)
                    respostaTipo = respostaJson.get("tipo")
                    if respostaTipo == 'SUB':
                        topicos = respostaJson.get("topicos")
                        for topico in topicos:
                            if topico not in self.subinscritos:
                                self.subinscritos[topico] = []
                            self.subinscritos[topico].append(client_socket)
                            print(f"cliente {addr} se inscreveu no topico: {topico}")
                        self.confirmacaoACK(client_socket)
                    elif respostaTipo == 'PUB':
                        topico = respostaJson.get("topico")
                        mensagem = respostaJson.get("mensagem")
                        if topico in self.subinscritos:
                            for sub_socket in self.subinscritos[topico]:
                                sub_socket.send(json.dumps({"tipo":"MENSAGEM","topico": topico,"mensagem": mensagem}).encode())
                            self.confirmacaoACK(client_socket)
                    elif respostaTipo == 'COMANDO':
                        comando = respostaJson.get("comando")
                        if comando == "LIST":
                            tabela_dados = {}
                            for chave, sockets in self.subinscritos.items():
                                ips_unicos = set()
                                for socket_info in sockets:
                                    raddr = f'{socket_info.getpeername()[0]}:{socket_info.getpeername()[1]}'
                                    ips_unicos.add(raddr)
                                tabela_dados[chave] = list(ips_unicos)
                                print(tabela_dados)  # Converter o set para lista antes de armazenar no dicionário
                            client_socket.send(json.dumps({"type":"RESPONSE","content":tabela_dados}).encode())

                        else:
                            print(f"Comando não encontrado do {addr}: {message}")
                    else:
                        print(f"Invalid message type received from {addr}: {message}")
                except json.JSONDecodeError:
                    print(f"Invalid JSON format received from {addr}")
            except Exception as e:
                print(f"Error occurred while processing message from {addr}: {e}")

    def run(self):
        while True:
            client_socket, addr = self.server_socket.accept()
            client_handler = threading.Thread(target=self.handle_client, args=(client_socket, addr))
            client_handler.start()

if __name__ == "__main__":
    broker = MessageBroker('localhost', 8888)
    broker.run()
