import socket
import time
import argparse
import json
import threading

class Subscriber:
    def __init__(self, host, port, topicos):
        self.host = host
        self.port = port
        self.topicos = topicos
        self.cliente_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.cliente_socket.connect((self.host, self.port))
        subscribe_message = {
            "tipo": "SUB",
            "topicos": self.topicos
        }
        self.cliente_socket.send(json.dumps(subscribe_message).encode())

    def confirmacaoSolicitação(self):
        #print("oi")

        #print("oi3")
        resposta = self.cliente_socket.recv(1024).decode()
        #print("oi2")
        try:
            #print("oi")
            respostaJSON = json.loads(resposta)
            if respostaJSON.get("tipo") == "ACK" and respostaJSON.get("sucesso") == True:
                print(respostaJSON.get("content").get("mensagem"))
                return True
        except json.JSONDecodeError:
            print(f"Formato JSON invalido: {resposta}")
        return False

    def escutarServidor(self):
        print("Escutando o servidor Broker")
        while True:
            resposta = self.cliente_socket.recv(1024).decode()
            try:
                respostajS = json.loads(resposta)
                if respostajS.get("tipo") == "MENSAGEM":
                    topico = respostajS.get("topico")
                    mensagem = respostajS.get("mensagem")
                    print(f"topico: {topico}\nMensagem: {mensagem}")
                else:
                    print("mensagem do tipo errado")
                
            except json.JSONDecodeError:
                print(f"Invalid JSON format received: {resposta}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Subscribe to multiple topics.')
    parser.add_argument('-t', '--topicos', nargs='+', help='List of topics to subscribe to', required=True)
    args = parser.parse_args()
    topicos = args.topicos
    subscriber = Subscriber('localhost', 8888, topicos)
    subscriber.confirmacaoSolicitação()
    subscriber.escutarServidor()
    