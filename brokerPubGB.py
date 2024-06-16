import argparse
import socket
import json
import threading
import time


class Publisher:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.cliente_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.cliente_socket.connect((self.host, self.port))

    def publish_message(self, topico, mensagem):
        payload = {
            "tipo": "PUB",
            "topico": topico,
            "mensagem": mensagem
        }
        self.cliente_socket.send(json.dumps(payload).encode())
        print(f"Published message for topic '{topico}': {mensagem}")

    def close(self):
        self.cliente_socket.close()
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
                publisher.close()
                return True
        except json.JSONDecodeError:
            print(f"Formato JSON invalido: {resposta}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='publicar uma mensagem em um topico')
    parser.add_argument('-t', '--topico', required=True, help='topico a receber a mensagem')
    parser.add_argument('-m', '--mensagem', required=True, help='mensagem a ser publicada')
    args = parser.parse_args()
    publisher = Publisher('localhost', 8888)
    publisher.publish_message(args.topico, args.mensagem)
    while publisher.confirmacaoSolicitação()==False:  # Enquanto receber True
        publisher.publish_message(args.topico, args.mensagem)
        time.sleep(1) 