import zmq

def main():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.connect("tcp://broker:5556")
    
    tarefas = []
    print("Servidor de tarefas iniciado e conectado ao broker...")

    while True:
        msg = socket.recv_json()
        acao = msg.get("acao")
        
        if acao == "adicionar":
            tarefa = msg.get("tarefa")
            tarefas.append(tarefa)
            socket.send_json({"status": "sucesso", "mensagem": f"Tarefa '{tarefa}' adicionada!"})
            
        elif acao == "remover":
            tarefa = msg.get("tarefa")
            if tarefa in tarefas:
                tarefas.remove(tarefa)
                socket.send_json({"status": "sucesso", "mensagem": f"Tarefa '{tarefa}' removida!"})
            else:
                socket.send_json({"status": "erro", "mensagem": "Erro: Tarefa não encontrada."})
                
        elif acao == "listar":
            socket.send_json({"status": "sucesso", "tarefas": tarefas})
            
        else:
            socket.send_json({"status": "erro", "mensagem": "Ação desconhecida."})

if __name__ == "__main__":
    main()
