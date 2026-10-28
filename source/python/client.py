import zmq
import sys

def main():
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://broker:5555")
    
    while True:
        print("\n=== GERENCIADOR DE TAREFAS ===")
        print("1. Adicionar tarefa")
        print("2. Remover tarefa")
        print("3. Listar tarefas")
        print("4. Sair")
        
        opcao = input("Escolha uma opção: ").strip() 
        
        if opcao == "1":
            tarefa = input("Digite a descrição da tarefa: ").strip()
            socket.send_json({"acao": "adicionar", "tarefa": tarefa})
            resposta = socket.recv_json()
            print(f">> {resposta['mensagem']}")
            
        elif opcao == "2":
            tarefa = input("Digite o nome exato da tarefa a remover: ").strip()
            socket.send_json({"acao": "remover", "tarefa": tarefa})
            resposta = socket.recv_json()
            print(f">> {resposta['mensagem']}")
            
        elif opcao == "3":
            socket.send_json({"acao": "listar"})
            resposta = socket.recv_json()
            print("\n>> SUAS TAREFAS:")
            lista = resposta.get("tarefas", [])
            if not lista:
                print("   Nenhuma tarefa cadastrada.")
            else:
                for i, t in enumerate(lista, 1):
                    print(f"   {i}. {t}")
                    
        elif opcao == "4":
            print("Encerrando o cliente...")
            break
            
        else:
            print(">> Opção inválida, tente novamente.")

if __name__ == "__main__":
    main()
