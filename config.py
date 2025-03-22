import subprocess
import os
import sys
import time

def run_command(command, cwd=None, shell=False):
    """Executa um comando no sistema e retorna o resultado."""
    try:
        result = subprocess.run(command, cwd=cwd, shell=shell, check=True, text=True, capture_output=True)
        print(f"Comando executado com sucesso: {' '.join(command if isinstance(command, list) else command.split())}")
        print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar o comando: {' '.join(command if isinstance(command, list) else command.split())}")
        print(f"Saída de erro: {e.stderr}")
        return False
    except Exception as e:
        print(f"Erro inesperado: {str(e)}")
        return False

def check_docker_installation():
    """Verifica se o Docker está instalado."""
    print("Verificando a instalação do Docker...")
    return run_command(["docker", "--version"])                                                                                                                                         

def create_network():
    """Cria a rede externa my_network se ela ainda não existir."""
    print("Verificando/Criando a rede externa 'my_network'...")
    # Verifica se a rede já existe
    result = subprocess.run(["docker", "network", "ls", "--filter", "name=my_network"], capture_output=True, text=True)
    if "my_network" not in result.stdout:
        return run_command(["docker", "network", "create", "my_network"])
    else:
        print("A rede 'my_network' já existe.")
        return True

def run_airflow_commands():
    """Acessa a pasta 'airflow' e executa os comandos do docker-compose."""
    airflow_dir = os.path.join(os.getcwd(), "airflow")
    if not os.path.exists(airflow_dir):
        print(f"Erro: A pasta 'airflow' não foi encontrada em {os.getcwd()}")
        return False

    print("Acessando a pasta 'airflow'...")
    # Executa o airflow-init
    if not run_command(["docker-compose", "up", "airflow-init"], cwd=airflow_dir):
        return False
    
    # Aguarda um pouco para garantir que o airflow-init termine
    time.sleep(5)
    
    # Executa o docker-compose up
    if not run_command(["docker-compose", "up", "-d"], cwd=airflow_dir):
        return False
    
    return True

def run_api_commands():
    """Acessa a pasta 'api' e executa o comando docker-compose."""
    api_dir = os.path.join(os.getcwd(), "api")
    if not os.path.exists(api_dir):
        print(f"Erro: A pasta 'api' não foi encontrada em {os.getcwd()}")
        return False

    print("Acessando a pasta 'api'...")
    return run_command(["docker-compose", "up", "--build", "-d"], cwd=api_dir)

def main():
    """Função principal que executa todas as etapas."""
    # Passo 1: Verificar instalação do Docker
    if not check_docker_installation():
        print("Docker não está instalado ou não está funcionando corretamente. Abortando.")
        sys.exit(1)

    # Passo 2: Criar a rede externa my_network
    if not create_network():
        print("Falha ao criar a rede 'my_network'. Abortando.")
        sys.exit(1)

    # Passo 3: Executar comandos na pasta airflow
    if not run_airflow_commands():
        print("Falha ao executar os comandos na pasta 'airflow'. Abortando.")
        sys.exit(1)

    # Passo 4: Executar comando na pasta api
    if not run_api_commands():
        print("Falha ao executar os comandos na pasta 'api'. Abortando.")
        sys.exit(1)

    print("Todos os comandos foram executados com sucesso!")

if __name__ == "__main__":
    main()