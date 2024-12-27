import requests
import psycopg2
from datetime import datetime

# Configurações
GITHUB_TOKEN = "your token"
REPO_OWNER = "torvalds"
REPO_NAME = "linux"
DATABASE_CONFIG = {
    "dbname": "ubuntu_data",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432,
}

# Função para buscar pull requests
def fetch_pull_requests():
    # Cabeçalho com token de autenticação (prepara HTTP header para autenticação com GitHub API)
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}

    # Url da API do GitHub para listar os pull requests de um repo
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/pulls"
    params = {"state": "all", "per_page": 100, "page": 1}
    
    # Lista para armazenar os resultados
    pull_requests = []

    # Loop para a paginação 
    while True:
        # Verifica se a resposta da API foi bem sucedida
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Erro ao buscar dados: {response.status_code} - {response.text}")
            break

        # Converte corpo da resposta para uma lista de dicionários
        data = response.json()
        if not data:
            break

        # Add os resultados à lista
        pull_requests.extend(data)
        params["page"] += 1

    return pull_requests

# Função para armazenar dados no PostgreSQL
def save_to_postgres(pull_requests):
    try:
        # Criando conexão com o banco
        conn = psycopg2.connect(**DATABASE_CONFIG)

        # Objeto para executar comandos SQL no banco
        cursor = conn.cursor()

        # Iterando sobre cada pull request
        for pr in pull_requests:
            # Inserindo os dados de um pr na tabela
            cursor.execute(
                """
                INSERT INTO pull_requests (pr_id, title, state, created_at, updated_at, closed_at, merged_at, author, repo_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pr_id) DO NOTHING;
                """,
                (
                    pr["id"],
                    pr["title"],
                    pr["state"],
                    pr["created_at"],
                    pr["updated_at"],
                    pr["closed_at"],
                    pr["merged_at"],
                    pr["user"]["login"],
                    f"{REPO_OWNER}/{REPO_NAME}",
                ),
            )

        # Confirmando mudanças
        conn.commit()
        print(f"{len(pull_requests)} pull requests inseridos com sucesso.")
    except Exception as e:
        print(f"Erro ao salvar no PostgreSQL: {e}")
    finally:
        cursor.close()
        conn.close()

# Executar o processo
if __name__ == "__main__":
    prs = fetch_pull_requests()
    print(f"{len(prs)} pull requests encontrados.")
    save_to_postgres(prs)