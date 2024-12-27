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


def fetch_comments_for_pr(pr_id, pull_number):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/issues/{pull_number}/comments"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error when trying to get the comments of the PR #{pull_number}: {response.status_code}")
        return []

    return response.json() 


def save_comments_to_postgres(pr_id, comments):
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()

        for comment in comments:
            cursor.execute(
                """
                INSERT INTO pr_comments (comment_id, pr_id, body, author, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (comment_id) DO NOTHING;
                """,
                (
                    comment["id"],
                    pr_id,
                    comment["body"],
                    comment["user"]["login"],
                    comment["created_at"],
                    comment["updated_at"],
                ),
            )

        conn.commit()
        print(f"{len(comments)} comments inserted for the PR {pr_id}")

    except Exception as e:
        print(f"Error when saving the comments: {e}")

    finally:
        cursor.close()
        conn.close()


# Função para armazenar dados no PostgreSQL
def save_to_postgres(pull_requests):
    try:
        # Criando conexão com o banco
        conn = psycopg2.connect(**DATABASE_CONFIG)

        # Objeto para executar comandos SQL no banco
        cursor = conn.cursor()

        # Iterando sobre cada pull request
        for pr in pull_requests:
            pr_id = pr["id"]
            pull_number = pr["number"]
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

            comments = fetch_comments_for_pr(pr_id, pull_number)
            save_comments_to_postgres(pr_id, comments)

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