import uuid

import httpx

URL = "http://127.0.0.1:9999/payments"
HEADERS = {
    "accept": "application/json",
    "Content-Type": "application/json",
}

# Número de requisições (padrão: 10)
X = 1000


def main():
    with httpx.Client() as client:
        for i in range(X):
            data = {"correlationId": str(uuid.uuid4()), "amount": 0}
            try:
                response = client.post(URL, headers=HEADERS, json=data)
                print(f"[{i + 1}/{X}] {response.status_code}: {response.text}")
            except Exception as e:
                print(f"[{i + 1}/{X}] Erro:", e)


if __name__ == "__main__":
    main()
