"""
Script de teste: simula a movimentação de vários pets.

Cada pet percorre uma ROTA FIXA de coordenadas (em ordem, ciclicamente),
com intervalo de movimentação próprio, obtido através do backend:

    GET /pet/{id}/intervalo

Exemplo:

    Pet 1 -> movimenta a cada 2 segundos
    Pet 2 -> movimenta a cada 5 segundos
    Pet 3 -> movimenta a cada 10 segundos

Requisitos:
    pip install requests

Como usar:
    python simular_localizacao_pets.py
    (Ctrl+C para parar)
"""

import sys
import time

import requests


# =========================================================================
# CONFIGURAÇÃO
# =========================================================================

BASE_URL = "http://localhost:8080/tela-login-angular"

LOGIN_PATH = "/auth/login"

LISTAR_PATH = "/pet/all"

LOCALIZACAO_PATH = "/pet/{id}/localizacao"

INTERVALO_PATH = "/pet/{id}/intervalo"

EMAIL = "admin@email.com"
SENHA = "admin"


# Rota fixa que os pets percorrem, em ordem, ciclicamente (sem coordenadas
# aleatórias — apenas estes pontos).
ROTA = [
    (-8.0387, -34.9642),
    (-8.0350, -34.9614),
    (-8.0336, -34.9598),
    (-8.0342, -34.9586),
    (-8.0301, -34.9576),
    (-8.0323, -34.9539),
    (-8.0311, -34.9514),
]

QTD_PETS_A_SIMULAR = 4


# =========================================================================
# LOGIN
# =========================================================================

def login(session: requests.Session) -> None:
    """Faz login e configura o cookie de autenticação."""

    resp = session.post(
        f"{BASE_URL}{LOGIN_PATH}",
        json={
            "email": EMAIL,
            "senha": SENHA
        },
    )

    resp.raise_for_status()

    token = resp.cookies.get("token")

    if not token:
        print(
            "[erro] Login OK, mas o cookie 'token' "
            "não veio na resposta."
        )
        sys.exit(1)

    session.headers.update({
        "Cookie": f"token={token}"
    })

    print(
        f"[login] OK — usuário: "
        f"{resp.json().get('nome', '?')}"
    )


# =========================================================================
# LISTAR PETS
# =========================================================================

def listar_pets(
    session: requests.Session,
    quantidade: int
) -> list[dict]:

    url = f"{BASE_URL}{LISTAR_PATH}"

    resp = session.get(
        url,
        params={
            "page": 0,
            "size": quantidade
        }
    )

    print(f"[debug] GET {resp.url}")
    print(f"[debug] status: {resp.status_code}")
    print(
        f"[debug] content-type: "
        f"{resp.headers.get('Content-Type')}"
    )
    print(
        f"[debug] corpo (primeiros 300 chars): "
        f"{resp.text[:300]!r}\n"
    )

    resp.raise_for_status()

    dados = resp.json()

    pets = dados.get(
        "content",
        dados if isinstance(dados, list) else []
    )

    if not pets:
        print(
            "[erro] Nenhum pet encontrado. "
            "Cadastre alguns pets antes de rodar o script."
        )
        sys.exit(1)

    print(
        f"[listar_pets] {len(pets)} pet(s) carregado(s): "
        f"{', '.join(p['nome'] for p in pets)}"
    )

    return pets


# =========================================================================
# BUSCAR INTERVALO DO PET
# =========================================================================

def buscar_intervalo(
    session: requests.Session,
    pet_id: int
) -> int:

    url = f"{BASE_URL}{INTERVALO_PATH.format(id=pet_id)}"

    resp = session.get(url)

    resp.raise_for_status()

    intervalo = resp.json()

    # Garante que seja inteiro
    intervalo = int(intervalo)

    # Evita intervalo 0 ou negativo
    if intervalo <= 0:
        print(
            f"[aviso] Pet {pet_id} possui intervalo "
            f"{intervalo}. Usando 1 segundo."
        )
        intervalo = 1

    return intervalo


# =========================================================================
# ATUALIZAR LOCALIZAÇÃO
# =========================================================================

def atualizar_localizacao(
    session: requests.Session,
    pet_id: int,
    lat: float,
    lon: float
) -> None:

    url = f"{BASE_URL}{LOCALIZACAO_PATH.format(id=pet_id)}"

    resp = session.put(
        url,
        json={
            "latitude": lat,
            "longitude": lon
        }
    )

    resp.raise_for_status()


# =========================================================================
# PRÓXIMA POSIÇÃO NA ROTA FIXA
# =========================================================================

def proxima_posicao_rota(indice_atual: int) -> tuple[tuple[float, float], int]:
    """Retorna a coordenada do próximo ponto da ROTA e o novo índice,
    avançando ciclicamente (volta ao início ao chegar no fim)."""

    proximo_indice = (indice_atual + 1) % len(ROTA)
    return ROTA[proximo_indice], proximo_indice


# =========================================================================
# MAIN
# =========================================================================

def main() -> None:

    session = requests.Session()

    # -------------------------------------------------------------
    # LOGIN
    # -------------------------------------------------------------

    login(session)

    # -------------------------------------------------------------
    # BUSCA OS PETS
    # -------------------------------------------------------------

    pets = listar_pets(
        session,
        QTD_PETS_A_SIMULAR
    )

    # -------------------------------------------------------------
    # POSIÇÃO INICIAL DE CADA PET NA ROTA
    # (espalha cada pet em um ponto diferente da rota, pra ficar
    # mais fácil visualizar todos se mexendo ao mesmo tempo)
    # -------------------------------------------------------------

    indice_rota = {
        pet["id"]: i % len(ROTA)
        for i, pet in enumerate(pets)
    }

    posicoes = {
        pet["id"]: ROTA[indice_rota[pet["id"]]]
        for pet in pets
    }

    # -------------------------------------------------------------
    # BUSCA O INTERVALO DE CADA PET
    # -------------------------------------------------------------

    intervalos = {}

    for pet in pets:

        try:

            intervalo = buscar_intervalo(
                session,
                pet["id"]
            )

            intervalos[pet["id"]] = intervalo

            print(
                f"[intervalo] {pet['nome']:15s} "
                f"-> {intervalo}s"
            )

        except requests.HTTPError as e:

            print(
                f"[erro] Não foi possível obter "
                f"o intervalo do pet {pet['nome']}: {e}"
            )

            # Se não conseguir buscar o intervalo,
            # usa 3 segundos como fallback.
            intervalos[pet["id"]] = 3

    # -------------------------------------------------------------
    # CONTROLA QUANDO CADA PET DEVE SE MOVER
    # -------------------------------------------------------------

    agora = time.monotonic()

    proximo_movimento = {
        pet["id"]: agora
        for pet in pets
    }

    print("\n============================================")
    print("SIMULAÇÃO INICIADA")
    print("============================================")

    for pet in pets:

        print(
            f"{pet['nome']:15s} -> "
            f"intervalo: {intervalos[pet['id']]}s "
            f"(inicia no ponto {indice_rota[pet['id']]} da rota)"
        )

    print("\nCtrl+C para parar.\n")

    # -------------------------------------------------------------
    # LOOP PRINCIPAL
    # -------------------------------------------------------------

    try:

        while True:

            agora = time.monotonic()

            for pet in pets:

                pet_id = pet["id"]

                # Verifica se já chegou a hora
                # desse pet se movimentar.

                if agora >= proximo_movimento[pet_id]:

                    (nova_lat, nova_lon), novo_indice = proxima_posicao_rota(
                        indice_rota[pet_id]
                    )

                    indice_rota[pet_id] = novo_indice
                    posicoes[pet_id] = (nova_lat, nova_lon)

                    try:

                        atualizar_localizacao(
                            session,
                            pet_id,
                            nova_lat,
                            nova_lon
                        )

                        print(
                            f"[movimento] "
                            f"{pet['nome']:15s} -> "
                            f"ponto {novo_indice} "
                            f"({nova_lat:.6f}, "
                            f"{nova_lon:.6f}) "
                            f"[intervalo: "
                            f"{intervalos[pet_id]}s]"
                        )

                    except requests.HTTPError as e:

                        print(
                            f"[erro] "
                            f"{pet['nome']}: {e}"
                        )

                    # Agenda o próximo movimento
                    # SOMENTE para esse pet.

                    proximo_movimento[pet_id] = (
                        agora + intervalos[pet_id]
                    )

            # Pequena pausa para não consumir 100% da CPU.
            time.sleep(0.1)

    except KeyboardInterrupt:

        print("\nSimulação encerrada.")


# =========================================================================
# EXECUÇÃO
# =========================================================================

if __name__ == "__main__":
    main()