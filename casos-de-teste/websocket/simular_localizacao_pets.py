"""
Script de teste: simula a movimentação de vários pets.

Cada pet possui seu próprio intervalo de movimentação,
obtido através do backend:

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

import random
import time
import sys

import requests


# =========================================================================
# CONFIGURAÇÃO
# =========================================================================

BASE_URL = "http://localhost:8080/tela-login-angular"

LOGIN_PATH = "/auth/login"

LISTAR_PATH = "/pet/all"

LOCALIZACAO_PATH = "/pet/{id}/localizacao"

# NOVO endpoint
INTERVALO_PATH = "/pet/{id}/intervalo"

EMAIL = "admin@email.com"
SENHA = "admin"


# Centro de referência para o passeio dos pets
CENTRO_LAT = -8.0476
CENTRO_LON = -34.8770

RAIO_MAX_GRAUS = 0.01

PASSO_MAX_GRAUS = 0.0008

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
# GERAR PRÓXIMA POSIÇÃO
# =========================================================================

def proxima_posicao(
    lat: float,
    lon: float
) -> tuple[float, float]:

    nova_lat = lat + random.uniform(
        -PASSO_MAX_GRAUS,
        PASSO_MAX_GRAUS
    )

    nova_lon = lon + random.uniform(
        -PASSO_MAX_GRAUS,
        PASSO_MAX_GRAUS
    )

    # Se sair do raio permitido,
    # coloca novamente dentro da área.

    if abs(nova_lat - CENTRO_LAT) > RAIO_MAX_GRAUS:
        nova_lat = CENTRO_LAT + random.uniform(
            -RAIO_MAX_GRAUS,
            RAIO_MAX_GRAUS
        )

    if abs(nova_lon - CENTRO_LON) > RAIO_MAX_GRAUS:
        nova_lon = CENTRO_LON + random.uniform(
            -RAIO_MAX_GRAUS,
            RAIO_MAX_GRAUS
        )

    return nova_lat, nova_lon


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
    # POSIÇÃO INICIAL DE CADA PET
    # -------------------------------------------------------------

    posicoes = {
        pet["id"]: (
            CENTRO_LAT + random.uniform(
                -RAIO_MAX_GRAUS,
                RAIO_MAX_GRAUS
            ),
            CENTRO_LON + random.uniform(
                -RAIO_MAX_GRAUS,
                RAIO_MAX_GRAUS
            )
        )
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
            f"intervalo: {intervalos[pet['id']]}s"
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

                    lat, lon = posicoes[pet_id]

                    nova_lat, nova_lon = proxima_posicao(
                        lat,
                        lon
                    )

                    posicoes[pet_id] = (
                        nova_lat,
                        nova_lon
                    )

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
