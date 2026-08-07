"""
Script de teste: simula a movimentação de vários pets, atualizando a
localização deles periodicamente para testar o WebSocket/broadcaster.

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
# CONFIGURAÇÃO — ajuste conforme o seu backend
# =========================================================================

BASE_URL = "http://localhost:8080/tela-login-angular"  # ajuste a porta/context-path se for diferente

# Ajuste se o AuthController tiver um prefixo de classe (ex: "/auth/login")
LOGIN_PATH = "/auth/login"

# Ajuste se o PetController tiver um prefixo diferente de "/pet"
LISTAR_PATH = "/pet/all"                    # GET  {BASE_URL}/pet?page=0&size=50
LOCALIZACAO_PATH = "/pet/{id}/localizacao"  # PUT {BASE_URL}/pet/{id}/localizacao

EMAIL = "admin@email.com"
SENHA = "admin"

# Centro de referência para o "passeio" dos pets (Recife, PE)
CENTRO_LAT = -8.0476
CENTRO_LON = -34.8770

RAIO_MAX_GRAUS = 0.01      # ~1km de raio de variação a partir do centro
PASSO_MAX_GRAUS = 0.0008   # o quanto cada pet "anda" a cada iteração
INTERVALO_SEGUNDOS = 3     # frequência de atualização
QTD_PETS_A_SIMULAR = 4     # quantos pets (dos já existentes) serão movimentados


# =========================================================================
# LÓGICA
# =========================================================================

def login(session: requests.Session) -> None:
    """Autentica e reenvia o cookie 'token' manualmente.
 
    O cookie de autenticação é marcado como Secure no backend (correto para
    produção), então clientes HTTP não o reenviam automaticamente em conexões
    http:// (como localhost sem TLS). Por isso extraímos o valor aqui e
    forçamos o envio via header em cada requisição subsequente.
    """
    resp = session.post(
        f"{BASE_URL}{LOGIN_PATH}",
        json={"email": EMAIL, "senha": SENHA},
    )
    resp.raise_for_status()
 
    token = resp.cookies.get("token")
    if not token:
        print("[erro] Login OK, mas o cookie 'token' não veio na resposta. "
              "Verifique o Set-Cookie no header da resposta.")
        sys.exit(1)
 
    session.headers.update({"Cookie": f"token={token}"})
    print(f"[login] OK — usuário: {resp.json().get('nome', '?')}")
 
 
def listar_pets(session: requests.Session, quantidade: int) -> list[dict]:
    url = f"{BASE_URL}{LISTAR_PATH}"
    resp = session.get(url, params={"page": 0, "size": quantidade})
 
    print(f"[debug] GET {resp.url}")
    print(f"[debug] status: {resp.status_code}")
    print(f"[debug] content-type: {resp.headers.get('Content-Type')}")
    print(f"[debug] corpo (primeiros 300 chars): {resp.text[:300]!r}\n")
 
    resp.raise_for_status()
    dados = resp.json()
    pets = dados.get("content", dados if isinstance(dados, list) else [])
    if not pets:
        print("[erro] Nenhum pet encontrado. Cadastre alguns pets antes de rodar o script.")
        sys.exit(1)
    print(f"[listar_pets] {len(pets)} pet(s) carregado(s): "
          f"{', '.join(p['nome'] for p in pets)}")
    return pets
 
 
def atualizar_localizacao(session: requests.Session, pet_id: int, lat: float, lon: float) -> None:
    url = f"{BASE_URL}{LOCALIZACAO_PATH.format(id=pet_id)}"
    resp = session.put(url, json={"latitude": lat, "longitude": lon})
    resp.raise_for_status()
 
 
def proxima_posicao(lat: float, lon: float) -> tuple[float, float]:
    """Move a posição em uma direção aleatória, mantendo dentro do raio do centro."""
    nova_lat = lat + random.uniform(-PASSO_MAX_GRAUS, PASSO_MAX_GRAUS)
    nova_lon = lon + random.uniform(-PASSO_MAX_GRAUS, PASSO_MAX_GRAUS)
 
    # se sair do raio permitido, "puxa" de volta em direção ao centro
    if abs(nova_lat - CENTRO_LAT) > RAIO_MAX_GRAUS:
        nova_lat = CENTRO_LAT + random.uniform(-RAIO_MAX_GRAUS, RAIO_MAX_GRAUS)
    if abs(nova_lon - CENTRO_LON) > RAIO_MAX_GRAUS:
        nova_lon = CENTRO_LON + random.uniform(-RAIO_MAX_GRAUS, RAIO_MAX_GRAUS)
 
    return nova_lat, nova_lon
 
 
def main() -> None:
    session = requests.Session()
    login(session)
 
    pets = listar_pets(session, QTD_PETS_A_SIMULAR)
 
    # posição inicial de cada pet: espalhados aleatoriamente perto do centro
    posicoes = {
        pet["id"]: (
            CENTRO_LAT + random.uniform(-RAIO_MAX_GRAUS, RAIO_MAX_GRAUS),
            CENTRO_LON + random.uniform(-RAIO_MAX_GRAUS, RAIO_MAX_GRAUS),
        )
        for pet in pets
    }
 
    print(f"\nSimulando movimentação de {len(pets)} pet(s) a cada "
          f"{INTERVALO_SEGUNDOS}s. Ctrl+C para parar.\n")
 
    try:
        while True:
            for pet in pets:
                lat, lon = posicoes[pet["id"]]
                nova_lat, nova_lon = proxima_posicao(lat, lon)
                posicoes[pet["id"]] = (nova_lat, nova_lon)
 
                try:
                    atualizar_localizacao(session, pet["id"], nova_lat, nova_lon)
                    print(f"  {pet['nome']:15s} -> ({nova_lat:.6f}, {nova_lon:.6f})")
                except requests.HTTPError as e:
                    print(f"  [erro] {pet['nome']}: {e}")
 
            time.sleep(INTERVALO_SEGUNDOS)
 
    except KeyboardInterrupt:
        print("\nSimulação encerrada.")
 
 
if __name__ == "__main__":
    main()
 