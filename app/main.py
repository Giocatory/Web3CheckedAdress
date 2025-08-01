import os
from datetime import datetime
from typing import List, Optional, Any, Dict

from fastapi import FastAPI, HTTPException
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv
from web3 import Web3
from web3.exceptions import BadFunctionCallOutput
import httpx
from cachetools import TTLCache

# --- попытка импортировать PoA middleware (Polygon) ---
use_poa = False
try:
    from web3.middleware.geth_poa import geth_poa_middleware

    use_poa = True
except ImportError:
    try:
        from web3.middleware import geth_poa_middleware  # fallback

        use_poa = True
    except ImportError:
        print("[warning] PoA middleware не найден, продолжаем без него.")

# --- загрузка конфигов ---
load_dotenv()
POLYGON_RPC_URL = os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")
TOKEN_ADDRESS = os.getenv(
    "TOKEN_ADDRESS", "0x1a9b54a3075119f1546c52ca0940551a6ce5d2d0"
)
POLYGONSCAN_API_KEY = os.getenv("POLYGONSCAN_API_KEY", None)
POLYGONSCAN_BASE = "https://api.polygonscan.com/api"

# --- инициализация Web3 ---
w3 = Web3(Web3.HTTPProvider(POLYGON_RPC_URL))
if use_poa:
    try:
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    except Exception:
        # иногда объект отличается, но тихо продолжаем
        print("[warning] Не удалось инжектить PoA middleware, продолжаем.")

# --- минимальный ERC20 ABI ---
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "totalSupply",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
]

# --- Pydantic модели ---
class BatchRequest(BaseModel):
    addresses: List[str]


class TopRequest(BaseModel):
    addresses: List[str]
    n: Optional[int] = 10


class CallContractRequest(BaseModel):
    contract_address: str
    abi: List[Any]
    method: str
    args: Optional[List[Any]] = []
    kwargs: Optional[Dict[str, Any]] = {}


# --- кэш токен-метаданных ---
token_info_cache = TTLCache(maxsize=10, ttl=300)


# --- вспомогательные функции ---
def to_checksum(address: str) -> str:
    try:
        return w3.to_checksum_address(address)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Неверный адрес: {address}")


def get_contract(address: str, abi: List[dict]):
    return w3.eth.contract(address=to_checksum(address), abi=abi)


def fetch_token_contract():
    return get_contract(TOKEN_ADDRESS, ERC20_ABI)


def human_balance(raw: int, decimals: int) -> float:
    return raw / (10 ** decimals)


def get_token_metadata():
    key = TOKEN_ADDRESS.lower()
    if key in token_info_cache:
        return token_info_cache[key]

    contract = fetch_token_contract()
    try:
        symbol = contract.functions.symbol().call()
    except Exception:
        symbol = None
    try:
        name = contract.functions.name().call()
    except Exception:
        name = None
    try:
        decimals = contract.functions.decimals().call()
    except Exception:
        decimals = 18
    try:
        total_supply_raw = contract.functions.totalSupply().call()
    except Exception:
        total_supply_raw = 0
    total_supply = total_supply_raw / (10 ** decimals) if decimals else 0
    info = {
        "symbol": symbol,
        "name": name,
        "decimals": decimals,
        "totalSupply": total_supply,
        "raw_totalSupply": total_supply_raw,
    }
    token_info_cache[key] = info
    return info


def get_balance_of(address: str) -> Dict[str, Any]:
    contract = fetch_token_contract()
    try:
        raw = contract.functions.balanceOf(to_checksum(address)).call()
    except BadFunctionCallOutput:
        raise HTTPException(
            status_code=400,
            detail=f"Невозможно получить баланс для {address}, возможно, неверный адрес.",
        )
    metadata = get_token_metadata()
    human = human_balance(raw, metadata["decimals"])
    return {
        "address": to_checksum(address),
        "raw_balance": str(raw),
        "balance": human,
        "symbol": metadata["symbol"],
    }


def get_balances_batch(addresses: List[str]) -> List[Dict[str, Any]]:
    out = []
    for addr in addresses:
        try:
            out.append(get_balance_of(addr))
        except Exception as e:
            out.append({"address": addr, "error": str(e)})
    return out


def fetch_last_token_tx_date(address: str) -> Optional[str]:
    if not POLYGONSCAN_API_KEY:
        return None
    params = {
        "module": "account",
        "action": "tokentx",
        "contractaddress": TOKEN_ADDRESS,
        "address": address,
        "page": 1,
        "offset": 1,
        "sort": "desc",
        "apikey": POLYGONSCAN_API_KEY,
    }
    try:
        resp = httpx.get(POLYGONSCAN_BASE, params=params, timeout=10)
        data = resp.json()
    except Exception:
        return None
    if data.get("status") != "1":
        return None
    result = data.get("result", [])
    if not result:
        return None
    tx = result[0]
    timestamp = int(tx.get("timeStamp", 0))
    dt = datetime.utcfromtimestamp(timestamp)
    return dt.isoformat() + "Z"


def get_top_from_list(candidate_addresses: List[str], top_n: int) -> List[Dict[str, Any]]:
    balances = get_balances_batch(candidate_addresses)
    valid = []
    for b in balances:
        if b.get("balance") is not None:
            valid.append(
                {
                    "address": b["address"],
                    "balance": b["balance"],
                    "raw": b["raw_balance"],
                }
            )
    sorted_list = sorted(valid, key=lambda x: x["balance"], reverse=True)
    return sorted_list[:top_n]


def get_top_with_tx_dates(candidate_addresses: List[str], top_n: int) -> List[Dict[str, Any]]:
    top = get_top_from_list(candidate_addresses, top_n)
    for item in top:
        item["last_transaction_date"] = fetch_last_token_tx_date(item["address"])
    return top


# --- FastAPI приложение ---
app = FastAPI(default_response_class=ORJSONResponse, title="DeNet Python Hero TBY API")


@app.on_event("startup")
def startup_event():
    try:
        block = w3.eth.block_number
        print(f"[startup] connected to RPC. Latest block: {block}")
    except Exception as e:
        print("[startup] RPC connection failed:", e)


@app.get("/healthz", summary="Проверка живости")
def healthz():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}


@app.get("/get_balance", summary="Уровень A: баланс одного адреса")
def api_get_balance(address: str):
    res = get_balance_of(address)
    return {
        "balance": res["balance"],
        "raw_balance": res["raw_balance"],
        "symbol": res["symbol"],
        "address": res["address"],
    }


@app.post("/get_balance_batch", summary="Уровень B: баланс нескольких адресов")
def api_get_balance_batch(req: BatchRequest):
    result = get_balances_batch(req.addresses)
    return {"balances": result}


@app.post("/get_top", summary="Уровень C: топ адресов по балансу из предоставленного списка")
def api_get_top(req: TopRequest):
    top = get_top_from_list(req.addresses, req.n or 10)
    return {"top": top}


@app.post("/get_top_with_transactions", summary="Уровень D: топ с датами последних транзакций")
def api_get_top_with_transactions(req: TopRequest):
    top = get_top_with_tx_dates(req.addresses, req.n or 10)
    return {"top": top}


@app.get("/get_token_info", summary="Уровень E: информация о токене")
def api_get_token_info():
    info = get_token_metadata()
    return {
        "symbol": info["symbol"],
        "name": info["name"],
        "decimals": info["decimals"],
        "totalSupply": info["totalSupply"],
        "raw_totalSupply": str(info["raw_totalSupply"]),
        "address": TOKEN_ADDRESS,
    }


@app.post("/call_contract", summary="Уровень E/F: произвольный вызов контракта")
def api_call_contract(req: CallContractRequest):
    try:
        contract = get_contract(req.contract_address, req.abi)
        func = getattr(contract.functions, req.method)
        call_obj = func(*req.args, **(req.kwargs or {}))
        result = call_obj.call()
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Ошибка вызова контракта: {e}")


@app.exception_handler(Exception)
def global_exception_handler(request, exc):
    return ORJSONResponse(
        status_code=500, content={"error": "Internal Server Error", "detail": str(exc)}
    )
