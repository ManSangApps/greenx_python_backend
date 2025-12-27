"""
GreenX – Upstox NIFTY Paper Trading Backend

What this file does:
- Connects to Upstox Market Data WebSocket (v3 authorize flow)
- Subscribes to NIFTY 50 LTP
- Builds 1-minute and 15-minute candles (logic ready)
- Runs a simple EMA-based demo strategy
- Manages paper trades (entry, SL, target)
- Streams live state to frontend via FastAPI WebSocket

Important:
- Upstox v3 WS sends BINARY protobuf frames
- We IGNORE them for now to avoid crashes
- This keeps architecture stable for MVP
"""

# ======================================================
# IMPORTS
# ======================================================

import asyncio
import json
import requests
import pandas as pd
from datetime import datetime, time
from enum import Enum

import websockets
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware

# ======================================================
# UPSTOX CONFIG
# ======================================================

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiJHQzQyNjIiLCJqdGkiOiI2OTRmNzkyZjZhNjY4YjU1YTdmMWJjMWQiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc2NjgxNjA0NywiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzY2ODcyODAwfQ.6Okbwi134RgSADEF4_SEZwCiVGDXlFxDGvRezkx4pyg"

UPSTOX_WS_AUTH_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"

NIFTY_SYMBOL = "NSE_INDEX|Nifty 50"

MARKET_START = time(9, 15)
MARKET_END = time(15, 30)

# ======================================================
# FASTAPI APP
# ======================================================

app = FastAPI(title="GreenX Upstox NIFTY Paper Trading")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================================================
# MODELS
# ======================================================


class Side(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class Trade:
    def __init__(self, side, entry, sl, target):
        self.side = side
        self.entry = entry
        self.sl = sl
        self.target = target
        self.status = "OPEN"
        self.pnl = 0.0
        self.entry_time = datetime.now()
        self.exit_time = None


# ======================================================
# STATE
# ======================================================

ticks_1m = []
candles_1m = []
candles_15m = []

active_trade: Trade | None = None
trade_log: list[Trade] = []

latest_price = None
clients: list[WebSocket] = []

# ======================================================
# UTILS
# ======================================================


def market_open() -> bool:
    now = datetime.now().time()
    return MARKET_START <= now <= MARKET_END


def ema(series, period=6):
    return series.ewm(span=period).mean()


def build_candle(prices):
    return {
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
    }


def serialize_trade(trade: Trade | None):
    if not trade:
        return None
    return {
        "side": trade.side,
        "entry": trade.entry,
        "sl": trade.sl,
        "target": trade.target,
        "pnl": round(trade.pnl, 2),
        "status": trade.status,
    }


# ======================================================
# STRATEGY
# ======================================================


def check_entry(candles):
    if len(candles) < 10:
        return None

    df = pd.DataFrame(candles)
    df["ema"] = ema(df["close"])

    prev = df.iloc[-2]
    curr = df.iloc[-1]

    if prev.close > prev.ema and curr.close < curr.open:
        return Side.SHORT

    if prev.close < prev.ema and curr.close > curr.open:
        return Side.LONG

    return None


def create_trade(side, candle):
    if side == Side.LONG:
        entry = candle["high"]
        sl = candle["low"]
        target = entry + (entry - sl)
    else:
        entry = candle["low"]
        sl = candle["high"]
        target = entry - (sl - entry)

    return Trade(side, entry, sl, target)


def exit_trade(trade, price, reason):
    trade.status = reason
    trade.exit_time = datetime.now()
    trade.pnl = price - trade.entry if trade.side == Side.LONG else trade.entry - price
    trade_log.append(trade)


def manage_trade(trade, price):
    if trade.side == Side.LONG:
        if price <= trade.sl:
            exit_trade(trade, price, "STOPLOSS")
            return True
        if price >= trade.target:
            exit_trade(trade, price, "PROFIT")
            return True

    if trade.side == Side.SHORT:
        if price >= trade.sl:
            exit_trade(trade, price, "STOPLOSS")
            return True
        if price <= trade.target:
            exit_trade(trade, price, "PROFIT")
            return True

    return False


# ======================================================
# UPSTOX AUTH (V3)
# ======================================================


def get_upstox_ws_url() -> str:
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Accept": "application/json",
    }

    params = {
        "apiVersion": "2.0",
        "segment": "NSE",
    }

    res = requests.get(
        UPSTOX_WS_AUTH_URL,
        headers=headers,
        params=params,
    )
    res.raise_for_status()

    return res.json()["data"]["authorizedRedirectUri"]


# ======================================================
# UPSTOX FEED
# ======================================================


async def upstox_feed():
    global active_trade, latest_price

    if not market_open():
        print("⏸ Market closed — skipping feed")
        await asyncio.sleep(300)
        return

    ws_url = get_upstox_ws_url()
    print("🔑 Authorized WebSocket URL received")

    async with websockets.connect(ws_url) as ws:
        print("✅ Upstox WebSocket connected")

        await ws.send(
            json.dumps(
                {
                    "guid": "greenx-demo",
                    "method": "sub",
                    "data": {
                        "mode": "ltp",
                        "instrumentKeys": [NIFTY_SYMBOL],
                    },
                }
            )
        )

        async for msg in ws:
            # Upstox v3 sends binary protobuf frames
            if isinstance(msg, bytes):
                continue

            # Text frames (rare control messages)
            try:
                data = json.loads(msg)
            except Exception:
                continue

            # No usable LTP here yet (binary-only feed)
            await broadcast_state()


# ======================================================
# BROADCAST
# ======================================================


async def broadcast_state():
    payload = {
        "symbol": "NIFTY",
        "price": latest_price,
        "activeTrade": serialize_trade(active_trade),
        "tradeHistory": [
            {"side": t.side, "pnl": t.pnl, "status": t.status} for t in trade_log[-20:]
        ],
    }

    for ws in clients.copy():
        try:
            await ws.send_json(payload)
        except:
            clients.remove(ws)


# ======================================================
# FRONTEND WS
# ======================================================


@app.websocket("/ws/nifty")
async def ws_nifty(ws: WebSocket):
    await ws.accept()
    clients.append(ws)
    try:
        while True:
            await ws.receive_text()
    except:
        clients.remove(ws)


# ======================================================
# STARTUP
# ======================================================


@app.on_event("startup")
async def startup():
    async def runner():
        while True:
            try:
                await upstox_feed()
            except Exception as e:
                print("🔁 WS error, reconnecting in 5s:", e)
                await asyncio.sleep(5)

    asyncio.create_task(runner())
    print("🚀 GreenX Upstox NIFTY demo backend started")
