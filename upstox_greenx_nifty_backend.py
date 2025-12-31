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
import os
import time
from datetime import datetime, time as time_obj
from enum import Enum
from typing import List, Optional

import requests
import websockets
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import MarketDataFeedV3_pb2 as pb

# ======================================================
# UPSTOX CONFIG
# ======================================================

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiJHQzQyNjIiLCJqdGkiOiI2OTU0OWQ1NTQ2NmZiMjEyODM2NjM0OTAiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc2NzE1Mjk4MSwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzY3MjE4NDAwfQ.HfXGHmeOQdyX7ow4TF3q8fXMJwr6Pn5EwuZKQuie7UY"

UPSTOX_WS_AUTH_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"

NIFTY_SYMBOL = "NSE_INDEX|Nifty 50"

UPSTOX_SUB_MODE = os.getenv("UPSTOX_SUB_MODE", "ltpc")

MARKET_START = time_obj(9, 15)
MARKET_END = time_obj(15, 0)
LAST_ENTRY_TIME = time_obj(14, 30)

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

active_trade: Optional[Trade] = None
trade_log: List[Trade] = []

latest_price = None
last_candle_time = None
clients: List[WebSocket] = []

last_ws_ltp_ts: Optional[float] = None

# ======================================================
# UTILS
# ======================================================


def market_open() -> bool:
    now = datetime.now().time()
    return MARKET_START <= now <= MARKET_END


def get_instrument_keys() -> List[str]:
    raw = os.getenv("UPSTOX_INSTRUMENT_KEYS")
    if raw:
        keys = [k.strip() for k in raw.split(",") if k.strip()]
        if keys:
            return keys
    return [NIFTY_SYMBOL]


def get_ltp_rest(instrument_key: str) -> Optional[float]:
    try:
        # Upstox documentation commonly shows v2 for market-quote/ltp.
        # Response keys may not always match the requested instrument_key exactly,
        # so parse defensively.
        url = "https://api.upstox.com/v2/market-quote/ltp"
        headers = {
            "Authorization": f"Bearer {ACCESS_TOKEN}",
            "Accept": "application/json",
        }
        res = requests.get(
            url, headers=headers, params={"instrument_key": instrument_key}
        )
        res.raise_for_status()
        payload = res.json()
        data = payload.get("data") or {}

        item = data.get(instrument_key)
        if not item and len(data) == 1:
            item = next(iter(data.values()))

        if isinstance(item, dict):
            for field in ("last_price", "ltp", "lastPrice", "LastTradedPrice"):
                val = item.get(field)
                if isinstance(val, (int, float)) and val > 0:
                    return float(val)

            # Some endpoints return a nested object
            ltpc = item.get("ltpc")
            if isinstance(ltpc, dict):
                val = ltpc.get("ltp")
                if isinstance(val, (int, float)) and val > 0:
                    return float(val)

        if data:
            print(
                f"⚠️ REST LTP check: unable to extract price. instrument_key={instrument_key} keys={list(data.keys())}"
            )
    except Exception as e:
        print(f"⚠️ REST LTP check failed for {instrument_key}: {e}")
    return None


def ema(series, period=6):
    """Simple EMA calculation without pandas"""
    if len(series) < period:
        return sum(series) / len(series)  # Simple average if not enough data

    multiplier = 2 / (period + 1)
    ema_val = series[0]
    for price in series[1:]:
        ema_val = (price * multiplier) + (ema_val * (1 - multiplier))
    return ema_val


def build_candle(prices):
    return {
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
    }


def serialize_trade(trade: Optional[Trade]):
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


def build_15m_candle():
    if len(candles_1m) < 15:
        return

    last_15 = candles_1m[-15:]

    candle = {
        "open": last_15[0]["open"],
        "high": max(c["high"] for c in last_15),
        "low": min(c["low"] for c in last_15),
        "close": last_15[-1]["close"],
    }

    closes = [c["close"] for c in candles_15m] + [candle["close"]]
    candle["ema6"] = ema(closes, 6)
    candle["timestamp"] = datetime.now().isoformat()

    candles_15m.append(candle)
    print(f"🕯️ 15m candle closed | C:{candle['close']} EMA6:{round(candle['ema6'],2)}")


# ======================================================
# STRATEGY
# ======================================================


def check_entry(candles):
    """
    GreenX 6 EMA Strategy (STRICT Handbook Version)
    Timeframe: 15 minutes ONLY
    """

    if len(candles) < 3:
        return None

    if datetime.now().time() >= LAST_ENTRY_TIME:
        return None

    prev = candles[-3]
    trade = candles[-2]

    def body_touches_ema(c):
        return min(c["open"], c["close"]) <= c["ema6"] <= max(c["open"], c["close"])

    if body_touches_ema(prev) or body_touches_ema(trade):
        return None

    prev_red = prev["close"] < prev["open"]
    prev_green = prev["close"] > prev["open"]
    trade_red = trade["close"] < trade["open"]
    trade_green = trade["close"] > trade["open"]

    # SHORT
    if (
        prev["close"] > prev["ema6"]
        and trade["close"] > trade["ema6"]
        and prev_red
        and trade_green
        and trade["low"] <= trade["ema6"] <= trade["high"]
        and latest_price is not None
        and latest_price < trade["low"]
    ):
        return {
            "side": Side.SHORT,
            "entry": trade["low"],
            "sl": trade["high"],
        }

    # LONG
    if (
        prev["close"] < prev["ema6"]
        and trade["close"] < trade["ema6"]
        and prev_green
        and trade_red
        and trade["low"] <= trade["ema6"] <= trade["high"]
        and latest_price is not None
        and latest_price > trade["high"]
    ):
        return {
            "side": Side.LONG,
            "entry": trade["high"],
            "sl": trade["low"],
        }

    return None


def create_trade(signal):
    entry = signal["entry"]
    sl = signal["sl"]
    target = (
        entry + (entry - sl) if signal["side"] == Side.LONG else entry - (sl - entry)
    )
    return Trade(signal["side"], entry, sl, target)


def exit_trade(trade, price, reason):
    trade.status = reason
    trade.exit_time = datetime.now()
    trade.pnl = price - trade.entry if trade.side == Side.LONG else trade.entry - price
    trade_log.append(trade)


def manage_trade(trade, price):
    if trade.side == Side.LONG:
        if price <= trade.sl or price >= trade.target:
            trade.status = "EXIT"
    else:
        if price >= trade.sl or price <= trade.target:
            trade.status = "EXIT"

    if trade.status == "EXIT":
        trade.exit_time = datetime.now()
        trade.pnl = (
            price - trade.entry if trade.side == Side.LONG else trade.entry - price
        )
        trade_log.append(trade)
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

    print(f"🔑 Requesting WebSocket URL from: {UPSTOX_WS_AUTH_URL}")
    print(f"🔑 Using token: {ACCESS_TOKEN[:20]}...")

    res = requests.get(
        UPSTOX_WS_AUTH_URL,
        headers=headers,
    )
    res.raise_for_status()

    payload = res.json()
    print(f"🔑 Authorize response: {payload}")

    data = payload.get("data") or {}
    ws_url = data.get("authorizedRedirectUri") or data.get("authorized_redirect_uri")
    if not ws_url:
        raise KeyError(f"Authorize response missing ws url. Keys: {list(data.keys())}")

    print(f"🔑 WebSocket URL obtained: {ws_url[:50]}...")
    return ws_url


# ======================================================
# UPSTOX FEED
# ======================================================


async def upstox_feed():
    global active_trade, latest_price

    if not market_open():
        print(
            "⏸ Market closed (by local clock) — connecting anyway to read market status"
        )

    ws_url = get_upstox_ws_url()
    print("🔑 Authorized WebSocket URL received")

    # Upstox sends its own keepalive pings. Disabling client pings avoids
    # disconnects if the server doesn't reply to client-initiated pings.
    async with websockets.connect(ws_url, ping_interval=None) as ws:
        print("✅ Upstox WebSocket connected")

        # Subscribe to NIFTY data
        instrument_keys = get_instrument_keys()
        await ws.send(
            json.dumps(
                {
                    "guid": "greenx-demo",
                    "method": "sub",
                    "data": {
                        "mode": UPSTOX_SUB_MODE,
                        "instrumentKeys": instrument_keys,
                    },
                }
            )
        )

        print(
            f"📡 Subscription sent | mode={UPSTOX_SUB_MODE} | instruments={instrument_keys}"
        )

        # Quick sanity check: validate instrument key via REST LTP once
        try:
            test_key = instrument_keys[0] if instrument_keys else None
            if test_key:
                rest_ltp = get_ltp_rest(test_key)
                print(f"🧪 REST LTP check | {test_key} => {rest_ltp}")
        except Exception as e:
            print(f"⚠️ REST LTP sanity check error: {e}")

        last_live_tick_ts = time.time()
        warned_no_live = False

        last_any_msg_ts = time.time()
        warned_silent = False

        while True:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                last_any_msg_ts = time.time()
            except asyncio.TimeoutError:
                if not warned_silent and (time.time() - last_any_msg_ts) > 15:
                    print(
                        "⚠️ WebSocket connected but no messages received for 15s after subscribe. "
                        "This usually indicates an invalid instrument key or a server-side stream issue."
                    )
                    warned_silent = True

                if not warned_no_live and (time.time() - last_live_tick_ts) > 15:
                    print(
                        "⚠️ No live_feed ticks received in 15s after subscribe. "
                        "Try UPSTOX_SUB_MODE=full and/or verify instrument keys via REST LTP endpoint."
                    )
                    warned_no_live = True
                continue

            # Handle both JSON text and binary messages from Upstox
            if isinstance(msg, str):
                try:
                    # Some connections may send keepalive as a plain text message
                    if msg.strip().lower() == "ping":
                        print("🏓 Received ping (text) - sending pong")
                        await ws.send("pong")
                        continue

                    data = json.loads(msg)
                    print(f"🔍 Received JSON: {data}")

                    # Handle market info (first message)
                    if data.get("type") == "market_info":
                        print("📊 Market status received")
                        market_info = data.get("marketInfo", {})
                        segment_status = market_info.get("segmentStatus", {})

                        nse_index_status = segment_status.get("NSE_INDEX")
                        print(f"🏢 NSE_INDEX status: {nse_index_status}")

                        if nse_index_status == "NORMAL_OPEN":
                            print(
                                "✅ NSE_INDEX market is open - expecting live data..."
                            )
                        else:
                            print(
                                f"⚠️ Market not open for NSE_INDEX: {nse_index_status}"
                            )
                        continue

                    # Handle live feed data
                    if data.get("type") == "live_feed":
                        last_live_tick_ts = time.time()
                        feeds = data.get("feeds", {})
                        print(f"📊 Live feed received with {len(feeds)} instruments")

                        for instrument_key, feed_data in feeds.items():
                            print(f"🔑 Instrument: {instrument_key}")

                            # Extract LTP from different feed structures
                            ltp = None

                            # Case 1: Direct LTPC
                            if "ltpc" in feed_data:
                                ltp = feed_data["ltpc"].get("ltp")
                                print(f"💰 LTP from ltpc: {ltp}")

                            # Case 2: Full Feed - Market FF
                            elif "fullFeed" in feed_data:
                                ff = feed_data["fullFeed"]
                                if "marketFF" in ff and "ltpc" in ff["marketFF"]:
                                    ltp = ff["marketFF"]["ltpc"].get("ltp")
                                    print(f"💰 LTP from marketFF: {ltp}")

                            # Case 3: First Level With Greeks
                            elif "firstLevelWithGreeks" in feed_data:
                                flg = feed_data["firstLevelWithGreeks"]
                                if "ltpc" in flg:
                                    ltp = flg["ltpc"].get("ltp")
                                    print(f"💰 LTP from firstLevelWithGreeks: {ltp}")

                            if ltp is not None and ltp > 0:
                                last_ws_ltp_ts = time.time()
                                latest_price = ltp
                                ticks_1m.append(ltp)
                                print(f"📈 NIFTY LTP: {ltp}")

                                current_time = datetime.now()
                                if (
                                    last_candle_time is None
                                    or (current_time - last_candle_time).seconds >= 60
                                    or len(ticks_1m) >= 10
                                ):
                                    if len(ticks_1m) > 0:
                                        candle = build_candle(ticks_1m)
                                        candle["timestamp"] = current_time.isoformat()
                                        candles_1m.append(candle)
                                        ticks_1m = []
                                        last_candle_time = current_time

                                        print(
                                            f"🕯️ 1m candle: O:{candle['open']} H:{candle['high']} L:{candle['low']} C:{candle['close']}"
                                        )

                                        if not active_trade:
                                            side = check_entry(candles_15m)
                                            if side:
                                                active_trade = create_trade(
                                                    side, candle
                                                )
                                                print(
                                                    f"📊 Trade entry: {side} @ {active_trade.entry}"
                                                )

                                        if active_trade:
                                            if manage_trade(active_trade, ltp):
                                                print(
                                                    f"✅ Trade closed: {active_trade.status} | PnL: {active_trade.pnl}"
                                                )
                                                active_trade = None

                                # Broadcast every time we get a valid LTP
                                await broadcast_state()
                            else:
                                print("⚠️ No valid LTP found in feed")

                except json.JSONDecodeError as e:
                    preview = msg.strip().replace("\n", " ")
                    if len(preview) > 200:
                        preview = preview[:200] + "..."
                    print(f"🔍 JSON decode error: {e} | raw={preview!r}")
                    continue
                except Exception as e:
                    print(f"🔍 Message processing error: {e}")
                    continue

            # Handle binary protobuf messages from Upstox
            elif isinstance(msg, bytes):
                try:
                    # Handle ping frames (some Upstox connections send them as data bytes)
                    if msg == b"ping":
                        print("🏓 Received ping - sending pong")
                        await ws.send(b"pong")
                        continue

                    # Parse the binary protobuf message
                    feed_response = pb.FeedResponse()
                    feed_response.ParseFromString(msg)

                    feeds_count = len(feed_response.feeds)
                    try:
                        type_name = pb.Type.Name(feed_response.type)
                    except Exception:
                        type_name = "unknown"

                    print(
                        f"🔍 Received protobuf: type={feed_response.type}({type_name}), feeds={feeds_count}"
                    )

                    # Handle market info (type 2)
                    if feed_response.type == 2:
                        print("📊 Market status received")
                        if hasattr(feed_response, "marketInfo"):
                            segment_status = feed_response.marketInfo.segmentStatus
                            if hasattr(segment_status, "items"):
                                for key, value in segment_status.items():
                                    print(f"🏢 Segment {key}: {value}")
                                    if (
                                        key == "NSE_INDEX" and value == 2
                                    ):  # NORMAL_OPEN = 2
                                        print(
                                            "✅ NSE_INDEX market is open - expecting live data..."
                                        )
                        continue

                    # Handle live feed data (type 0 or 1)
                    if feed_response.type in [0, 1] and feeds_count == 0:
                        print(
                            f"⚠️ Live feed tick received but feeds are empty. Check instrument keys: {get_instrument_keys()}"
                        )

                    if feed_response.type in [0, 1] and feeds_count > 0:
                        last_live_tick_ts = time.time()
                        print(f"📊 Live feed received with {feeds_count} instruments")

                        for instrument_key, feed in feed_response.feeds.items():
                            print(f"🔑 Instrument: {instrument_key}")

                            # Extract LTP from different feed structures
                            ltp = None

                            # Case 1: Direct LTPC
                            if feed.HasField("ltpc"):
                                ltp = feed.ltpc.ltp
                                print(f"💰 LTP from ltpc: {ltp}")

                            # Case 2: Full Feed - Index FF (for NIFTY)
                            elif feed.HasField("fullFeed"):
                                ff = feed.fullFeed
                                if ff.HasField("indexFF"):
                                    index_ff = ff.indexFF
                                    if index_ff.HasField("ltpc"):
                                        ltp = index_ff.ltpc.ltp
                                        print(f"💰 LTP from indexFF.ltpc: {ltp}")
                                    elif index_ff.HasField("marketOHLC"):
                                        ohlc_list = index_ff.marketOHLC.ohlc
                                        if len(ohlc_list) > 0:
                                            ltp = ohlc_list[0].close
                                            print(f"💰 LTP from indexFF.ohlc: {ltp}")
                                elif ff.HasField("marketFF") and ff.marketFF.HasField(
                                    "ltpc"
                                ):
                                    ltp = ff.marketFF.ltpc.ltp
                                    print(f"� LTP from marketFF: {ltp}")

                            # Case 3: First Level With Greeks
                            elif feed.HasField("firstLevelWithGreeks"):
                                flg = feed.firstLevelWithGreeks
                                if flg.HasField("ltpc"):
                                    ltp = flg.ltpc.ltp
                                    print(f"💰 LTP from firstLevelWithGreeks: {ltp}")

                            if ltp is not None and ltp > 0:
                                last_ws_ltp_ts = time.time()
                                latest_price = ltp
                                ticks_1m.append(ltp)

                                # 🎯 PROMINENT NIFTY PRICE DISPLAY
                                print(f"\n🎯🎯🎯 NIFTY PRICE: {ltp} 🎯🎯🎯\n")
                                print(f"📈 NIFTY LTP: {ltp}")

                                current_time = datetime.now()
                                if (
                                    last_candle_time is None
                                    or (current_time - last_candle_time).seconds >= 60
                                    or len(ticks_1m) >= 10
                                ):
                                    if len(ticks_1m) > 0:
                                        candle = build_candle(ticks_1m)
                                        candle["timestamp"] = current_time.isoformat()
                                        candles_1m.append(candle)
                                        ticks_1m = []
                                        last_candle_time = current_time

                                        print(
                                            f"🕯️ 1m candle: O:{candle['open']} H:{candle['high']} L:{candle['low']} C:{candle['close']}"
                                        )

                                        if not active_trade:
                                            side = check_entry(candles_15m)
                                            if side:
                                                active_trade = create_trade(
                                                    side, candle
                                                )
                                                print(
                                                    f"� Trade entry: {side} @ {active_trade.entry}"
                                                )

                                        if active_trade:
                                            if manage_trade(active_trade, ltp):
                                                print(
                                                    f"✅ Trade closed: {active_trade.status} | PnL: {active_trade.pnl}"
                                                )
                                                active_trade = None

                                # Broadcast every time we get a valid LTP
                                await broadcast_state()
                            else:
                                print("⚠️ No valid LTP found in feed")

                except Exception as e:
                    print(f"🔍 Protobuf decode error: {e}")
                    # Try to decode as string as fallback
                    try:
                        text = msg.decode("utf-8")
                        if text.startswith("{"):
                            data = json.loads(text)
                            print(f"🔍 Binary was actually JSON: {data}")
                    except:
                        print(f"🔍 Binary message (hex): {msg.hex()[:50]}...")

            else:
                print(f"🔍 Unknown message type: {type(msg)}")


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
        "timestamp": datetime.now().isoformat(),
    }

    print(f"📡 Broadcasting state: price={latest_price}, clients={len(clients)}")

    for ws in clients.copy():
        try:
            await ws.send_json(payload)
        except Exception as e:
            print(f"❌ Failed to send to client: {e}")
            clients.remove(ws)


# ======================================================
# FRONTEND WS
# ======================================================


@app.websocket("/ws/nifty")
async def ws_nifty(ws: WebSocket):
    await ws.accept()
    clients.append(ws)
    print(f"🔌 Client connected. Total clients: {len(clients)}")

    # Send initial state immediately
    try:
        await broadcast_state()
    except Exception as e:
        print(f"❌ Failed to send initial state: {e}")

    try:
        while True:
            await ws.receive_text()
    except Exception:
        clients.remove(ws)
        print(f"🔌 Client disconnected. Total clients: {len(clients)}")


@app.get("/nifty")
async def get_nifty_state():
    return {
        "symbol": "NIFTY",
        "price": latest_price,
        "activeTrade": serialize_trade(active_trade),
        "tradeHistory": [
            {"side": t.side, "pnl": t.pnl, "status": t.status}
            for t in trade_log[-20:]
        ],
        "timestamp": datetime.now().isoformat(),
    }



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

    async def rest_ltp_fallback():
        global latest_price, last_ws_ltp_ts

        instrument_keys = get_instrument_keys()
        key = instrument_keys[0] if instrument_keys else None
        if not key:
            return

        interval_s = float(os.getenv("REST_LTP_POLL_INTERVAL", "2"))
        ws_grace_s = float(os.getenv("WS_LTP_GRACE_SECONDS", "20"))

        while True:
            await asyncio.sleep(interval_s)

            # If we are receiving LTP via WS, don't hammer REST.
            if (
                last_ws_ltp_ts is not None
                and (time.time() - last_ws_ltp_ts) < ws_grace_s
            ):
                continue

            ltp = get_ltp_rest(key)
            if ltp is None or ltp <= 0:
                continue

            latest_price = ltp

            # Keep candle builder fed even when WS is silent
            ticks_1m.append(ltp)

            # Visible terminal output
            print(f"\n🎯🎯🎯 NIFTY PRICE (REST): {ltp} 🎯🎯🎯\n")

            if len(clients) > 0:
                try:
                    await broadcast_state()
                except Exception as e:
                    print(f"❌ REST fallback broadcast error: {e}")

    async def periodic_broadcaster():
        while True:
            await asyncio.sleep(5)  # Broadcast every 5 seconds
            if len(clients) > 0:
                try:
                    await broadcast_state()
                except Exception as e:
                    print(f"❌ Periodic broadcast error: {e}")

    asyncio.create_task(runner())
    asyncio.create_task(rest_ltp_fallback())
    asyncio.create_task(periodic_broadcaster())
    print("🚀 GreenX Upstox NIFTY demo backend started")
