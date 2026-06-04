from fastapi import FastAPI, Depends, WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
import asyncio
from typing import List

from database import get_db
from models import CryptoTick

app = FastAPI(title="Crypto Analytics Pipeline API")

# --- WEBSOCKET MANAGER ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)

manager = ConnectionManager()


# --- REST ENDPOINTS (Historical Data) ---
@app.get("/api/historical/{symbol}")
async def get_historical_ticks(symbol: str, limit: int = 50, db: AsyncSession = Depends(get_db)):
    """
    Fetches the most recent 'limit' number of ticks for a given symbol.
    React will use this to draw the initial chart line.
    """
    # Query the database, order by newest first, and limit the results
    query = select(CryptoTick)\
        .where(CryptoTick.symbol == symbol.upper())\
        .order_by(CryptoTick.timestamp.desc())\
        .limit(limit)
    
    result = await db.execute(query)
    ticks = result.scalars().all()
    
    # Reverse the array so the oldest is first (standard for time-series charts)
    return ticks[::-1]


# --- WEBSOCKET ENDPOINT (Live Data) ---
@app.websocket("/ws/live/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str, db: AsyncSession = Depends(get_db)):
    """
    Maintains an open connection to the React frontend, continuously
    querying the database for the absolute latest tick and pushing it.
    """
    await manager.connect(websocket)
    try:
        last_seen_id = None
        
        while True:
            # Query the absolute newest record
            query = select(CryptoTick)\
                .where(CryptoTick.symbol == symbol.upper())\
                .order_by(CryptoTick.timestamp.desc())\
                .limit(1)
            
            result = await db.execute(query)
            latest_tick = result.scalars().first()
            
            # Only broadcast if it's a new record we haven't sent yet
            if latest_tick and latest_tick.id != last_seen_id:
                last_seen_id = latest_tick.id
                
                payload = {
                    "id": latest_tick.id,
                    "symbol": latest_tick.symbol,
                    "price": latest_tick.price,
                    "quantity": latest_tick.quantity,
                    "timestamp": latest_tick.timestamp.isoformat()
                }
                
                await manager.broadcast(payload)
            
            # Wait 1 second before checking for new Spark micro-batches
            await asyncio.sleep(1)
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)