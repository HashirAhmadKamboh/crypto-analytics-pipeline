from fastapi import FastAPI, Depends, WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_db
import asyncio
from typing import List

app = FastAPI(title="Crypto Analytics Pipeline API")

# Simple connection manager to keep track of active UI clients
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, web_socket: WebSocket):
        await web_socket.accept()
        self.active_connections.append(web_socket)

    def disconnect(self, web_socket: WebSocket):
        self.active_connections.remove(web_socket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                # Handle stale connections gracefully
                self.disconnect(connection)

manager = ConnectionManager()

@app.get("/")
async def root():
    return {"status": "online", "service": "crypto-backend"}

@app.websocket("/ws/ticks")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive; can listen for client-side filter changes here
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)