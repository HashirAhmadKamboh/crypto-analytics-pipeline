import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import websocket

# Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "crypto-ticks"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

# Initialize Kafka Producer with reconnection retry logic
print("Connecting to Kafka Broker...")
producer = None
while not producer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except NoBrokersAvailable:
        print("Kafka broker not available yet, retrying in 2 seconds...")
        time.sleep(2)

print("Successfully connected to Kafka.")

def on_message(ws, message):
    try:
        data = json.loads(message)
        
        # Structure the payload cleanly for Spark consumption
        payload = {
            "symbol": data["s"],          # e.g., BTCUSDT
            "price": float(data["p"]),    # Trade price
            "quantity": float(data["q"]), # Trade quantity
            "timestamp": data["E"]        # Event time (epoch ms)
        }
        
        # Send payload to Kafka topic
        producer.send(KAFKA_TOPIC, value=payload)
        print(f"Sent to Kafka -> {payload['symbol']}: ${payload['price']} | Qty: {payload['quantity']}")
        
    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### Binance WebSocket Closed ###")

def on_open(ws):
    print("### Opened Connection to Binance WebSocket ###")

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # Run the stream indefinitely
    ws.run_forever()