# Architecture workflow:

Binance WebSocket $\rightarrow$ Kafka (Ingestion) $\rightarrow$ Spark Streaming (Processing) $\rightarrow$ PostgreSQL (Storage) $\rightarrow$ FastAPI (Serving) $\rightarrow$ React.js (Dashboard)
