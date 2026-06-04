import asyncio
from database import engine, Base
import models

async def init_models():
    print("Creating database tables...")
    async with engine.begin() as conn:
        # This will create tables if they don't exist
        await conn.run_sync(Base.metadata.create_all)
    print("Tables created successfully.")

if __name__ == "__main__":
    asyncio.run(init_models())