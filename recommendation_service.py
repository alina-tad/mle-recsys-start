# Создадим шаблон сервиса, который пока что умеет только возвращать пустой список.

import logging

from fastapi import FastAPI
from contextlib import asynccontextmanager
from recomendations_handler import rec_store

logger = logging.getLogger("uvicorn.error")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    logger.info("Starting")

    global rec_store

    rec_store.load(
        "personal",
        "final_recommendations_feat.parquet",
        columns=["user_id", "item_id", "rank"],
    )
    rec_store.load(
        "default",
        "top_recs.parquet",
        columns=["item_id", "rank"],
    )

    yield
    # этот код выполнится только один раз при остановке сервиса
    rec_store.stats()
    
    logger.info("Stopping")
    
# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)

@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs = rec_store.get(user_id, k)
    rec_store.stats()
    return {"recs": recs}