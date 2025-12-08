# Создадим шаблон сервиса, который пока что умеет только возвращать пустой список.

import logging

from fastapi import FastAPI
from contextlib import asynccontextmanager
from recomendations_handler import rec_store
import requests

logger = logging.getLogger("uvicorn.error")
features_store_url = "http://127.0.0.1:8010"
events_store_url = "http://127.0.0.1:8020" 

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

@app.post("/recommendations_offline")
async def recommendations_offline(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs = rec_store.get(user_id, k)
    rec_store.stats()
    return {"recs": recs}

def dedup_ids(ids):
    """
    Дедублицирует список идентификаторов, оставляя только первое вхождение
    """
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]
    return ids


@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
    """

    headers = {"Content-type": "application/json", "Accept": "text/plain"}

    # получаем список последних событий пользователя, возьмём три последних
    params = {"user_id": user_id, "k": 3}
    # ваш код здесь:
    resp = requests.post(events_store_url + "/get", headers=headers, params=params)
    events = resp.json()["events"]

    # получаем список айтемов, похожих на последние три, с которыми взаимодействовал пользователь
    items = []
    scores = []

    for item_id in events:
        # для каждого item_id получаем список похожих в item_similar_items
        # ваш код здесь:
        params = {"item_id": item_id, "k": k}
        resp_sim = requests.post(
            features_store_url + "/similar_items",
            headers=headers,
            params=params,
        )
        item_similar_items = resp_sim.json()

        items += item_similar_items["item_id_2"]
        scores += item_similar_items["score"]

    # сортируем похожие объекты по scores в убывающем порядке
    combined = list(zip(items, scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]

    # удаляем дубликаты, чтобы не выдавать одинаковые рекомендации
    recs = dedup_ids(combined)

    return {"recs": recs}


@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    # офлайн и онлайн отдельно
    recs_offline_resp = await recommendations_offline(user_id, k)
    recs_online_resp = await recommendations_online(user_id, k)

    recs_offline = recs_offline_resp["recs"]
    recs_online = recs_online_resp["recs"]

    recs_blended: list[int] = []

    min_length = min(len(recs_offline), len(recs_online))

    # чередуем элементы из списков:
    # 1-й, 3-й, 5-й... — онлайн; 2-й, 4-й, 6-й... — офлайн
    for i in range(min_length):
        recs_blended.append(recs_online[i])   # нечётная позиция
        recs_blended.append(recs_offline[i])  # чётная позиция

    # добавляем оставшиеся элементы в конец (если один список длиннее другого)
    if len(recs_online) > min_length:
        recs_blended.extend(recs_online[min_length:])
    if len(recs_offline) > min_length:
        recs_blended.extend(recs_offline[min_length:])

    # удаляем дубликаты, сохраняя первый порядок
    recs_blended = dedup_ids(recs_blended)

    # оставляем только первые k рекомендаций
    recs_blended = recs_blended[:k]

    return {"recs": recs_blended}