import json
from datetime import datetime
from typing import Set, List

from fastapi import FastAPI, WebSocket, HTTPException, WebSocketDisconnect
from pydantic import BaseModel, field_validator
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, insert, update, delete

from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

COLUMN_ROAD_STATE = "road_state"
COLUMN_X = "x"
COLUMN_Y = "y"
COLUMN_Z = "z"
COLUMN_LATITUDE = "latitude"
COLUMN_LONGITUDE = "longitude"
COLUMN_TIMESTAMP = "timestamp"

app = FastAPI()
# WebSocket subscriptions
subscriptions: Set[WebSocket] = set()

DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column(COLUMN_ROAD_STATE, String),
    Column(COLUMN_X, Float),
    Column(COLUMN_Y, Float),
    Column(COLUMN_Z, Float),
    Column(COLUMN_LATITUDE, Float),
    Column(COLUMN_LONGITUDE, Float),
    Column(COLUMN_TIMESTAMP, DateTime),
)


class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator('timestamp', mode='before')
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).")


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData


# Database model
class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime


def get_db():
    db = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    try:
        return db
    finally:
        db.close()


# FastAPI WebSocket endpoint
@app.websocket("/ws/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    subscriptions.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions.remove(websocket)


# Function to send data to subscribed users
async def send_data_to_subscribers(data):
    for websocket in subscriptions:
        await websocket.send_json(json.dumps(data))


# FastAPI CRUDL endpoints

@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    # Insert data to database
    # Send data to subscribers
    db = get_db()
    processed_agent_data_list = []

    for item in data:
        try:
            new_processed_agent_data = {
                COLUMN_ROAD_STATE: item.road_state,
                COLUMN_X: item.agent_data.accelerometer.x,
                COLUMN_Y: item.agent_data.accelerometer.y,
                COLUMN_Z: item.agent_data.accelerometer.z,
                COLUMN_LATITUDE: item.agent_data.gps.latitude,
                COLUMN_LONGITUDE: item.agent_data.gps.longitude,
                COLUMN_TIMESTAMP: item.agent_data.timestamp
            }

            processed_agent_data_list.append(new_processed_agent_data)

        except Exception:
            raise HTTPException(status_code=500, detail="Error")

    insert_processed_agent_data = insert(processed_agent_data).values(processed_agent_data_list)

    try:
        db.execute(insert_processed_agent_data)
        db.commit()
    except Exception:
        raise HTTPException(status_code=500, detail="Error")
    finally:
        db.close()


# Send data to subscribers
@app.get("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def read_processed_agent_data(processed_agent_data_id: int):
    # Get data by id
    db = get_db()
    try:
        select_processed_agent_data = select(processed_agent_data).where(
            processed_agent_data.c.id == processed_agent_data_id)
        select_processed_agent_data_value = db.execute(select_processed_agent_data).fetchone()
        return select_processed_agent_data_value
    except Exception:
        raise HTTPException(status_code=500, detail="Error")
    finally:
        db.close()


# Get data by id

@app.get("/processed_agent_data/", response_model=list[ProcessedAgentDataInDB])
def list_processed_agent_data():
    # Get list of data
    db = get_db()
    try:
        all_processed_agent_data = db.query(processed_agent_data).all()
        return all_processed_agent_data
    except Exception:
        raise HTTPException(status_code=500, detail="Error")
    finally:
        db.close()


# Get list of data

@app.put("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    # Update data
    db = get_db()
    try:
        update_processed_agent_data = (
            update(processed_agent_data)
            .where(processed_agent_data.c.id == processed_agent_data_id)
            .values({
                COLUMN_ROAD_STATE: data.road_state,
                COLUMN_X: data.agent_data.accelerometer.x,
                COLUMN_Y: data.agent_data.accelerometer.y,
                COLUMN_Z: data.agent_data.accelerometer.z,
                COLUMN_LATITUDE: data.agent_data.gps.latitude,
                COLUMN_LONGITUDE: data.agent_data.gps.longitude,
                COLUMN_TIMESTAMP: data.agent_data.timestamp
            }))
        db.execute(update_processed_agent_data)
        db.commit()

        select_processed_agent_data = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        data_value = db.execute(select_processed_agent_data).fetchone()
        return data_value
    except Exception:
        raise HTTPException(status_code=500, detail="Error")
    finally:
        db.close()


@app.delete("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def delete_processed_agent_data(processed_agent_data_id: int):
    # Delete by id
    db = get_db()
    try:
        select_processed_agent_data = db.execute(
            select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)).fetchone()

        if select_processed_agent_data is None:
            raise HTTPException(status_code=400, detail="Not found")

        delete_data = delete(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        db.execute(delete_data)
        db.commit()
        return select_processed_agent_data
    except Exception:
        raise HTTPException(status_code=500, detail="Error")
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
