from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_api_gateway import StoreGateway

import requests
from typing import List


class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]):
        if processed_agent_data_batch:
            data_to_send = []
            for processed_agent_data in processed_agent_data_batch:
                agent_data = {
                    "road_state": processed_agent_data.road_state,
                    "agent_data": {
                        "accelerometer": {
                            "x": processed_agent_data.agent_data.accelerometer.x,
                            "y": processed_agent_data.agent_data.accelerometer.y,
                            "z": processed_agent_data.agent_data.accelerometer.z
                        },
                        "gps": {
                            "latitude": processed_agent_data.agent_data.gps.latitude,
                            "longitude": processed_agent_data.agent_data.gps.longitude
                        },
                        "timestamp": processed_agent_data.agent_data.timestamp.isoformat()
                    }
                }
                data_to_send.append(agent_data)
            headers = {
                'accept': 'application/json',
                'Content-Type': 'application/json',
            }
            response = requests.post(
                f"{self.api_base_url}/processed_agent_data/",
                json=data_to_send,
                headers=headers
            )
            if response.status_code == 200:
                print("Success")
            else:
                print("Error")
