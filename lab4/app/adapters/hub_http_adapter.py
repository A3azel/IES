import logging

import requests

from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.hub_gateway import HubGateway


class HubHttpAdapter(HubGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url

    def save_data(self, processed_data: ProcessedAgentData):

        agent_data = {
            "road_state": processed_data.road_state,
            "agent_data": {
                "accelerometer": {
                    "x": processed_data.agent_data.accelerometer.x,
                    "y": processed_data.agent_data.accelerometer.y,
                    "z": processed_data.agent_data.accelerometer.z
                },
                "gps": {
                    "latitude": processed_data.agent_data.gps.latitude,
                    "longitude": processed_data.agent_data.gps.longitude
                },
                "timestamp": processed_data.agent_data.timestamp.isoformat()
            }
        }

        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        response = requests.post(f"{self.api_base_url}/processed_agent_data/", json=agent_data, headers=headers)

        if response.status_code != 200:
            logging.info(
                f"Invalid Hub response\nData: {processed_data.model_dump_json()}\nResponse: {response}"
            )
            return False
        return True

