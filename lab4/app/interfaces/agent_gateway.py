from abc import ABC, abstractmethod

class AgentGateway(ABC):
    """
     Abstract class representing the Agent Gateway interface. 7
     All agent gateway adapters must implement these methods. 8
     """
    @abstractmethod
    def on_message(self, client, userdata, msg):
     """

     Method to handle incoming messages from the agent. 14
     Parameters:

     client: MQTT client instance.

     userdata: Any additional user data passed to the MQTT client. 17
     msg: The MQTT message received from the agent. 18
     """

     pass
    @abstractmethod
    def connect(self):
     """
     Method to establish a connection to the agent.
     """

     pass

    @abstractmethod
    def start(self):

         """
        31
         Method to start listening for messages from the agent. 32
         """

         pass

    @abstractmethod
    def stop(self):
         """
         Method to stop the agent gateway and clean up resources. 39
         """
         pass