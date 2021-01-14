"""Contains functionality related to Weather"""
import logging
import json

logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
      
        # Process incoming weather messages. Set the temperature and status.
        message_value = json.loads(message.value())
        try:
            self.temperature = message_value['temperature']
            self.status = message_value['status']
        except:
            logger.debug(f"An exception has occured during the modification of a weather message topic {message.topic()}")
