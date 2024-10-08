"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""

        #
        # TODO (done): Process incoming weather messages. Set the temperature and status.
        #
        try:
            value = message.value()
            self.temperature = value.get("temperature", self.temperature)
            self.status = value.get("status", self.status)
        except Exception as e:
            logger.error(f"Failed to process weather message: {e}")
