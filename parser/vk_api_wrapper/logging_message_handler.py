import logging
from .api import API


class MessageHandler(logging.Handler):
    def __init__(self, access_token, user_id):
        super().__init__()
        self.api = API(access_token)
        self.user_id = user_id

    def emit(self, record: logging.LogRecord) -> None:
        message = self.format(record)
        self.api.messages_send(self.user_id, message)
