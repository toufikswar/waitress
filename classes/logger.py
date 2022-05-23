import logging

from datetime import datetime


class Logger:

    def __init__(self):
        self.logger = logging.getLogger("fuser")

    @staticmethod
    def get_current_datetime():
        now = datetime.now()
        date_now = now.strftime("%m-%d-%Y-%H-%M-%S")
        return date_now

    def set_file_handler(self):
        f_handler = logging.FileHandler(f'./logs/logs_{self.get_current_datetime()}.log')
        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(f_format)
        self.logger.addHandler(f_handler)

    def set_level(self, level=logging.DEBUG):
        self.logger.setLevel(level)





