import logging
import sys

from config import LoggerConfig


class LoggingMixin:
  def __init__(self, logger_config: LoggerConfig, *args, **kwargs):
    self.level = logger_config.level
    self.date_format = logger_config.date_format
    self.log_format = logger_config.log_format
    super().__init__(*args, **kwargs)

  @property
  def logger(self):
    name = f'{self.__class__.__module__}.{self.__class__.__name__}'
    logger = logging.getLogger(name)

    if not logger.handlers:  # Prevent adding duplicate handlers
      logger.setLevel(self.level)

      # Stream handler (stdout)
      handler = logging.StreamHandler(sys.stdout)
      handler.setLevel(self.level)

      # Formatter
      formatter = logging.Formatter(
        fmt=self.log_format,
        datefmt=self.date_format,
      )
      handler.setFormatter(formatter)

      logger.addHandler(handler)
      logger.propagate = False  # Avoid double logging via root

    return logger


# Test code
if __name__ == '__main__':
  # Configure root logging

  class MyService(LoggingMixin):
    def do_something(self):
      self.logger.info('Doing something...')
      self.logger.debug('Debug details here.')

  config = LoggerConfig(logging.DEBUG)
  service = MyService(logger_config=config)
  service.do_something()
