import logging

from . import config


__all__ = ['log']


def has_level_handler(logger):
    """Check if there is a handler in the logging chain that will handle the
    given logger's :meth:`effective level <~logging.Logger.getEffectiveLevel>`.
    """
    level = logger.getEffectiveLevel()
    current = logger

    while current:
        if any(handler.level <= level for handler in current.handlers):
            return True

        if not current.propagate:
            break

        current = current.parent

    return False


def create_logger():

    logger = logging.getLogger(__package__)
    logger.setLevel(config.LOG_LEVEL)

    default_handler = logging.StreamHandler()
    default_handler.setFormatter(
        logging.Formatter(
            "[%(asctime)s] %(levelname)s in %(module)s: %(message)s")
    )

    if not has_level_handler(logger):
        logger.addHandler(default_handler)

    return logger


log = create_logger()
