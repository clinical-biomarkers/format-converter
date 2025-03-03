import logging
from pathlib import Path
from typing import Optional
import hashlib
from functools import lru_cache
import sys
from datetime import datetime

_LOGGED_MESSAGES: set[str] = set()
MAX_LOGGED_MESSAGES = 1000


class LoggerFactory:
    """Handles creation and configuration of loggers."""

    _LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    _CONSOLE_FORMAT = "%(name)s - %(levelname)s - %(message)s"
    _instance: Optional["LoggerFactory"] = None
    _initialized: bool = False
    _debug: bool = False

    def __init__(self) -> None:
        if not LoggerFactory._instance:
            LoggerFactory._instance = self
            self.root_logger = logging.getLogger("format_converter")
            self.root_logger.setLevel(logging.INFO)

    @classmethod
    def initialize(
        cls,
        log_path: Path,
        debug: bool = False,
        console_output: bool = True,
        rotate_logs: bool = True,
    ) -> None:
        """Initialize logging configuration.

        Parameters
        ----------
        log_path : Path
            Path to log file
        debug : bool, optional
            Whether to enable debug logging, by default False
        console_output : bool, optional
            Whether to output logs to console, by default True
        rotate_logs : bool, optional
            Whether to rotate logs by date, by default True
        """
        instance = cls()
        if instance._initialized:
            return

        # Set log level
        cls._debug = debug
        level = logging.DEBUG if debug else logging.INFO
        instance.root_logger.setLevel(level)

        log_path.parent.mkdir(parents=True, exist_ok=True)

        # Rotate logs if enabled
        if rotate_logs:
            date_str = datetime.now().strftime("%Y%m%d")
            log_path = log_path.with_stem(f"{log_path.stem}_{date_str}")

        # Create file handler
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(cls._LOG_FORMAT))
        file_handler.setLevel(level)
        instance.root_logger.addHandler(file_handler)

        if console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(logging.Formatter(cls._CONSOLE_FORMAT))
            console_handler.setLevel(level)
            instance.root_logger.addHandler(console_handler)

        instance._initialized = True
        instance.root_logger.info("-" * 100)
        instance.root_logger.info(f"Logging initialized. Debug mode: {debug}")

    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """Get a logger with the specified name.

        Parameters
        ----------
        name : str
            Logger name, typically the class name

        Returns
        -------
        logging.Logger
            Configured logger instance
        """
        if not cls._instance or not cls._instance._initialized:
            raise RuntimeError(
                "LoggerFactory must be initialized before getting loggers"
            )

        logger = logging.getLogger(f"format_converter.{name}")
        logger.setLevel(logging.DEBUG if cls._debug else logging.INFO)
        return logger

    @classmethod
    def is_debug_enabled(cls) -> bool:
        """Check if debug logging is enabled.

        Returns
        -------
        bool
            True if debug logging is enabled
        """
        return cls._debug


def log_once(logger: logging.Logger, message: str, level: int = logging.INFO) -> None:
    """Log a message only once, avoiding duplicates.

    Parameters
    ----------
    logger : logging.Logger
        Logger instance to use
    message : str
        Message to log
    level : int, optional
        Logging level, by default logging.INFO
    """
    msg_hash = _get_message_hash(message)
    if msg_hash not in _LOGGED_MESSAGES:
        if len(_LOGGED_MESSAGES) >= MAX_LOGGED_MESSAGES:
            _LOGGED_MESSAGES.pop()
        _LOGGED_MESSAGES.add(msg_hash)
        logger.log(level, message)


@lru_cache(maxsize=1000)
def _get_message_hash(message: str) -> str:
    """Generate hash for message deduplication.

    Parameters
    ----------
    message : str
        Message to hash

    Returns
    -------
    str
        Message hash
    """
    return hashlib.md5(message.encode()).hexdigest()


class LoggedClass:
    """Base class that provides logging functionality."""

    def __init__(self, logger_name: Optional[str] = None) -> None:
        """Initialize logger for the class.

        Parameters
        ----------
        logger_name : Optional[str], optional
            Name for the logger. If None, uses class name
        """
        self._logger_name = logger_name or self.__class__.__name__
        self._logger: Optional[logging.Logger] = None

    @property
    def logger(self) -> logging.Logger:
        if self._logger is None:
            self._logger = LoggerFactory.get_logger(self._logger_name)
        return self._logger

    def debug(self, msg: str) -> None:
        """Log debug message.

        Parameters
        ----------
        msg : str
            Message to log
        """
        self.logger.debug(msg)

    def info(self, msg: str) -> None:
        """Log info message.

        Parameters
        ----------
        msg : str
            Message to log
        """
        self.logger.info(msg)

    def warning(self, msg: str) -> None:
        """Log warning message.

        Parameters
        ----------
        msg : str
            Message to log
        """
        self.logger.warning(msg)

    def error(self, msg: str) -> None:
        """Log error message.

        Parameters
        ----------
        msg : str
            Message to log
        """
        self.logger.error(msg)

    def exception(self, msg: str) -> None:
        """Log exception message with traceback.

        Parameters
        ----------
        msg : str
            Message to log
        """
        self.logger.exception(msg)
