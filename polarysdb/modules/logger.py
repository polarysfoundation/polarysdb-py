"""
polarysdb.modules.logger
Configurable logger matching the Go logger module interface.
"""

import logging
import sys
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional


class Level(IntEnum):
    DEBUG = 0
    INFO = 1
    WARN = 2
    ERROR = 3


@dataclass
class Config:
    min_level: Level = Level.INFO
    to_console: bool = True
    to_file: bool = False
    file_path: Optional[str] = None


class Logger:
    """
    Structured logger mirroring the Go logger.Logger interface.
    """

    def __init__(self, cfg: Optional[Config] = None):
        if cfg is None:
            cfg = Config()
        self.cfg = cfg
        self._logger = logging.getLogger("polarysdb")
        self._logger.setLevel(self._to_logging_level(cfg.min_level))

        if not self._logger.handlers:
            if cfg.to_console:
                handler = logging.StreamHandler(sys.stdout)
                handler.setFormatter(
                    logging.Formatter("[%(asctime)s] %(levelname)s  %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S")
                )
                self._logger.addHandler(handler)

            if cfg.to_file and cfg.file_path:
                fh = logging.FileHandler(cfg.file_path)
                fh.setFormatter(
                    logging.Formatter("[%(asctime)s] %(levelname)s  %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S")
                )
                self._logger.addHandler(fh)

    @staticmethod
    def _to_logging_level(level: Level) -> int:
        return {
            Level.DEBUG: logging.DEBUG,
            Level.INFO:  logging.INFO,
            Level.WARN:  logging.WARNING,
            Level.ERROR: logging.ERROR,
        }[level]

    def debug(self, *args):
        self._logger.debug(" ".join(str(a) for a in args))

    def debugf(self, fmt: str, *args):
        self._logger.debug(fmt % args)

    def info(self, *args):
        self._logger.info(" ".join(str(a) for a in args))

    def infof(self, fmt: str, *args):
        self._logger.info(fmt % args)

    def warn(self, *args):
        self._logger.warning(" ".join(str(a) for a in args))

    def warnf(self, fmt: str, *args):
        self._logger.warning(fmt % args)

    def error(self, *args):
        self._logger.error(" ".join(str(a) for a in args))

    def errorf(self, fmt: str, *args):
        self._logger.error(fmt % args)