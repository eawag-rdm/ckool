#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2021 Christian Foerster @EAWAG. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
# Core: Log information in different streams and files.
# ==============================================================================
import datetime as _dt
import logging as _logging
import logging.handlers as _handlers
import os as _os
import sys as _sys
from random import randint as _randint


class MultiLineFormatter(_logging.Formatter):
    def format(self, record):
        record_string = _logging.Formatter.format(self, record)
        if record.exc_text is None:
            header, _ = record_string.split(record.message)
        else:
            header, _ = record_string.split(record.message + "\n" + record.exc_text)
        record_string = record_string.replace("\n", "\n" + " " * len(header))
        return record_string


class DebugStreamHandler(_logging.StreamHandler):
    def __init__(self, stream):
        if stream is None:
            stream = _sys.stdout
        super().__init__(stream)

    def emit(self, record):
        if not record.levelno == _logging.DEBUG:
            return
        _logging.StreamHandler.emit(self, record)


class SingletonType(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonType, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class MainLogger(_logging.Logger, metaclass=SingletonType):
    def __init__(
        self, verbose=False, _format="[%(asctime)s - %(levelname)s] - %(message)s"
    ):
        self.__format = _format
        self.setLevel(_logging.DEBUG)
        super().__init__(__name__)

        self._debug_handler = None
        self._verbose_handler = None

        if verbose:
            self._add_verbose_stream()

    def _add_debug_stream(self, stream=None):
        self._debug_handler = DebugStreamHandler(stream)
        self._debug_handler.setLevel(_logging.DEBUG)
        formatter = MultiLineFormatter(self.__format, "%Y-%m-%d %H:%M:%S")
        self._debug_handler.setFormatter(formatter)
        self.addHandler(self._debug_handler)

    def _add_verbose_stream(self, stream=None):
        self._verbose_handler = _logging.StreamHandler(stream)
        self._verbose_handler.setLevel(_logging.INFO)
        formatter = MultiLineFormatter(self.__format, "%Y-%m-%d %H:%M:%S")
        self._verbose_handler.setFormatter(formatter)
        self.addHandler(self._verbose_handler)

    def debug_on(self, on=True):
        if on:
            if self._debug_handler is None:
                self._add_debug_stream(_sys.stdout)
        else:
            if self._debug_handler is not None:
                self.removeHandler(self._debug_handler)
                self._debug_handler = None

    def verbose_on(self, on=True):
        if on:
            if self._verbose_handler is None:
                self._add_verbose_stream(_sys.stdout)
        else:
            if self._verbose_handler is not None:
                self.removeHandler(self._verbose_handler)
                self._verbose_handler = None

    def reload(self, verbose_stream, debug_stream):
        # verbose mode
        self.verbose_on(verbose_stream)

        # debug mode
        self.debug_on(debug_stream)

    def __del__(self):
        handlers = self.handlers[:]
        for handler in handlers:
            self.removeHandler(handler)
            handler.close()


def get_logger(
    stream=_sys.stdout,
    logger_level=_logging.DEBUG,
    fmt="[%(asctime)s - %(levelname)s] - %(message)s",
    logger_id="".join([chr(_randint(65, 120)) for i in range(10)]),
):
    logger = _logging.getLogger(logger_id)
    logger.setLevel(logger_level)
    handler = _logging.StreamHandler(stream)
    handler.setLevel(logger_level)
    formatter = MultiLineFormatter(fmt, "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
