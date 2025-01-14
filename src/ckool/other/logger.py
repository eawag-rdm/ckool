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
import logging
import logging as _logging
from random import randint

from rich.console import Console
from rich.logging import RichHandler


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
        self.console = Console()  # Create a Rich Console instance
        self.setLevel(_logging.DEBUG)
        super().__init__(__name__)

        self._debug_handler = None
        self._verbose_handler = None
        self._log_file_handler = None

        if verbose:
            self._add_verbose_stream()

    def _add_debug_stream(self):
        self._debug_handler = RichHandler(console=self.console, rich_tracebacks=True)
        self._debug_handler.setLevel(_logging.DEBUG)
        self.addHandler(self._debug_handler)

    def _add_verbose_stream(self):
        self._verbose_handler = RichHandler(console=self.console)
        self._verbose_handler.setLevel(_logging.INFO)
        self.addHandler(self._verbose_handler)

    def debug_on(self, on=True):
        if on:
            if self._debug_handler is None:
                self._add_debug_stream()
        else:
            if self._debug_handler is not None:
                self.removeHandler(self._debug_handler)
                self._debug_handler = None

    def verbose_on(self, on=True):
        if on:
            if self._verbose_handler is None:
                self._add_verbose_stream()
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
    logger_level=logging.DEBUG,
    fmt="%(message)s",
    logger_id="".join([chr(randint(65, 120)) for i in range(10)]),
):
    logger = logging.getLogger(logger_id)
    logger.setLevel(logger_level)
    handler = RichHandler(
        console=Console(),  # None uses the default console
        level=logger_level,
        show_time=True,
        show_path=True,
        rich_tracebacks=True,
        markup=True,
        show_level=True,
        log_time_format="%Y-%m-%d %H:%M:%S",
    )
    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
