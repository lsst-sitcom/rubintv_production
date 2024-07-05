import contextlib
import logging
import os
import sys
from typing import Any
from dataclasses import dataclass, field


LOGS_PATH = './logs'


def check_folder_exists(folder_path):
    return os.path.exists(folder_path) and os.path.isdir(folder_path)


@dataclass(frozen=True)
class BaseTestScript:
    name: str
    path: str
    ttype: str
    args: list[Any] = field(default_factory=list)
    delay: float = 0.0
    display_on_pass: bool = False
    tee_output: bool = False
    do_debug: bool = False

    def __str__(self):
        args_str = ":".join(str(arg) for arg in self.args)
        return f"{self.path}/{self.name}:"\
               f"{args_str}{'+debug' if self.do_debug else ''}"

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash((self.path, tuple(self.args)))

    @classmethod
    def from_existing(cls, existing, new_path):
        return cls(
            path=existing.path,
            name=existing.name,
            args=existing.args,
            delay=existing.delay,
            display_on_pass=existing.display_on_pass,
            tee_output=existing.tee_output,
            do_debug=existing.do_debug,
            ttype=existing.ttype
        )


@dataclass(frozen=True)
class TestScript(BaseTestScript):
    name: str
    path: str = 'scripts.summit.LSSTComCamSim'
    ttype: str = 'non_meta'

    def __hash__(self):
        return hash((self.path, tuple(self.args)))


@dataclass(frozen=True)
class MetaTestScript(BaseTestScript):
    name: str
    path: str = 'meta_test_scripts'
    ttype: str = 'meta'

    def __hash__(self):
        return hash((self.path, tuple(self.args)))


class Tee:
    def __init__(self, *files):
        self.files = files

    def write(self, obj):
        for f in self.files:
            f.write(obj)
            f.flush()  # Ensure immediate output

    def flush(self):
        for f in self.files:
            f.flush()


class LoggingTee(logging.Handler):
    def __init__(self, *handlers):
        super().__init__()
        self.handlers = handlers

    def emit(self, record):
        for handler in self.handlers:
            handler.emit(record)
            handler.flush()


@contextlib.contextmanager
def conditional_redirect(tee_output, f_stdout, f_stderr, log_handler, root_logger):
    if tee_output:
        file_handler = logging.FileHandler(f'{LOGS_PATH}/logging_{os.getpid()}.log')
        file_handler.setLevel(logging.DEBUG)  # Set the logging level for the file handler
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(log_format)
        file_handler.setFormatter(formatter)
        stdout = sys.stdout
        stderr = sys.stderr
        sys.stdout = Tee(stdout, f_stdout)
        sys.stderr = Tee(stderr, f_stderr)
        console_handler = logging.StreamHandler(sys.stdout)
        log_handler = LoggingTee(log_handler, console_handler, file_handler)
        root_logger.addHandler(log_handler)
        root_logger.addHandler(file_handler)
        try:
            yield
        finally:
            sys.stdout = stdout
            sys.stderr = stderr
            root_logger.removeHandler(log_handler)
    else:
        with contextlib.redirect_stdout(f_stdout), contextlib.redirect_stderr(f_stderr):
            yield
