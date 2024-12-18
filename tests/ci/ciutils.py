import contextlib
import logging
import sys
from dataclasses import dataclass


@dataclass
class TestScript:
    path: str
    args: list[str] = None
    delay: float = 0.0
    display_on_pass: bool = False
    tee_output: bool = False
    do_debug: bool = False

    def __post_init__(self):
        if self.args is None:
            self.args = []

    def __str__(self):
        args_str = ":".join(self.args)
        return f"{self.path}:{args_str}{'+debug' if self.do_debug else ''}"

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash((self.path, tuple(self.args)))

    @classmethod
    def from_existing(cls, existing, new_path):
        return cls(
            path=new_path,
            args=existing.args,
            delay=existing.delay,
            display_on_pass=existing.display_on_pass,
            tee_output=existing.tee_output,
            do_debug=existing.do_debug,
        )


@dataclass
class Check:
    passed: bool
    message: str

    def __str__(self):
        return f"{'✅' if self.passed else '❌'} {self.message}"


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


@contextlib.contextmanager
def conditional_redirect(tee_output, f_stdout, f_stderr, log_handler, root_logger):
    if tee_output:
        stdout = sys.stdout
        stderr = sys.stderr
        sys.stdout = Tee(stdout, f_stdout)
        sys.stderr = Tee(stderr, f_stderr)
        console_handler = logging.StreamHandler(sys.stdout)
        log_handler = LoggingTee(log_handler, console_handler)
        root_logger.addHandler(log_handler)
        try:
            yield
        finally:
            sys.stdout = stdout
            sys.stderr = stderr
            root_logger.removeHandler(log_handler)
    else:
        with contextlib.redirect_stdout(f_stdout), contextlib.redirect_stderr(f_stderr):
            yield
