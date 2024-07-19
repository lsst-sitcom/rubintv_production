from __future__ import annotations
import datetime
import multiprocessing
from dataclasses import dataclass, field
from enum import Enum
import logging
import os
from overrides import overrides
from typing import Any


class LogUtils:
    _BASE_LOGS_PATH = "./logs/logs_{0}"
    _LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    _LOG_PATH = ""

    @staticmethod
    def create_log_folder() -> None:
        identifier = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        LogUtils._LOG_PATH = LogUtils._BASE_LOGS_PATH.format(identifier)
        if not LogUtils.check_folder_exists(LogUtils._LOG_PATH):
            os.makedirs(LogUtils._LOG_PATH, exist_ok = True)

    @staticmethod
    def initialize_main_logger() -> logging.Logger:
        file_handler = logging.FileHandler(f'{LogUtils.get_log_folder()}/main.log')
        file_handler.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter(LogUtils._LOG_FORMAT)
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)
        root_logger = logging.getLogger('main')
        root_logger.addHandler(file_handler)
        root_logger.addHandler(stream_handler)
        return root_logger

    @staticmethod
    def get_log_folder() -> str:
        if LogUtils._LOG_PATH == "":
            LogUtils.create_log_folder()
        return LogUtils._LOG_PATH

    @staticmethod
    def check_folder_exists(folder_path: str) -> bool:
        return os.path.exists(folder_path) and os.path.isdir(folder_path)

    @staticmethod
    def initialize_script_logger(script_info: ScriptRunBaseInformation) -> logging.Logger:
        log_path = f"{script_info.log_path}/{script_info.ttype}"
        os.makedirs(log_path, exist_ok = True)
        file_handler = logging.FileHandler(f'{log_path}/{script_info.name}_{os.getpid()}.log')
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(LogUtils._LOG_FORMAT)
        file_handler.setFormatter(formatter)
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)
        LogUtils._initialize_logger(root_logger, script_info)
        return root_logger

    @staticmethod
    def get_script_log_file(script_info: ScriptTestInformation) -> str:
        return (f"{script_info.process_info.log_path}/{script_info.process_info.ttype}/"
                f"{script_info.process_info.name}_{script_info.pid}.log")

    @staticmethod
    def _initialize_logger(logger: logging.Logger, script_info: ScriptRunBaseInformation) -> None:
        logger.info(f"Starting {script_info.name} with args {script_info.args}")


class QueueWriter:
    def __init__(self, queue):
        self._queue = queue

    def write(self, message):
        if message.strip() != "":  # Avoid sending empty messages
            self._queue.put(message)

    def flush(self):
        pass  # Required for file-like object

    # Implementing additional methods to ensure compatibility
    def fileno(self):
        # Return a valid file descriptor if necessary; otherwise, raise an
        # exception or return a dummy value
        return -1

    def isatty(self):
        # Return False to indicate this is not a TTY device
        return False

    def close(self):
        # Handle closing if necessary
        pass


class ScriptResultInformation:

    def __init__(
            self,
            exit_code: int = 0,
            error_message: str = "No Error",
            traceback: str | None = None):
        self._exit_code = exit_code
        self._error_message = error_message
        self._pid = os.getpid()
        self._traceback = traceback

    @property
    def exit_code(self) -> int:
        return self._exit_code

    @property
    def error_message(self) -> str:
        return self._error_message

    @property
    def pid(self) -> int:
        return self._pid

    @property
    def traceback(self) -> str | None:
        return self._traceback

    def __str__(self):
        if self._exit_code == "0":
            return f"pid: {self._pid} Finished with no Error"
        elif not self._traceback:
            return f"pid: {self._pid}\nError: {self._error_message}\nExit code {self._exit_code}"
        else:
            return f"pid: {self._pid}\nError: {self._error_message}\nExit code {self._exit_code}"\
                   f"\n Traceback:\n{self._traceback}"


class TestState(Enum):
    PENDING = 0
    RUNNING = 1
    SUCCEED = 2
    ERROR = 3


@dataclass(frozen=True)
class ScriptRunBaseInformation:
    name: str
    path: str
    ttype: str
    log_path: str = LogUtils.get_log_folder()
    args: list[Any] = field(default_factory=list)
    delay: float = 0.0
    should_fail: bool = False
    do_debug: bool = False

    def __str__(self):
        args_str = ":".join(str(arg) for arg in self.args)
        return f"{self.path}/{self.name}:"\
               f"{args_str}{'+debug' if self.do_debug else ''}"

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash((self.path, tuple(self.args)))


@dataclass(frozen=True)
class ScriptRunInformation(ScriptRunBaseInformation):
    name: str
    path: str = 'scripts.summit.LSSTComCamSim'
    ttype: str = 'non_meta'

    def __hash__(self):
        return hash((self.path, tuple(self.args)))


@dataclass(frozen=True)
class MetaTestScriptRunInformation(ScriptRunBaseInformation):
    name: str
    path: str = 'meta_test_scripts'
    ttype: str = 'meta'
    should_fail: bool = False

    def __hash__(self):
        return hash((self.path, tuple(self.args)))


class TestInformation:

    def __init__(self, name: str, state: TestState = TestState.PENDING, is_remote: bool = False):
        self._name = name
        self._is_remote = False
        self._state = state

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> TestState:
        return self._state

    @state.setter
    def state(self, value: TestState):
        self._state = value

    @property
    def is_remote(self) -> bool:
        return self._is_remote

    def show_information(self, complete: bool = False):
        return str(self)

    def __str__(self):
        return f"Test: {self.name} State: {self.state}"


class ScriptTestInformation(TestInformation):

    def __init__(
        self,
        pid: int,
        process_info: ScriptRunBaseInformation,
        process: multiprocessing.Process,
        state: TestState = TestState.PENDING
    ):
        super().__init__(process_info.name, state, is_remote=True)
        self._pid = pid
        self._process_info = process_info
        self._process = process
        self._result: ScriptResultInformation | None = None

    @property
    def process_info(self) -> ScriptRunBaseInformation:
        return self._process_info

    @property
    def process(self) -> multiprocessing.Process:
        return self._process

    @property
    def result(self) -> ScriptResultInformation:
        assert self._result is not None
        return self._result

    @result.setter
    def result(self, value: ScriptResultInformation):
        self._result = value

    def is_final_state_as_expected(self) -> bool:
        return ((self.result.exit_code != 0 and self.process_info.should_fail is True)
                or (self.result.exit_code == 0 and self.process_info.should_fail is False))

    @property
    def pid(self) -> int:
        return self._pid

    @pid.setter
    def pid(self, value: int):
        self._pid = value

    @overrides
    def show_information(self, complete: bool = False) -> str:
        ret_str = str(self)
        if complete:
            ret_str += f"{self.result}"
        else:
            ret_str += f"PID: {self.pid}"
            ret_str += f" Exit Code: {self.result.exit_code}"
        return ret_str

    def __getattr__(self, item):
        return getattr(self.process_info, item)


class ScriptManager:

    def __init__(self):
        self._tests: list[ScriptTestInformation] = []

    def add_script(self, test: ScriptTestInformation):
        assert isinstance(test, ScriptTestInformation)
        self._tests.append(test)

    def get_processes(self, state: TestState | set[TestState]) -> list[multiprocessing.Process]:
        if not isinstance(state, set):
            state = {state}
        return [script.process for script in self._tests
                if script.state in state and script.process is not None]

    def get_pids(self, state: TestState | set[TestState]) -> list[int]:
        if not isinstance(state, set):
            state = {state}
        return [script.pid for script in self._tests if script.state in state]

    def get_number_running_scripts(self) -> int:
        return len([script for script in self._tests if script.state == TestState.RUNNING])

    def get_number_failed_scripts(self) -> int:
        return len([script for script in self._tests if script.state == TestState.ERROR])

    def get_number_pending_scripts(self) -> int:
        return len([script for script in self._tests if script.state == TestState.PENDING])

    def get_number_succeed_scripts(self) -> int:
        return len([script for script in self._tests if script.state == TestState.SUCCEED])

    def get_script_info(self, pid: int) -> ScriptTestInformation:
        return next(script for script in self._tests if script.pid == pid)

    def get_script_info_by_name(self, name: str) -> ScriptTestInformation:
        return next(script for script in self._tests if script.name == name)

    def __len__(self):
        return len(self._tests)

    def __iter__(self):
        return iter(self._tests)


class TestManager:

    def __init__(self):
        self._tests: list[TestInformation] = []

    def add_test(self, test: TestInformation):
        assert isinstance(test, TestInformation)
        self._tests.append(test)

    def get_field_by_status(self, field: str, status: TestState) -> list[Any]:
        return [getattr(script, field) for script in self._tests if script.state == status]

    def get_number_running_tests(self) -> int:
        return len([script for script in self._tests if script.state == TestState.RUNNING])

    def get_number_failed_tests(self) -> int:
        return len([script for script in self._tests if script.state == TestState.ERROR])

    def get_number_pending_tests(self) -> int:
        return len([script for script in self._tests if script.state == TestState.PENDING])

    def get_number_succeed_tests(self) -> int:
        return len([script for script in self._tests if script.state == TestState.SUCCEED])

    def __len__(self):
        return len(self._tests)

    def __iter__(self):
        return iter(self._tests)
