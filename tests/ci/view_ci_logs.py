#!/usr/bin/env python3
"""
Convenience script to view CI test logs.

Usage:
    python view_ci_logs.py <pid>                      # View process ID from latest run
    python view_ci_logs.py <script_name>              # View script name (partial match) from latest run
    python view_ci_logs.py --list                     # List all available log files from latest run
    python view_ci_logs.py --runs                     # List all test runs
    python view_ci_logs.py --run <run_id> <pid>       # View logs from specific run by run ID
    python view_ci_logs.py --run <N> <pid>            # View logs from Nth most recent run (0=latest)
    python view_ci_logs.py --tracebacks               # Find all tracebacks in latest run
    python view_ci_logs.py --tracebacks --run <N>     # Find all tracebacks in specified run
"""  # noqa: W505

import argparse
import re
import sys
from pathlib import Path


class Traceback:
    """Represents a traceback found in a log file."""

    def __init__(self, logFile: Path, preLines: list[str], tracebackLines: list[str]) -> None:
        self.logFile = logFile
        self.preLines = preLines
        self.tracebackLines = tracebackLines

    def __str__(self) -> str:
        return f"Traceback from {self.logFile.name}"


def getBaseLogDir() -> Path:
    """
    Get the base CI logs directory path.

    Returns
    -------
    logDir : `Path`
        The base log directory containing timestamped subdirectories.
    """
    scriptDir = Path(__file__).parent.resolve()
    packageDir = scriptDir.parent.parent
    logDir = packageDir / "ci_logs"
    return logDir


def getLogDir(baseLogDir: Path, runIdentifier: str | None = None) -> Path:
    """
    Get the log directory for a specific run or the latest run.

    Parameters
    ----------
    baseLogDir : `Path`
        The base log directory.
    runIdentifier : `str`, optional
        Either a run ID (timestamp or label_timestamp) or an integer index
        (as string). If None, uses latest.

    Returns
    -------
    logDir : `Path`
        The log directory for the specified run.
    """
    if runIdentifier is None:
        # Use the 'latest' symlink
        latestLink = baseLogDir / "latest"
        if latestLink.exists() and latestLink.is_symlink():
            return baseLogDir / latestLink.readlink()

        # Fallback: find the most recent directory
        runs = listTestRuns(baseLogDir)
        if runs:
            return baseLogDir / runs[0]

        return baseLogDir

    # Check if it's an integer index
    if runIdentifier.isdigit():
        index = int(runIdentifier)
        runs = listTestRuns(baseLogDir)
        if 0 <= index < len(runs):
            return baseLogDir / runs[index]
        raise ValueError(f"Run index {index} out of range (0-{len(runs) - 1})")

    # Treat as run ID (timestamp or label_timestamp)
    runDir = baseLogDir / runIdentifier
    if not runDir.exists():
        raise ValueError(f"Run directory not found: {runDir}")

    return runDir


def listTestRuns(baseLogDir: Path) -> list[str]:
    """
    List all test run timestamps in chronological order.

    Parameters
    ----------
    baseLogDir : `Path`
        The base log directory.

    Returns
    -------
    runs : `list[str]`
        List of run timestamps, newest first.
    """
    if not baseLogDir.exists():
        return []

    runs = []
    for entry in baseLogDir.iterdir():
        # Skip the 'latest' symlink and only include directories
        if entry.is_dir() and entry.name != "latest" and not entry.is_symlink():
            runs.append(entry.name)

    # Sort in reverse chronological order (newest first)
    return sorted(runs, reverse=True)


def listLogFiles(logDir: Path) -> list[Path]:
    """
    List all log files in the directory.

    Parameters
    ----------
    logDir : `Path`
        The directory containing log files.

    Returns
    -------
    logFiles : `list[Path]`
        Sorted list of log file paths.
    """
    if not logDir.exists():
        return []
    return sorted(logDir.glob("*.log"))


def findLogsByPid(logDir: Path, pid: str) -> list[Path]:
    """
    Find log files matching a process ID.

    Parameters
    ----------
    logDir : `Path`
        The directory containing log files.
    pid : `str`
        The process ID to search for.

    Returns
    -------
    logFiles : `list[Path]`
        List of matching log file paths.
    """
    pattern = f"*_pid_{pid}.log"
    return sorted(logDir.glob(pattern))


def findLogsByName(logDir: Path, name: str) -> list[Path]:
    """
    Find log files matching a script name (partial match).

    Parameters
    ----------
    logDir : `Path`
        The directory containing log files.
    name : `str`
        The script name to search for.

    Returns
    -------
    logFiles : `list[Path]`
        List of matching log file paths.
    """
    allLogs = listLogFiles(logDir)
    return [log for log in allLogs if name.lower() in log.name.lower()]


def extractTracebacks(logFile: Path, contextLines: int = 5) -> list[Traceback]:
    """
    Extract tracebacks from a log file.

    Parameters
    ----------
    logFile : `Path`
        The log file to search.
    contextLines : `int`, optional
        Number of lines before the traceback to include.

    Returns
    -------
    tracebacks : `list[Traceback]`
        List of found tracebacks.
    """
    tracebacks = []

    with open(logFile, "r") as f:
        lines = f.readlines()

    # Pattern to match "Traceback (most recent call last):"
    tracebackPattern = re.compile(r"Traceback \(most recent call last\):")

    i = 0
    while i < len(lines):
        if tracebackPattern.search(lines[i]):
            # Found a traceback start
            # Get context lines before
            startIdx = max(0, i - contextLines)
            preLines = lines[startIdx:i]

            # Extract the traceback
            tracebackLines = [lines[i]]
            i += 1

            # Continue until we hit a line that doesn't start with spaces or "
            # File" or contains an error message
            while i < len(lines):
                line = lines[i]
                # Check if this is part of the traceback
                if (
                    line.startswith("  ")
                    or line.startswith("Traceback")
                    or line.strip().endswith("Error:")
                    or line.strip().endswith("Error")
                    or re.match(r"^[A-Z]\w+Error:", line)
                    or re.match(r"^[A-Z]\w+Exception:", line)
                ):
                    tracebackLines.append(line)
                    i += 1
                    # If we hit an error line without a colon at the end, check
                    # next line
                    if re.match(r"^[A-Z]\w+(Error|Exception):", line):
                        # This is the error message, continue for one more line
                        # if it exists
                        if i < len(lines) and lines[i].strip():
                            # Check if next line is indented or looks like
                            # continuation
                            if not lines[i].startswith("Traceback"):
                                tracebackLines.append(lines[i])
                                i += 1
                        break
                else:
                    break

            tracebacks.append(Traceback(logFile, preLines, tracebackLines))
        else:
            i += 1

    return tracebacks


def findAllTracebacks(logDir: Path) -> dict[Path, list[Traceback]]:
    """
    Find all tracebacks in all log files in a directory, grouped by file.

    Parameters
    ----------
    logDir : `Path`
        The directory containing log files.

    Returns
    -------
    tracebacksByFile : `dict[Path, list[Traceback]]`
        Dictionary mapping log files to their tracebacks.
    """
    tracebacksByFile: dict[Path, list[Traceback]] = {}
    logFiles = listLogFiles(logDir)

    for logFile in logFiles:
        # Skip meta test logs
        if logFile.name.startswith("meta"):
            continue

        tracebacks = extractTracebacks(logFile)
        if tracebacks:
            tracebacksByFile[logFile] = tracebacks

    return tracebacksByFile


def printTraceback(traceback: Traceback) -> None:
    """
    Print a traceback with context.

    Parameters
    ----------
    traceback : `Traceback`
        The traceback to print.
    """
    print(f"\n{'=' * 80}")
    print(f"Traceback from: {traceback.logFile.name}")
    print(f"{'=' * 80}\n")

    if traceback.preLines:
        print("Context (5 lines before traceback):")
        print("-" * 80)
        for line in traceback.preLines:
            print(line.rstrip())
        print("-" * 80)
        print()

    print("Traceback:")
    print("-" * 80)
    for line in traceback.tracebackLines:
        print(line.rstrip())
    print("-" * 80)


def printAllTracebacksForFile(logFile: Path, tracebacks: list[Traceback]) -> None:
    """
    Print all tracebacks from a specific log file.

    Parameters
    ----------
    logFile : `Path`
        The log file.
    tracebacks : `list[Traceback]`
        List of tracebacks from this file.
    """
    print(f"\n{'#' * 80}")
    print(f"# Log file: {logFile.name}")
    print(f"# Found {len(tracebacks)} traceback(s)")
    print(f"{'#' * 80}")

    for tb in tracebacks:
        printTraceback(tb)


def handleTracebackMode(logDir: Path) -> None:
    """
    Handle the traceback finding and display mode.

    Parameters
    ----------
    logDir : `Path`
        The directory containing log files to search.
    """
    print(f"Searching for tracebacks in {logDir.name}...\n")

    tracebacksByFile = findAllTracebacks(logDir)

    if not tracebacksByFile:
        print("No tracebacks found!")
        return

    totalTracebacks = sum(len(tbs) for tbs in tracebacksByFile.values())
    print(f"Found {totalTracebacks} traceback(s) across {len(tracebacksByFile)} log file(s):\n")

    # Create a sorted list of (logFile, tracebacks) for consistent indexing
    fileList = sorted(tracebacksByFile.items(), key=lambda x: x[0].name)

    # List all files with tracebacks
    for i, (logFile, tracebacks) in enumerate(fileList, 1):
        # Get a preview of the last error line (the actual error message)
        errorLine = ""
        if tracebacks and tracebacks[-1].tracebackLines:
            # Get the last non-empty line from the last traceback
            for line in reversed(tracebacks[-1].tracebackLines):
                stripped = line.strip()
                if stripped:
                    errorLine = stripped
                    break

        print(f"  {i}. {logFile.name} ({len(tracebacks)} traceback(s))")
        if errorLine:
            previewLength = 80
            if len(errorLine) > previewLength:
                print(f"     Last: {errorLine[:previewLength]}...")
            else:
                print(f"     Last: {errorLine}")

    print("\nOptions:")
    print("  - Enter number(s) to view (e.g., '1', '1,3,5', or '1-3')")
    print("  - Enter 'all' to view all tracebacks from all files")
    print("  - Enter 'q' to quit")

    choice = input("\nYour choice: ").strip()

    if choice.lower() == "q":
        return

    if choice.lower() == "all":
        for logFile, tracebacks in fileList:
            printAllTracebacksForFile(logFile, tracebacks)
        return

    # Parse selection
    try:
        indices = parseSelection(choice, len(fileList))
        for idx in indices:
            logFile, tracebacks = fileList[idx]
            printAllTracebacksForFile(logFile, tracebacks)
    except ValueError as e:
        print(f"Error: {e}")
        return


def parseSelection(selection: str, maxIndex: int) -> list[int]:
    """
    Parse user selection string into list of indices.

    Parameters
    ----------
    selection : `str`
        User input string (e.g., '1', '1,3,5', '1-3').
    maxIndex : `int`
        Maximum valid index (1-based).

    Returns
    -------
    indices : `list[int]`
        List of 0-based indices.
    """
    indices: list[int] = []
    parts = selection.split(",")

    for part in parts:
        part = part.strip()
        if "-" in part:
            # Range
            start, end = part.split("-")
            startNum = int(start.strip())
            endNum = int(end.strip())
            del start, end

            if startNum < 1 or endNum > maxIndex or startNum > endNum:
                raise ValueError(f"Invalid range: {part}")

            indices.extend(range(startNum - 1, endNum))
        else:
            # Single number
            num = int(part)
            if num < 1 or num > maxIndex:
                raise ValueError(f"Invalid selection: {num}")
            indices.append(num - 1)

    return sorted(set(indices))


def printLogFile(logPath: Path) -> None:
    """
    Print the contents of a log file.

    Parameters
    ----------
    logPath : `Path`
        The log file to print.
    """
    print(f"\n{'=' * 80}")
    print(f"Log file: {logPath.name}")
    print(f"{'=' * 80}\n")

    with open(logPath, "r") as f:
        print(f.read())


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="View CI test logs by process ID or script name",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "identifier",
        nargs="?",
        help="Process ID or script name to search for",
    )
    parser.add_argument(
        "--list",
        "-l",
        action="store_true",
        help="List all available log files from the selected run",
    )
    parser.add_argument(
        "--runs",
        "-r",
        action="store_true",
        help="List all test runs",
    )
    parser.add_argument(
        "--run",
        metavar="RUN_ID_OR_INDEX",
        help="Specify which run to view (run ID or index, 0=latest)",
    )
    parser.add_argument(
        "--tracebacks",
        "-t",
        action="store_true",
        help="Find and display all tracebacks in the selected run",
    )

    args = parser.parse_args()

    baseLogDir = getBaseLogDir()

    if not baseLogDir.exists():
        print(f"Error: Log directory does not exist: {baseLogDir}")
        print("Have you run the test suite yet?")
        sys.exit(1)

    # List runs mode
    if args.runs:
        runs = listTestRuns(baseLogDir)
        if not runs:
            print(f"No test runs found in {baseLogDir}")
            sys.exit(0)

        print(f"Available test runs in {baseLogDir}:\n")
        for i, run in enumerate(runs):
            marker = " (latest)" if i == 0 else ""
            print(f"  {i}. {run}{marker}")
        sys.exit(0)

    # Determine which run to use
    try:
        logDir = getLogDir(baseLogDir, args.run)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    if not logDir.exists():
        print(f"Error: Log directory does not exist: {logDir}")
        print("Have you run the test suite yet?")
        sys.exit(1)

    # Traceback mode
    if args.tracebacks:
        handleTracebackMode(logDir)
        sys.exit(0)

    # Show which run we're viewing
    if args.run is not None:
        print(f"Viewing logs from run: {logDir.name}\n")
    else:
        print(f"Viewing logs from latest run: {logDir.name}\n")

    # List mode
    if args.list:
        logFiles = listLogFiles(logDir)
        if not logFiles:
            print(f"No log files found in {logDir}")
            sys.exit(0)

        print("Available log files:\n")
        for logFile in logFiles:
            print(f"  {logFile.name}")
        sys.exit(0)

    # Search mode
    if not args.identifier:
        parser.print_help()
        sys.exit(1)

    identifier = args.identifier

    # Try searching by PID first (if identifier is numeric)
    matchingLogs = []
    if identifier.isdigit():
        matchingLogs = findLogsByPid(logDir, identifier)

    # If no PID matches, try searching by name
    if not matchingLogs:
        matchingLogs = findLogsByName(logDir, identifier)

    if not matchingLogs:
        print(f"No log files found matching '{identifier}'")
        print("\nUse --list to see all available log files")
        sys.exit(1)

    if len(matchingLogs) == 1:
        printLogFile(matchingLogs[0])
    else:
        print(f"Found {len(matchingLogs)} matching log files:\n")
        for i, logFile in enumerate(matchingLogs, 1):
            print(f"  {i}. {logFile.name}")

        print("\nEnter the number of the log file to view (or 'q' to quit): ", end="")
        try:
            choice = input().strip()
            if choice.lower() == "q":
                sys.exit(0)

            index = int(choice) - 1
            if 0 <= index < len(matchingLogs):
                printLogFile(matchingLogs[index])
            else:
                print("Invalid selection")
                sys.exit(1)
        except (ValueError, KeyboardInterrupt):
            print("\nInvalid input or interrupted")
            sys.exit(1)


if __name__ == "__main__":
    main()
