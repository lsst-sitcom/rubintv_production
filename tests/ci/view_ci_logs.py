#!/usr/bin/env python3
"""
Convenience script to view CI test logs.

Usage:
    python view_ci_logs.py <pid>                    # View process ID from latest run
    python view_ci_logs.py <script_name>            # View script name (partial match) from latest run
    python view_ci_logs.py --list                   # List all available log files from latest run
    python view_ci_logs.py --runs                   # List all test runs
    python view_ci_logs.py --run <run_id> <pid>     # View logs from specific run by run ID
    python view_ci_logs.py --run <N> <pid>          # View logs from Nth most recent run (0=latest)
"""  # noqa: W505

import argparse
import sys
from pathlib import Path


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
    """List all log files in the directory."""
    if not logDir.exists():
        return []
    return sorted(logDir.glob("*.log"))


def findLogsByPid(logDir: Path, pid: str) -> list[Path]:
    """Find log files matching a process ID."""
    pattern = f"*_pid_{pid}.log"
    return sorted(logDir.glob(pattern))


def findLogsByName(logDir: Path, name: str) -> list[Path]:
    """Find log files matching a script name (partial match)."""
    allLogs = listLogFiles(logDir)
    return [log for log in allLogs if name.lower() in log.name.lower()]


def printLogFile(logPath: Path) -> None:
    """Print the contents of a log file."""
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
