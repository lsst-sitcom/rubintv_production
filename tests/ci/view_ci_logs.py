#!/usr/bin/env python3
"""
Convenience script to view CI test logs.

Usage:
    python view_ci_logs.py <pid>          # View process ID
    python view_ci_logs.py <script_name>  # View script name (partial match)
    python view_ci_logs.py --list         # List all available log files
"""

import argparse
import sys
from pathlib import Path


def getLogDir() -> Path:
    """Get the CI logs directory path."""
    scriptDir = Path(__file__).parent.resolve()
    packageDir = scriptDir.parent.parent
    logDir = packageDir / "ci_logs"
    return logDir


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
        help="List all available log files",
    )

    args = parser.parse_args()

    logDir = getLogDir()

    if not logDir.exists():
        print(f"Error: Log directory does not exist: {logDir}")
        print("Have you run the test suite yet?")
        sys.exit(1)

    # List mode
    if args.list:
        logFiles = listLogFiles(logDir)
        if not logFiles:
            print(f"No log files found in {logDir}")
            sys.exit(0)

        print(f"Available log files in {logDir}:\n")
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
