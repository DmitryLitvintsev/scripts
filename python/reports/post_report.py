import argparse
from datetime import datetime, timedelta
import os
import requests

home = os.getenv("HOME", "/Users/dmitrylitvintsev")


TOKEN_FILE = os.path.join(home, ".config/scd-reporting/token")
URL = "https://scd-reporting.fnal.gov/api/entries/"


def week_bounds(dt: datetime = None):
    if dt is None:
        dt = datetime.now()
    monday = dt - timedelta(days=dt.weekday())
    start = monday.replace(hour=8, minute=0, second=0, microsecond=0)
    friday = monday + timedelta(days=4)
    end = friday.replace(hour=17, minute=0, second=0, microsecond=0)
    return start, end


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Post weekly activity report to scd-reporting.\n\n"
            "The report file must be a plain-text or **Markdown** (`.md`) "
            "file containing the weekly activity update.  "
            "It will be submitted to the SCD Reporting API at:\n"
            f"  {URL}"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-r", "--report",
        metavar="FILE",
        help=(
            "Path to the weekly report file (Markdown format). "
            "Example: -r ~/reports/week42.md"
        ),
    )
    args = parser.parse_args()
    if not args.report:
        parser.print_help()
        print(
            "\n[ERROR] A report file is required.\n"
            "  Provide a Markdown-formatted report file with -r / --report.\n"
            "  Example:\n"
            "    python post_report.py -r ~/reports/week42.md\n"
        )
        raise SystemExit(1)
    return args


def main():
    args = parse_args()

    with open(TOKEN_FILE, "r") as f:
        token = f.read().strip()

    start, end = week_bounds()

    payload = {
        "title": "Weekly status report",
        "project": "other",
        "description": (
            "Project Milestone:\n"
            "Performance Goal: FY26-Q3\n"
            "Update: Completed integration tests."
        ),
        "category": "other",
        "period_kind": "week",
        "period_start": start.strftime("%Y-%m-%d"),
        "period_end": end.strftime("%Y-%m-%d"),
    }

    with open(args.report, "r") as report:
        payload["description"] = report.read()

    r = requests.post(
        URL,
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()
    data = r.json()
    print(f"Created entry #{data['id']}: {data['url']}")


if __name__ == "__main__":
    main()
