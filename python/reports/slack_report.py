#!/usr/bin/env python3
"""Weekly activity report generator — Slack"""

from __future__ import annotations

import argparse
import logging
import logging.config
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import time
import requests
import yaml

#from dotenv import load_dotenv
#load_dotenv()


#slacks = { 'FNAL' :  os.getenv("FNAL_SLACK_TOKEN"),
#           'DCACHE' : os.getenv("DCACHE_SLACK_TOKEN"),
#           'AMSC' : os.getenv("AMSC_SLACK_TOKEN"),
#           'ESNET' : os.getenv("ESNET_SLACK_TOKEN")
#           }
#

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

home = os.getenv("HOME")
TOKEN_FILE = os.path.join(home, ".config/scd-reporting/access_tokens.yaml")


# ── Date range ────────────────────────────────────────────────────────────────

def get_date_range(mode: str = "last_week", days: int = 7) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    if mode == "last_week":
        monday = now - timedelta(days=now.weekday() + 7)
        start = monday.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=7) - timedelta(microseconds=1)
    elif mode == "this_week":
        monday = now - timedelta(days=now.weekday())
        start = monday.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
    else:  # rolling N days
        start = (now - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
    return start, end



# ── Slack ─────────────────────────────────────────────────────────────────────

def fetch_slack(token: str, start: datetime, end: datetime) -> dict:
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.post("https://slack.com/api/auth.test", headers=headers, timeout=30)
    r.raise_for_status()
    auth = r.json()
    if not auth.get("ok"):
        raise RuntimeError(f"Slack auth failed: {auth.get('error')}")
    user_id = auth["user_id"]
    team = auth["team"]
    user = auth["user"]

    #print(f"Report for {team} team")

    # List all conversations the user is a member of

    start_str = start.strftime("%Y-%m-%d")
    end_str =  end.strftime("%Y-%m-%d")
    messages_by_team = {}
    messages_by_team[team] = {}

    cursor = None
    while True:
        params: dict = { "query": f"from:@{user} after:{start_str} before:{end_str}",
                         "sort": "timestamp",
                         "sort_dir": "asc",
                         "limit": 10000,
                         }
        if cursor:
            params["cursor"] = cursor
        r = requests.get("https://slack.com/api/search.messages", headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if data.get("ok"):
            messages = data["messages"]["matches"]
            for msg in messages:
                channel = msg.get("channel")
                if channel:
                    channel_name = channel.get("name")
                    if channel_name not in messages_by_team[team]:
                        messages_by_team[team][channel_name] = []
                    messages_by_team[team][channel_name].append(msg["text"])
        cursor = (data.get("response_metadata") or {}).get("next_cursor")
        if not cursor:
            break
    return  messages_by_team


# ── Report renderer ───────────────────────────────────────────────────────────

def render_report(
    start: datetime,
    end: datetime,
    slack: dict,
) -> str:
    lines = []
    week_str = f"{start.strftime('%B %d')} – {end.strftime('%B %d, %Y')}"
    lines += [f"# Weekly Activity Report", f"**Period:** {week_str}\n", "---\n"]

    all_messages = 0
    if slack:
        for team, channels in slack.items():
            by_channel = {}
            total = 0
            lines.append(f"## {team}\n")
            for channel, messages in channels.items():
                lines.append(f"### In {channel} channel:\n")
                by_channel[channel] = len(messages)
                all_messages += len(messages)
                for message in messages:
                    msg = re.sub("```","\n```\n",message)
                    lines.append(f" - {msg}:\n")
                    total += 1
            lines.append(f"**Messages sent:** {total}\n")
            lines.append("**By channel:**")
            for ch, count in sorted(by_channel.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"- `#{ch}`: {count} message{'s' if count != 1 else ''}")
            lines.append("")

    # Summary table
    lines += ["---\n", "## Summary\n", "| Source | Activity |", "|--------|----------|"]
    if slack:
        lines.append(f"| Slack  | {all_messages} messages sent |")

    lines.append(f"\n_Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}_")
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    #parser = argparse.ArgumentParser(description="Generate a weekly activity report from GitHub, GitLab, Slack, and Outlook.")
    parser = argparse.ArgumentParser(description="Generate a weekly activity report from Slack messages")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--last-week", action="store_true", default=True, help="Last Mon–Sun (default)")
    grp.add_argument("--this-week", action="store_true", help="Current week so far")
    grp.add_argument("--days", type=int, metavar="N", help="Rolling last N days")
    parser.add_argument("--output", "-o", metavar="FILE", help="Write report to FILE instead of stdout")
    args = parser.parse_args()

    if args.days:
        start, end = get_date_range("rolling", args.days)
    elif args.this_week:
        start, end = get_date_range("this_week")
    else:
        start, end = get_date_range("last_week")

    # Load tokens
    tokens = None
    try:
        tokens_path = Path(TOKEN_FILE)
        if tokens_path.stat().st_mode != 33152 :
            logger.error(
                "Tokens file %s permissions too permissive, should be 0600",
                TOKENS_FILE
            )
            sys.exit(1)

        tokens = yaml.safe_load(tokens_path.read_text())
        if not tokens:
            logger.error("Failed to load tokensuration from %s", TOKENS_FILE)
            sys.exit(1)

    except (OSError, IOError) as exc:
        if isinstance(exc, FileNotFoundError):
            logger.error("Tokens file %s does not exist", TOKENS_FILE)
        else:
            logger.error("Error reading tokens file: %s", exc)
        sys.exit(1)
    except yaml.YAMLError as exc:
        logger.error("Error parsing tokens file: %s", exc)
        sys.exit(1)

    print(f"Period: {start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')}", file=sys.stderr)


    slacks = tokens.get("slack")

    if not slacks:
        logger.error("Did not find slack tokens in ${TOKENS_FILE}")
        sys.exit(1)

    messages_by_team = {}
    for tok in slacks.values():
        #print("Fetching Slack …", file=sys.stderr)
        slack_data = fetch_slack(tok, start, end)
        messages_by_team.update(slack_data)

    report = render_report(start, end, messages_by_team)

    if args.output:
        Path(args.output).write_text(report, encoding="utf-8")
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(report)


if __name__ == "__main__":
    main()
