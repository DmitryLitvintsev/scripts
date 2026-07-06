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
    now = datetime.now()
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

def fetch_user_cache(token: str):
    """Fetches all workspace users and returns a dict mapping ID to Real Name."""
    print("Building workspace user cache...", file=sys.stderr)
    user_cache = {}
    cursor = None
    headers = {"Authorization": f"Bearer {token}",
               "Content-Type": "application/json; charset=utf-8"}
    
    
    while True:
        url = "https://slack.com/api/users.list"
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
            
        res = requests.get(url, headers=headers, params=params).json()
        if not res.get("ok"):
            print(f"Warning: Could not fetch user names ({res.get('error')})")
            return {}

        for member in res.get("members", []):
            user_id = member["id"]
            # Fallback chain: Real Name -> Display Name -> Username
            real_name = member.get("real_name") or member.get("profile", {}).get("display_name") or member.get("name")
            user_cache[user_id] = real_name

        cursor = res.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
        time.sleep(0.2)

    print(f"Cached {len(user_cache)} users.", file=sys.stderr)
    return user_cache
        

def fetch_slack_new(token: str, start: datetime, end: datetime) -> dict:

    user_cache = fetch_user_cache(token)
    
    headers = {"Authorization": f"Bearer {token}",
               "Content-Type": "application/json; charset=utf-8"}

    r = requests.post("https://slack.com/api/auth.test", headers=headers, timeout=30)
    r.raise_for_status()
    auth = r.json()
    if not auth.get("ok"):
        raise RuntimeError(f"Slack auth failed: {auth.get('error')}")
    user_id = auth["user_id"]
    team = auth["team"]
    user = auth["user"]

    start_str = str(time.mktime(start.timetuple()))
    end_str = str(time.mktime(end.timetuple()))
    
    messages_by_team = {}
    messages_by_team[team] = {}


    channels_url = "https://slack.com/api/conversations.list"
    channels = []
    cursor = None
    print("Fetching full channel and DM list (this may take a moment)...", file=sys.stderr)
    
    while True:
        channels_url = "https://slack.com/api/conversations.list"
        params = {
            "types": "public_channel,im,mpim,private_channel", 
            "limit": 200
        }
        if cursor:
            params["cursor"] = cursor
            
        channels_response = requests.get(channels_url, headers=headers, params=params).json()
        
        if not channels_response.get("ok"):
            logger.error(f"Failed to fetch channels: {channels_response.get('error')}")
            return

        channels.extend(channels_response.get("channels", []))
        
        # Check if there are more pages of channels/DMs to pull
        cursor = channels_response.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
        time.sleep(0.2) # Avoid hitting rate limits during discovery

    print(f"Total conversation spaces found: {len(channels)}", file=sys.stderr)

    counter = 0
    number_of_channels = len(channels)

    for channel in channels:
        counter += 1 
        if channel.get("is_archived"):
            continue
            
        channel_id = channel["id"]
        #channel_name = channel["name"]

        # Format a friendly display name depending on what type of conversation it is
        if channel.get("is_im"):
        # It's a 1:1 DM. The target person's ID is stored in the 'user' field of the channel object
            #channel_name = f"DM with User: {channel.get('user')}"
            channel_name = f"DM with User: {user_cache.get(channel.get('user'))}"
        elif channel.get("is_mpim"):
            channel_name = f"Group DM ({channel.get('name')})"
        else:
            channel_name = f"#{channel.get('name')}"
        
        print(f"\rScanning channels in {team}: {counter: >6}/{number_of_channels}",  end="", flush=True, file=sys.stderr)

        cursor = None
        page = 1

        while True:
            history_url = "https://slack.com/api/conversations.history"
            history_params = {
                "channel": channel_id, 
                "limit": 100,
                "oldest": start_str,  # Start point
                "latest": end_str   # End point
            }
            if cursor:
                  history_params["cursor"] = cursor
            history_response = requests.get(history_url,
                                            headers=headers,
                                            params=history_params).json()
            messages = history_response.get("messages", [])
            for msg in messages:
                if msg.get("user") == user_id:
                    readable_ts = datetime.fromtimestamp(float(msg.get("ts"))).strftime('%Y-%m-%d %H:%M:%S')
                    if channel_name not in messages_by_team[team]:
                        messages_by_team[team][channel_name] = []
                    messages_by_team[team][channel_name].append(msg["text"])
            metadata = history_response.get("response_metadata", {})
            cursor = metadata.get("next_cursor")
            if not cursor:
                break
                
            page += 1

    return  messages_by_team


def fetch_slack(token: str, start: datetime, end: datetime, summary) -> dict:
    headers = {"Authorization": f"Bearer {token}",
               "Content-Type": "application/json; charset=utf-8"}

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
                         "limit": 10,
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
                lines.append(f"### In {channel}:\n")
                by_channel[channel] = len(messages)
                all_messages += len(messages)
                for message in messages:
                    msg = re.sub("```","\n```\n",message)
                    lines.append(f" - {msg}:\n")
                    total += 1 
            lines.append(f"**Messages sent:** {total}\n")
            lines.append("**By channel:**")
            for ch, count in sorted(by_channel.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"- `{ch}`: {count} message{'s' if count != 1 else ''}")
            lines.append("")

    # Summary table
    lines += ["---\n", "## Summary\n", "| Source | Activity |", "|--------|----------|"]
    if slack:
        lines.append(f"| Slack  | {all_messages} messages sent |")

    lines.append(f"\n_Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}_")
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a weekly activity report from GitHub, GitLab, Slack, and Outlook.")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--last-week", action="store_true", default=True, help="Last Mon–Sun (default)")
    grp.add_argument("--this-week", action="store_true", help="Current week so far")
    grp.add_argument("--days", type=int, metavar="N", help="Rolling last N days")
    parser.add_argument("--output", "-o", metavar="FILE", help="Write report to FILE instead of stdout")
    parser.add_argument("--auth-outlook", action="store_true", help="Run MSAL device-code flow for Outlook token")
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
        slack_data = fetch_slack_new(tok, start, end)
        messages_by_team.update(slack_data)

    report = render_report(start, end, messages_by_team)

    if args.output:
        Path(args.output).write_text(report, encoding="utf-8")
        print(f"\nReport written to {args.output}", file=sys.stderr)
    else:
        print(report)


if __name__ == "__main__":
    main()
