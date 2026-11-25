#!/usr/bin/env python3
"""Weekly activity report generator — Slack"""

from __future__ import annotations

import argparse
import logging
import logging.config
import os
import sys
import re
import time
import requests
import yaml

from datetime import datetime, timedelta, timezone
from pathlib import Path


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
# prefix of the Slack DMs sent by run_weekly_report.py; such messages are not activity
BOT_TAG = "[weekly-report]"


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

def safe_api_request(url, headers, params, max_retries=5):
    """Wraps requests to handle rate limits and structural API stutter gracefully."""
    backoff = 2
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params)
            # If Slack explicitly tells us we are rate limited
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", backoff))
                print(f"  [Rate Limited] Slack requested backoff. Sleeping for {retry_after}s...")
                time.sleep(retry_after)
                continue

            data = response.json()

            # If the API layer returned an error
            if not data.get("ok"):
                error_msg = data.get("error")
                if error_msg == "ratelimited":
                    print(f"  [Rate Limited API] Sleeping for {backoff}s and retrying...")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                else:
                    print(f"  API Error: {error_msg}")
                    return None

            return data

        except requests.exceptions.RequestException as e:
            print(f"  Network error: {e}. Retrying in {backoff}s...")
            time.sleep(backoff)
            backoff *= 2

    return None


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
            return user_cache

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


def fetch_slack(token: str, start: datetime, end: datetime) -> dict:

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
        #channels_url = "https://slack.com/api/conversations.list"
        # this API seems to contain only channels the user is member of:
        channels_url = "https://slack.com/api/users.conversations"

        params = {
            "types": "public_channel,im,mpim,private_channel",
            "exclude_archived" : True,
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

    print(f"Total number of conversations found in {team}: {len(channels)}", file=sys.stderr)

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
            username = None
            if user_cache:
                username = user_cache.get(channel.get('user'), channel.get('user'))
            else:
                username = channel.get('user')
            channel_name = f"DM with User: {username}"
        elif channel.get("is_mpim"):
            channel_name = f"Group DM ({channel.get('name')})"
        else:
            channel_name = f"#{channel.get('name')}"

        print(f"\rScanning channels in {team}: {counter: >6}/{number_of_channels}",
              end="", flush=True, file=sys.stderr)

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
#            history_response = requests.get(history_url,
#                                            headers=headers,
#                                            params=history_params).json()
            history_response = safe_api_request(history_url, headers, history_params)
            if not  history_response:
                logger.error(f"Failed to query history {team} {channel_name}")
                break

            messages = history_response.get("messages", [])
            for msg in messages:
                # skip the notifications of run_weekly_report.py (DMs to yourself)
                if msg.get("user") == user_id and not msg.get("text", "").startswith(BOT_TAG):
                    readable_ts = datetime.fromtimestamp(float(msg.get("ts"))).strftime('%Y-%m-%d %H:%M:%S')
                    if channel_name not in messages_by_team[team]:
                        messages_by_team[team][channel_name] = []
                    messages_by_team[team][channel_name].append(msg["text"])
            metadata = history_response.get("response_metadata", {})
            cursor = metadata.get("next_cursor")
            if not cursor:
                break
            #time.sleep(0.2)
            page += 1
        #time.sleep(0.3)
    print("\n", file=sys.stderr)
    return  messages_by_team


GITHUB_API = "https://api.github.com"


TOKEN_EXPIRY_WARNING_DAYS = 30
TOKEN_RENEWAL_HINT = (
    "regenerate it at https://github.com/settings/personal-access-tokens "
    "(public repositories read-only, no permissions, lifetime at most 366 days: "
    "the dCache organization rejects longer-lived tokens) and update "
    f"{TOKEN_FILE}")


class GitHubRateLimited(Exception):
    """Unauthenticated GitHub API limit (60 requests/hour) exhausted."""


class GitHubTokenRejected(Exception):
    """Token expired or revoked."""


def github_get(session: requests.Session, url: str, params: dict | None = None):
    r = session.get(url, params=params, timeout=30)
    if r.status_code == 401 and "Authorization" in session.headers:
        raise GitHubTokenRejected(
            f"GitHub token rejected (expired or revoked): {TOKEN_RENEWAL_HINT}")
    if r.status_code in (403, 429) and r.headers.get("X-RateLimit-Remaining") == "0":
        reset = datetime.fromtimestamp(int(r.headers.get("X-RateLimit-Reset", "0")))
        raise GitHubRateLimited(f"GitHub API rate limit exhausted until {reset:%H:%M}")
    r.raise_for_status()
    # sent with every authenticated response, e.g. "2027-11-24 06:00:00 UTC"
    expiry = r.headers.get("github-authentication-token-expiration")
    if expiry:
        session.token_expiry = expiry
    return r.json()


def token_expiry_warning(session: requests.Session) -> str | None:
    expiry = getattr(session, "token_expiry", None)
    if not expiry:
        return None
    try:
        expires = datetime.strptime(expiry.replace(" UTC", ""), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    days = (expires - datetime.now(timezone.utc).replace(tzinfo=None)).days
    if days > TOKEN_EXPIRY_WARNING_DAYS:
        return None
    return f"GitHub token expires {expires:%Y-%m-%d} (in {days} days): {TOKEN_RENEWAL_HINT}"


# Local time is the OS timezone (or $TZ), like datetime.now() used for the
# report period. astimezone() without an argument applies the DST rules for
# each moment, unlike a fixed offset taken from "now".

def to_local(timestamp: str) -> datetime:
    """GitHub UTC timestamp -> naive local time, comparable with start/end."""
    return (datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            .astimezone().replace(tzinfo=None))


def to_utc_iso(local: datetime) -> str:
    """Naive local time -> UTC timestamp for the GitHub API."""
    return local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_github(username: str, token: str | None,
                 start: datetime, end: datetime) -> dict:
    """Commits and pull requests by username in [start, end], grouped by repository.

    The public events API no longer includes commits in PushEvent payloads (only
    the before/head SHAs) nor titles in PullRequestEvent payloads, so each push
    is resolved with the compare API and each pull request is fetched. A token
    only raises the rate limit (5000 instead of 60 requests/hour); without one
    a typical week (15-30 requests) still fits.
    """
    session = requests.Session()
    session.headers.update({"Accept": "application/vnd.github+json",
                            "X-GitHub-Api-Version": "2022-11-28"})
    if token:
        session.headers["Authorization"] = f"Bearer {token}"

    repos: dict = {}
    seen_shas: set = set()
    error = None

    def repo_entry(name):
        return repos.setdefault(name, {"commits": [], "pull_requests": [], "issues": []})

    pr_cache: dict = {}

    def pull_request(url):
        """PR details (title, link, base branch, merged); one request per PR."""
        if url not in pr_cache:
            pr_cache[url] = github_get(session, url)
        return pr_cache[url]

    def add_activity(repo, kind, action, number, title, url, when, base=None,
                     assignee=None):
        """One line per (action, number); repeated comments are counted."""
        items = repo_entry(repo)[kind]
        for item in items:
            if item["action"] == action and item["number"] == number:
                item["count"] += 1
                item["when"] = max(item["when"], when)
                item["date"] = item["when"].strftime("%Y-%m-%d")
                return
        items.append({"action": action, "number": number, "title": " ".join(title.split()),
                      "url": url, "base": base, "assignee": assignee, "count": 1,
                      "when": when, "date": when.strftime("%Y-%m-%d")})

    try:
        events = []
        # the events API returns at most 300 events (3 pages), newest first
        for page in range(1, 4):
            batch = github_get(session, f"{GITHUB_API}/users/{username}/events",
                               {"per_page": 100, "page": page})
            events.extend(batch)
            if len(batch) < 100 or to_local(batch[-1]["created_at"]) < start:
                break

        for event in events:
            created_at = to_local(event["created_at"])
            if not start <= created_at <= end:
                continue
            repo = event["repo"]["name"]
            payload = event.get("payload", {})

            if event["type"] == "PushEvent":
                if set(payload["before"]) == {"0"}:
                    # new branch: nothing to compare with
                    commits = github_get(
                        session, f"{GITHUB_API}/repos/{repo}/commits",
                        {"sha": payload["head"], "author": username,
                         "since": to_utc_iso(start), "until": to_utc_iso(end),
                         "per_page": 100})
                else:
                    commits = github_get(
                        session,
                        f"{GITHUB_API}/repos/{repo}/compare/{payload['before']}...{payload['head']}"
                    ).get("commits", [])
                for c in commits:
                    login = (c.get("author") or {}).get("login") or ""
                    # a push may carry other people's commits (merges, syncs);
                    # the same commit may be pushed to several repositories
                    if login.lower() != username.lower() or c["sha"] in seen_shas:
                        continue
                    seen_shas.add(c["sha"])
                    authored = to_local(c["commit"]["author"]["date"])
                    repo_entry(repo)["commits"].append({
                        "sha": c["sha"][:7],
                        "url": c["html_url"],
                        "message": c["commit"]["message"].strip().split("\n")[0],
                        "date": authored.strftime("%Y-%m-%d"),
                        "when": authored,
                    })

            elif event["type"] in ("PullRequestEvent", "PullRequestReviewEvent",
                                   "PullRequestReviewCommentEvent"):
                action = payload.get("action")
                if event["type"] == "PullRequestReviewEvent":
                    action = "reviewed"
                elif event["type"] == "PullRequestReviewCommentEvent":
                    action = "commented"
                elif action not in ("opened", "closed", "merged", "reopened"):
                    continue
                pr = pull_request(payload["pull_request"]["url"])
                if action == "closed" and pr.get("merged"):
                    # older payloads report a merge as "closed"
                    action = "merged"
                add_activity(repo, "pull_requests", action, pr["number"], pr["title"],
                             pr["html_url"], created_at, base=pr["base"]["ref"])

            elif event["type"] in ("IssuesEvent", "IssueCommentEvent"):
                issue = payload["issue"]
                action = ("commented" if event["type"] == "IssueCommentEvent"
                          else payload.get("action"))
                if action not in ("opened", "closed", "reopened", "assigned", "commented"):
                    continue
                # comments on pull requests arrive as issue comments
                kind = "pull_requests" if "pull_request" in issue else "issues"
                assignee = (payload.get("assignee") or {}).get("login")
                if assignee and assignee.lower() == username.lower():
                    assignee = None  # self-assigned: "Assigned issue" is enough
                add_activity(repo, kind, action, issue["number"], issue["title"],
                             issue["html_url"], created_at, assignee=assignee)
    except (GitHubRateLimited, GitHubTokenRejected, requests.RequestException) as exc:
        error = str(exc)
        print(f"[GitHub] {error}; report is incomplete", file=sys.stderr)

    warning = token_expiry_warning(session)
    if warning:
        print(f"[GitHub] {warning}", file=sys.stderr)

    # oldest first, like the Slack section
    for entry in repos.values():
        for items in entry.values():
            items.sort(key=lambda item: item["when"])
    return {"repos": dict(sorted(repos.items())), "error": error, "warning": warning}


# ── Report renderer ───────────────────────────────────────────────────────────

GITHUB_ACTIONS = {
    "opened": "Opened",
    "merged": "Merged",
    "closed": "Closed",
    "reopened": "Reopened",
    "assigned": "Assigned",
    "reviewed": "Reviewed",
    "commented": "Commented on",
}


def render_report(
    start: datetime,
    end: datetime,
    slack: dict,
    github: dict
) -> str:
    lines = []
    week_str = f"{start.strftime('%B %d')} – {end.strftime('%B %d, %Y')}"
    lines += [f"# Weekly Activity Report", f"**Period:** {week_str}\n", "---\n"]

    if github:
        lines.append("## GitHub\n")
        if github.get("error"):
            lines.append(f"_Incomplete: {github['error']}_\n")
        if github.get("warning"):
            lines.append(f"_Warning: {github['warning']}_\n")
        for repo, entry in github.get("repos", {}).items():
            lines.append(f"### {repo}\n")
            for c in entry["commits"]:
                lines.append(f"- [`{c['sha']}`]({c['url']}) {c['message']} _{c['date']}_")
            for kind, noun in (("pull_requests", "pull request"), ("issues", "issue")):
                for item in entry[kind]:
                    verb = GITHUB_ACTIONS.get(item["action"], item["action"].capitalize())
                    line = f"- {verb} {noun} [#{item['number']} {item['title']}]({item['url']})"
                    if item.get("base") and item["action"] in ("opened", "merged"):
                        line += f" into `{item['base']}`"
                    if item.get("assignee") and item["action"] == "assigned":
                        line += f" to {item['assignee']}"
                    if item["count"] > 1:
                        what = "comments" if item["action"] == "commented" else "times"
                        line += f" ({item['count']} {what})"
                    lines.append(f"{line} _{item['date']}_")
            lines.append("")
        if not github.get("repos"):
            lines.append("_No activity this period._\n")

    if slack:
        all_messages = 0
        lines.append("## Slack\n")
        for team, channels in slack.items():
            by_channel = {}
            total = 0
            lines.append(f"### {team}\n")
            for channel, messages in channels.items():
                lines.append(f"#### In {channel}:\n")
                by_channel[channel] = len(messages)
                all_messages += len(messages)
                for message in messages[::-1]: # reverse order
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
    if github:
        if not github.get("repos"):
            lines.append(f"| GitHub | _No activity this period._ |")
        else:
            repos = github["repos"].values()
            number_of_commits = sum(len(e["commits"]) for e in repos)
            number_of_prs = len({(r, i["number"]) for r, e in github["repos"].items()
                                 for i in e["pull_requests"]})
            number_of_issues = len({(r, i["number"]) for r, e in github["repos"].items()
                                    for i in e["issues"]})
            lines.append(f"| GitHub | {number_of_commits} commits, {number_of_prs} pull requests, "
                         f"{number_of_issues} issues in {len(github['repos'])} public repositories |")



    lines.append(f"\n_Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}_")
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a weekly activity report from Slack and GitHub")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--last-week", action="store_true", default=True, help="Last Mon–Sun (default)")
    grp.add_argument("--this-week", action="store_true", help="Current week so far")
    grp.add_argument("--days", type=int, metavar="N", help="Rolling last N days")
    parser.add_argument("--output", "-o", metavar="FILE", help="Write report to FILE instead of stdout")
    parser.add_argument("--skip", nargs="+", choices=["github", "slack"], default=[], metavar="SOURCE")
    args = parser.parse_args()

    if args.days:
        start, end = get_date_range("rolling", args.days)
    elif args.this_week:
        start, end = get_date_range("this_week")
    else:
        start, end = get_date_range("last_week")

    skip = set(args.skip)

    # Load tokens
    tokens = None
    try:
        tokens_path = Path(TOKEN_FILE)
        if tokens_path.stat().st_mode != 33152 :
            logger.error(
                "Tokens file %s permissions too permissive, should be 0600",
                TOKEN_FILE
            )
            sys.exit(1)

        tokens = yaml.safe_load(tokens_path.read_text())
        if not tokens:
            logger.error("Failed to load tokensuration from %s", TOKEN_FILE)
            sys.exit(1)

    except (OSError, IOError) as exc:
        if isinstance(exc, FileNotFoundError):
            logger.error("Tokens file %s does not exist", TOKEN_FILE)
        else:
            logger.error("Error reading tokens file: %s", exc)
        sys.exit(1)
    except yaml.YAMLError as exc:
        logger.error("Error parsing tokens file: %s", exc)
        sys.exit(1)

    print(f"Period: {start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')}", file=sys.stderr)

    github_data: dict = {}
    if "github" not in skip:
        # github: {<username>: <token>}; the token may be empty (anonymous access)
        githubs = tokens.get("github") or {}
        if not githubs:
            print(f"[GitHub] Skipped: no 'github' entry in {TOKEN_FILE}", file=sys.stderr)
        for username, token in githubs.items():
            try:
                data = fetch_github(username, token, start, end)
            except Exception as exc:
                print(f"[GitHub] {username}: {exc}", file=sys.stderr)
                continue
            # merge several accounts into one GitHub section
            github_data.setdefault("repos", {}).update(data["repos"])
            for key in ("error", "warning"):
                if data[key]:
                    github_data[key] = data[key]

    messages_by_team: dict = {}

    if "slack" not in skip:

        slacks = tokens.get("slack")

        if not slacks:
            logger.error("Did not find slack tokens in %s", TOKEN_FILE)
            sys.exit(1)

        for tok in slacks.values():
            print("Fetching Slack …", file=sys.stderr)
            slack_data = fetch_slack(tok, start, end)
            messages_by_team.update(slack_data)

    report = render_report(start, end, messages_by_team, github_data)

    if args.output:
        Path(args.output).write_text(report, encoding="utf-8")
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(report)


if __name__ == "__main__":
    main()
