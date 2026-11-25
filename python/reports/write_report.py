#!/usr/bin/env python3
"""Write the weekly activity report from the raw activity dump, using Claude.

Workflow:
  1. generate_report.py -o ~/work/weekly_reports/raw/raw_<start>_<end>.md
  2. write_report.py            (this script; newest raw file by default)
  3. -> ~/work/weekly_reports/weekly_report_<monday>_<friday>.md
  4. the previous published report in ~/work/weekly_reports/ is passed along
     for continuity, and Dmitry's own notes for the week
     (~/work/weekly_reports/raw/notes_<monday>*.md) if present
  5. post_report.py

Report rules are in report_instructions.md (next to this script), used as
the system prompt. The model gateway (base_url, auth_token, model) is read
from the "anthropic" section of ~/.config/scd-reporting/access_tokens.yaml.

Requires Python >= 3.10 and the anthropic SDK, e.g. in
~/.local/share/scd-reporting/venv.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import anthropic
import yaml

SCRIPT_DIR = Path(__file__).resolve().parent
TOKEN_FILE = Path.home() / ".config/scd-reporting/access_tokens.yaml"
REPORTS_DIR = Path.home() / "work/weekly_reports"
INSTRUCTIONS = SCRIPT_DIR / "report_instructions.md"

DEFAULT_MODEL = "claude-opus-5-5"
MAX_TOKENS = 64000

# **Period:** November 16 – November 22, 2026   (header of generate_report.py output)
PERIOD_RE = re.compile(r"\*\*Period:\*\*\s+(\w+ \d+)\s+–\s+(\w+ \d+),\s+(\d{4})")
REPORT_DATE_RE = re.compile(r"weekly_report_(\d{4}-\d{2}-\d{2})")
URL_RE = re.compile(r"https?://[^\s)\]>\"'`]+")


def fail(message: str) -> None:
    print(f"write_report: {message}", file=sys.stderr)
    sys.exit(1)


def load_gateway() -> dict:
    try:
        if TOKEN_FILE.stat().st_mode & 0o077:
            fail(f"{TOKEN_FILE} permissions too permissive, should be 0600")
        config = (yaml.safe_load(TOKEN_FILE.read_text()) or {}).get("anthropic")
    except (OSError, yaml.YAMLError) as exc:
        fail(f"cannot read {TOKEN_FILE}: {exc}")
    if not config or not config.get("auth_token"):
        fail(f"no 'anthropic' section with auth_token in {TOKEN_FILE}")
    return config


def report_period(raw_text: str) -> tuple[date, date]:
    """Start and end date of the raw dump, from its **Period:** line."""
    match = PERIOD_RE.search(raw_text)
    if not match:
        fail("raw input has no '**Period:** <Month D> – <Month D>, <YYYY>' line")
    start_md, end_md, year = match.group(1), match.group(2), int(match.group(3))
    end = datetime.strptime(f"{end_md} {year}", "%B %d %Y").date()
    start = datetime.strptime(f"{start_md} {year}", "%B %d %Y").date()
    if start > end:  # period crossing New Year
        start = start.replace(year=year - 1)
    return start, end


def working_week_end(start: date, end: date) -> date:
    """Last working day reported: a Mon-Sun period is reported as Mon-Fri."""
    friday = start + timedelta(days=4 - start.weekday()) if start.weekday() < 5 else start
    return min(end, friday)


def previous_report(before: date) -> Path | None:
    """Newest published report starting before the given date (drafts excluded)."""
    candidates = []
    for path in REPORTS_DIR.glob("weekly_report_*.md"):
        match = REPORT_DATE_RE.match(path.name)
        if not match or ".draft" in path.name:
            continue
        try:
            started = date.fromisoformat(match.group(1))
        except ValueError:
            continue
        if started < before:
            candidates.append((started, path))
    return max(candidates)[1] if candidates else None


def newest_raw() -> Path:
    raws = sorted((p for p in (REPORTS_DIR / "raw").glob("*.md")
                   if not p.name.startswith("notes_")),
                  key=lambda p: p.stat().st_mtime)
    if not raws:
        fail(f"no raw input in {REPORTS_DIR / 'raw'}")
    return raws[-1]


def week_notes(start: date) -> list[Path]:
    """Dmitry's notes for the week: raw/notes_<monday>*.md."""
    return sorted((REPORTS_DIR / "raw").glob(f"notes_{start}*.md"))


def build_request(raw_text: str, previous: str | None, notes: str | None,
                  start: date, end: date) -> str:
    period = f"{start:%B} {start.day} – {end:%B} {end.day}, {end.year}"
    parts = [f"Write the weekly activity report for {period}. "
             f"Generation date: {date.today().isoformat()}.\n"]
    if previous is not None:
        parts.append("<previous_report>\n" + previous.strip() + "\n</previous_report>\n")
    else:
        parts.append("There is no previous report.\n")
    if notes:
        parts.append("<notes>\n" + notes.strip() + "\n</notes>\n")
    parts.append("<activity>\n" + raw_text.strip() + "\n</activity>")
    return "\n".join(parts)


def generate(config: dict, instructions: str, request: str, effort: str) -> str:
    client = anthropic.Anthropic(base_url=config.get("base_url"),
                                 auth_token=config["auth_token"])
    try:
        with client.messages.stream(
            model=config.get("model", DEFAULT_MODEL),
            max_tokens=MAX_TOKENS,
            system=instructions,
            messages=[{"role": "user", "content": request}],
            output_config={"effort": effort},
        ) as stream:
            message = stream.get_final_message()
    except anthropic.AuthenticationError:
        fail(f"gateway rejected the auth_token in {TOKEN_FILE}")
    except anthropic.NotFoundError:
        fail(f"gateway does not know model {config.get('model', DEFAULT_MODEL)!r}")
    except anthropic.RateLimitError:
        fail("gateway rate limit or budget exhausted; retry later")
    except anthropic.APIStatusError as exc:
        fail(f"gateway error {exc.status_code}: {exc.message}")
    except anthropic.APIConnectionError as exc:
        fail(f"cannot reach gateway {config.get('base_url')}: {exc}")

    usage = message.usage
    print(f"write_report: model {message.model}, tokens in {usage.input_tokens} "
          f"out {usage.output_tokens}, stop {message.stop_reason}", file=sys.stderr)
    if message.stop_reason == "refusal":
        details = getattr(message, "stop_details", None)
        fail(f"model declined to write the report ({getattr(details, 'category', None)})")
    if message.stop_reason == "max_tokens":
        fail(f"report truncated at max_tokens={MAX_TOKENS}")

    text = "".join(block.text for block in message.content if block.type == "text").strip()
    # unwrap a report returned inside a ```markdown fence
    fenced = re.fullmatch(r"```(?:markdown|md)?\n(.*)\n```", text, re.DOTALL)
    return (fenced.group(1) if fenced else text).strip() + "\n"


RB_URL_RE = re.compile(r"https?://rb\.dcache\.org/r/(\d+)/?$")


def unknown_links(report: str, *sources: str) -> list[str]:
    """URLs in the report that appear in none of the inputs (likely invented).

    A review board link may be written out from a bare review number
    ("RB 14847"), so it counts as known when that number is in the input.
    """
    known = "\n".join(sources)
    unknown = set()
    for url in (u.rstrip(".,;:") for u in URL_RE.findall(report)):
        if url in known:
            continue
        rb = RB_URL_RE.match(url)
        if rb and re.search(rf"\b{rb.group(1)}\b", known):
            continue
        unknown.add(url)
    return sorted(unknown)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--raw", type=Path, help="raw activity dump (default: newest in raw/)")
    parser.add_argument("--previous", type=Path,
                        help="previous report for continuity (default: newest before this week)")
    parser.add_argument("--notes", type=Path, action="append",
                        help="notes file(s) for the week (default: raw/notes_<monday>*.md)")
    parser.add_argument("--output", "-o", type=Path,
                        help="report file (default: weekly_report_<monday>_<friday>.md)")
    parser.add_argument("--effort", default="high",
                        choices=["low", "medium", "high", "xhigh", "max"])
    parser.add_argument("--force", action="store_true",
                        help="overwrite an existing report (it may contain your edits)")
    parser.add_argument("--dry-run", action="store_true",
                        help="show what would be sent, do not call the model")
    args = parser.parse_args()

    raw_path = args.raw or newest_raw()
    raw_text = raw_path.read_text(encoding="utf-8")
    start, end = report_period(raw_text)
    end = working_week_end(start, end)

    previous_path = args.previous or previous_report(start)
    previous_text = previous_path.read_text(encoding="utf-8") if previous_path else None

    notes_paths = args.notes if args.notes is not None else week_notes(start)
    notes_text = "\n\n".join(p.read_text(encoding="utf-8").strip() for p in notes_paths) or None

    output = args.output or REPORTS_DIR / f"weekly_report_{start}_{end}.md"
    if output.exists() and not args.force:
        fail(f"{output} exists (may contain your edits); use --force or --output")

    instructions = INSTRUCTIONS.read_text(encoding="utf-8")
    request = build_request(raw_text, previous_text, notes_text, start, end)

    print(f"write_report: period {start} – {end}\n"
          f"  raw:          {raw_path} ({len(raw_text):,} chars)\n"
          f"  previous:     {previous_path or 'none'}\n"
          f"  notes:        {', '.join(map(str, notes_paths)) or 'none'}\n"
          f"  instructions: {INSTRUCTIONS} ({len(instructions):,} chars)\n"
          f"  output:       {output}", file=sys.stderr)
    if args.dry_run:
        return

    report = generate(load_gateway(), instructions, request, args.effort)

    tmp = output.with_name(output.name + ".tmp")
    tmp.write_text(report, encoding="utf-8")
    os.replace(tmp, output)
    print(f"write_report: report written to {output}", file=sys.stderr)

    invented = unknown_links(report, raw_text, previous_text or "", notes_text or "")
    if invented:
        print("write_report: WARNING: links not found in the input, check them:",
              file=sys.stderr)
        for url in invented:
            print(f"  {url}", file=sys.stderr)


if __name__ == "__main__":
    main()
