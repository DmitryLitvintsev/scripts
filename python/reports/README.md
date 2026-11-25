# Useful scripts to generate and post weekly reports

The weekly SCD activity report is produced in five steps. Each step reads
and writes plain files, so any step can be checked or rerun on its own.

| Step | Script | Input | Output |
|------|--------|-------|--------|
| 1. Collect activity | `generate_report.py` | Slack, GitHub | `~/work/weekly_reports/raw/raw_<start>_<end>.md` |
| 2. Write the report | `write_report.py` | raw dump, your notes, previous report, `report_instructions.md` | `~/work/weekly_reports/weekly_report_<monday>_<friday>.md` |
| 3. Review | you | the report | the edited report |
| 4. (continuity) | `write_report.py` | the published reports in `~/work/weekly_reports/` | used as context the next week |
| 5. Publish | `post_report.py` | the report | an entry in SCD reporting |

The report is sent by the end of business on Friday, for the current week.
Example for the week of November 16, run on Friday, November 20:

```
$ cd ~/git/scripts/python/reports
$ python3 generate_report.py --this-week -o ~/work/weekly_reports/raw/raw_2026-11-16_2026-11-20.md
$ ~/.local/share/scd-reporting/venv/bin/python write_report.py
$ emacs ~/work/weekly_reports/weekly_report_2026-11-16_2026-11-20.md
$ python3 post_report.py -r ~/work/weekly_reports/weekly_report_2026-11-16_2026-11-20.md
```

To redo a past week, use `--last-week` (or `--days N`) in step 1 and pass the
raw file with `--raw` in step 2.

## Running from cron (`run_weekly_report.py`)

`run_weekly_report.py` runs the whole pipeline unattended: it collects this
week's activity, writes the report, posts it, and sends you a Slack DM with
the link to the new entry, so you can fix it up on the website. If a step
fails, the DM says which one, with the last lines of its output.

```
$ crontab -l
MAILTO=""
0 16 * * 5 $HOME/.local/share/scd-reporting/venv/bin/python $HOME/git/scripts/python/reports/run_weekly_report.py
```

It runs every Friday at 16:00, local time. `MAILTO=""` is there because the
machine has no mail program: the Slack DM and the log are the only channels.

- **Log:** `~/work/weekly_reports/logs/weekly_report_<date>.log`, with the
  output of every step.
- **One post per week:** after posting, the entry URL is saved in
  `weekly_report_<monday>_<friday>.md.posted`. A rerun in the same week only
  repeats the link. Delete that file to post again.
- **Existing report kept:** if the week's report already exists (written by
  hand, or by a run that failed while posting), it is posted as it is, not
  regenerated.
- **Overlapping runs:** a lock file prevents two runs at the same time.
- **Test without posting:** `run_weekly_report.py --test` writes
  `weekly_report_<monday>_<friday>.draft.md` and DMs you, but does not post.
- **Test the notification only:**
  `run_weekly_report.py --notify-only "hello"`.

The DM is sent with the Slack token named `fnal` in `access_tokens.yaml`. To
use another one, add `notify: {slack: <name>}`. That Slack app needs the
**`chat:write`** user scope in addition to the read scopes below, then
*Reinstall to Workspace*. The message goes to your own DM, which only you
can see. Each of these messages starts with `[weekly-report]`, and
`generate_report.py` skips messages with that tag, so the notifications do
not show up as activity in a later report. Other notes you send yourself in
that DM still count as activity.

## Setup

### Python

`generate_report.py` and `post_report.py` run with the system `python3`
(3.9 or later) and need `requests` and `pyyaml`.

`write_report.py` needs Python 3.10 or later and the Anthropic SDK. Install
them in a virtual environment outside the repository:

```
$ python3.13 -m venv ~/.local/share/scd-reporting/venv
$ ~/.local/share/scd-reporting/venv/bin/pip install anthropic requests pyyaml
```

### Credentials

All credentials live in `${HOME}/.config/scd-reporting/`, outside the
repository. The files must be readable only by you; the scripts refuse to run
otherwise:

```
chmod 0600 ${HOME}/.config/scd-reporting/access_tokens.yaml ${HOME}/.config/scd-reporting/token
```

`access_tokens.yaml` holds the Slack, GitHub and model gateway credentials
(the `access_tokens.yaml` in this repository is a template with placeholders
only):

```
slack:
  fnal :   xoxp-...
  dcache : xoxp-...

github:
  <github-username> : github_pat_...

anthropic:
  base_url: https://litellm.fnal.gov
  auth_token: sk-...
  model: azure/claude-opus-5-5
```

- `slack` (mandatory for the Slack part): the keys are arbitrary names, so
  you can remember which token is which.
- `github`: maps your GitHub user name to a token. The token may be empty,
  which means anonymous access (see "GitHub" below).
- `anthropic`: the model gateway used by `write_report.py`. Any gateway that
  speaks the Anthropic Messages API works; use the model name the gateway
  expects.

`token` holds the bearer token for the SCD reporting API, used by
`post_report.py`.

#### Slack tokens

You get Slack tokens with wide read/search permissions from the Slack API
following these instructions:

- Navigate to the [Slack API Apps Page](https://api.slack.com/apps).
- Click the Create New App button.
- Select *From scratch*.
- Enter an App Name and select the Slack Workspace where you want to use it.
- Click Create App
- Configure Scopes (Permissions):
  - On the left sidebar menu, look under Features and select _OAuth & Permissions_
  - Scroll down to the Scopes section.
  - User Token Scopes
  - add all read permissions:
    * `channels:history`
    * `channels:read`
    * `groups:history`
    * `groups:read`
    * `im:history`
    * `im:read`
    * `mpim:history`
    * `mpim:read`
    * `search:read`
    * `users:read`
  - for the token that sends the cron job's DM (`fnal` by default), also
    `chat:write`


- Install the App and Retrieve the Token (download)

#### GitHub token

Public GitHub data needs no token, but anonymous access is limited to 60
API requests per hour per IP address, which a few reruns can exhaust. A token
raises the limit to 5000. Create a fine-grained token at
[github.com/settings/personal-access-tokens](https://github.com/settings/personal-access-tokens):

- Repository access: **Public repositories (read-only)**, no permissions.
- Expiration: **at most 366 days**. The dCache organization rejects
  fine-grained tokens with a longer lifetime, and GitHub then silently leaves
  out all `dCache/dcache` activity.

`generate_report.py` warns in the report 30 days before the token expires,
and explains how to renew it when the token is rejected.

## Step 1: collect activity (`generate_report.py`)

```
$ python3 generate_report.py --help
usage: generate_report.py [-h] [--last-week | --this-week | --days N]
                          [--output FILE] [--skip SOURCE [SOURCE ...]]

Generate a weekly activity report from Slack and GitHub

  --last-week           Last Mon–Sun (default)
  --this-week           Current week so far
  --days N              Rolling last N days
  --output FILE, -o FILE
                        Write report to FILE instead of stdout
  --skip SOURCE [SOURCE ...]
```

`--skip github` or `--skip slack` leaves out that source.

The output is a raw Markdown dump with:

- **GitHub:** per repository, your commits (link, one-line message, date),
  then pull requests (opened, merged, reviewed, commented on, with the target
  branch) and issues (opened, closed, assigned, commented on).
- **Slack:** the messages you sent, per workspace and channel or DM. The
  script sees only your messages, not the whole conversation. Writing Slack
  messages so they make sense on their own helps the summary a lot.
- **Summary:** counts per source.

Notes on GitHub data: the events API covers at most the last 90 days. A
typical week costs 15–30 API requests. If the rate limit runs out, the report
is still written, marked "_Incomplete_".

Times are in the computer's local timezone (or `$TZ`).

`slack_report.py` is the older, Slack-only version of this script.

## Your notes for the week

Some work is invisible to Slack and GitHub: meetings, seminars, review board
requests, Puppet commits, work done in person. Write it down during the week
in `~/work/weekly_reports/raw/notes_<monday>.md`, e.g.
`notes_2026-11-23.md`, one line or bullet per item:

```
- Attended weekly FDP meeting
- Attended code safety seminar led by Andrew Norman
- Posted bulk deadlock fix for review: RB 14847
- Puppet: improved SFA HSM script, production@e6f3d72
```

`write_report.py` picks up every `notes_<monday>*.md` of the report week.
Notes are treated as authoritative: every item ends up in the report, in the
area it belongs to.

Review board requests are linked in the report whenever they appear in a
Slack message, a commit message or the notes, also as a bare number ("RB
14847", "r/14847"). Mentioning them is worth it, even though only logged-in
users can open them.

## Step 2: write the report (`write_report.py`)

```
$ ~/.local/share/scd-reporting/venv/bin/python write_report.py --help
usage: write_report.py [-h] [--raw RAW] [--previous PREVIOUS] [--notes NOTES]
                       [--output OUTPUT]
                       [--effort {low,medium,high,xhigh,max}] [--force]
                       [--dry-run]

  --raw RAW             raw activity dump (default: newest in raw/)
  --previous PREVIOUS   previous report for continuity (default: newest before
                        this week)
  --notes NOTES         notes file(s) for the week (default:
                        raw/notes_<monday>*.md)
  --output, -o OUTPUT   report file (default:
                        weekly_report_<monday>_<friday>.md)
  --effort {low,medium,high,xhigh,max}
  --force               overwrite an existing report (it may contain your
                        edits)
  --dry-run             show what would be sent, do not call the model
```

With no options, it takes the newest raw dump in `~/work/weekly_reports/raw/`,
the notes of that week, and the newest published report of an earlier week,
and writes `~/work/weekly_reports/weekly_report_<monday>_<friday>.md`. A
generation takes 1–2 minutes and about 10K output tokens.

- **The rules** for the report (structure, area names, style, links, what to
  leave out) are in [`report_instructions.md`](report_instructions.md), used
  as the system prompt. Edit that file to change how reports are written.
- **Continuity:** the previous week's report, as you edited and published it,
  is passed along so ongoing topics keep their headings and report only
  what changed. Files with `.draft` in the name are never used for this.
- **Your edits are safe:** an existing report is never overwritten without
  `--force`. To try something without touching a report, write elsewhere,
  e.g. `--output ~/work/weekly_reports/weekly_report_2026-11-16_2026-11-20.draft.md`.
- **Link check:** after writing, the script lists every link in the report
  that does not occur in the input (raw dump, notes, previous report). The
  instructions forbid invented links, so such a link needs checking.
- `--dry-run` shows which files would be used without calling the model.

## Step 5: post to SCD reporting (`post_report.py`)

`post_report.py` posts the report in md format to the SCD reporting API:

```
$ python post_report.py
usage: post_report.py [-h] [-r FILE]

Post weekly activity report to scd-reporting.

The report file must be a plain-text or **Markdown** (`.md`) file containing the weekly activity update.  It will be submitted to the SCD Reporting API at:
  https://scd-reporting.fnal.gov/api/entries/

options:
  -h, --help            show this help message and exit
  -r FILE, --report FILE
                        Path to the weekly report file (Markdown format). Example: -r ~/reports/week42.md

[ERROR] A report file is required.
  Provide a Markdown-formatted report file with -r / --report.
  Example:
    python post_report.py -r ~/reports/week42.md

```

The report period sent to the SCD API is always the *current* week
(Monday to Friday), so post the report in the week it covers, which the
Friday workflow above does.


The software is licensed under [The GNU Affero General Public License](https://www.gnu.org/licenses/agpl-3.0.ca.html)

©️ 2026 Dmitry Litvintsev
