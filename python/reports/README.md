# Useful scripts to generate and post weekly reports

## Slack

Script `slack_report.py` complies messages that you sent
to slack for a given time range. Geenerates report in md format.

```
$ python slack_report.py --help
usage: slack_report.py [-h] [--last-week | --this-week | --days N] [--output FILE]

Generate a weekly activity report from Slack messages

optional arguments:
  -h, --help            show this help message and exit
  --last-week           Last Mon–Sun (default)
  --this-week           Current week so far
  --days N              Rolling last N days
  --output FILE, -o FILE
                        Write report to FILE instead of stdout
```

Generate report:

```
$ python slack_report.py  --this-week --output report.md
Period: 2026-06-22 → 2026-06-26
Report written to report.md
```

The script uses access tokens that have to be placed in the file `access_tokens.yaml` in
directory `${HOME}/.config/scd-reporting/` The file has to be protected like so:

```
chmod 0600 ${HOME}/.config/scd-reporting/access_tokens.yaml
```

The content of the `access_tokens.yaml` file should look like:

```
slack:
  fnal :   xoxp-...
  dcache : xoxp-...
```

The `slack` key is mandatory and should be called `slack`. The keys corresponding
to slack access tokens are arbitrary strings for better organization. So one can
remember what token corresponds to what.

You get slack tokens with wide read/search permissions from slack API following these instructions:

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

- Install the App and  Retrieve the Token (download)


## Generating SCD Report

The script `slack_report.py` only sees your messages - so it does not see the whole conversation. I use
the generated report as input for Claude to generate final report for SCD.

With that in mind I try to actually write my messages in slack so that claude will have easier
time reconstructing the whole conversation to make a more meaningful summary.

## Post to scd reporting

`post_report.py` can be used to post the report in md format to scd reporting API

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

To get correct dates in SCD API this script has to be run at end of the week (or during the week) as it takes start / end of the current week as parameters to SCD reports
API.


The software is licensed under [The GNU Affero General Public License](https://www.gnu.org/licenses/agpl-3.0.ca.html)

©️ 2026 Dmitry Litvintsev
