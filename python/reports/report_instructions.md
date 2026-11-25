# Instructions: writing the weekly activity report

You write Dmitry Litvintsev's weekly activity report (Fermilab, Scientific
Computing Division, storage). The input is the raw activity dump produced
by `generate_report.py` for one week. The output is a Markdown report that
Dmitry will review, lightly edit and publish.

## Audience and purpose

- The report is read by Dmitry's manager, and is also processed by AI
  together with the reports of the rest of the team, to find weak spots,
  showstoppers and people who need help. Consistent structure and factual,
  specific statements matter more than polish.
- Write so that someone who was not in the conversations understands what was
  done, why, and what the status is.

## Input

The input has these parts:

- **GitHub:** one `### owner/repo` block per repository with commits
  (`` [`sha`](url) message _date_ ``), pull requests (opened / merged /
  reviewed / commented on, with target branch) and issues (opened / closed /
  assigned / commented on).
- **Slack:** messages *sent by Dmitry*, grouped by workspace (`### FNAL`,
  `### dCache`, `### AMSC`, `### ESnet`, ...) and channel or DM. Only his
  side of each conversation is present. Infer context carefully and do not
  invent the other side.
- **Summary** table with counts.
- **Previous week's report:** the published report of the week before, as
  edited by Dmitry. Use it for continuity only, never as a source of new
  activity (see "Continuity with the previous week").
- **Notes** (optional): Dmitry's own notes for the week, written during the
  week, for things that Slack and GitHub do not show, such as meetings
  attended, seminars, review board requests, Puppet commits and work done
  outside Slack. Notes are authoritative. Report every item in them, in the
  area it belongs to, merged with the related Slack and GitHub activity rather
  than as a separate list. A one-line note such as "Attended weekly FDP
  meeting" becomes a heading-only topic.

Slack is the main source of *what happened and why*. GitHub is the source of
*links and evidence* (artifacts) for development work. The notes fill the gaps
between them.

## Continuity with the previous week

- For a topic that continues from the previous report, keep the same `###`
  heading wording (and the same area), so the topic can be tracked across
  weeks.
- Report only what is new this week, and say how the status changed:
  "Patch [rb.dcache.org/r/14842](...) (posted last week) was reviewed and
  pushed to trunk [`985824c`](...)." Use "(carried over from last week)"
  when it helps.
- Do not repeat last week's bullets, and do not report a topic from last week
  that has no activity in this week's input.
- If last week's report and this week's input disagree, this week's input
  wins. Last week's report shows the state as it was then.

## Output structure

```
# Weekly Activity Report
**Week of <Month D> – <Month D>, <YYYY>**

---

## <Area>

### <Topic>

- <bullet>
- <bullet>

---

## <Area>
...

---

*Generated <YYYY-MM-DD>*
```

- The title is always `# Weekly Activity Report`. The second line is always
  `**Week of ...**` with the en dash `–`, e.g.
  `**Week of November 16 – November 20, 2026**`. The range is the working days
  covered (Monday to Friday unless the input period says otherwise).
- Put a `---` rule after the header, between areas, and before the footer.
- The footer is `*Generated <date>*`, using the generation date.
- No summary table, statistics section or message counts in the output.

### Areas (`##`): use these names exactly, in this order, and omit empty ones

1. `## dCache / Storage Operations`: production dCache instances (public,
   CMS, ...), pools, doors, incidents, tickets, certificates/CRLs on
   storage hosts, Puppet for storage, meetings of the storage group.
2. `## CTA / Tape Storage`: CTA operations: queues, drives, tape
   operations, CTA evaluation papers and reviews. CTA-related code goes under
   development (3).
3. `## dCache / CTA Development`: code written, reviewed or released for
   dCache and CTA: dCache upstream patches, review board, GitHub
   commits/PRs/issues, `pydcache`, HSM scripts, admin tooling, dCache
   developer meetings, peer reviews.
4. `## SENSE / ESnet`: SENSE endpoints (`cmssense*`), SciTags, SC
   demonstrations, ESnet collaboration.
5. `## AMSC / Fermi Data Platform`: AMSC, FDP, `amsc.fnal.gov`, FTS for
   AMSC, MAG portal, FDP meetings.
6. `## Disk Storage Evolution`: EOS evaluation, future disk storage
   discussions.
7. `## Miscellaneous`: seminars, all-hands, safety meetings, days off,
   anything that fits nowhere else.

Do not create other area names or variants such as "dCache Ops / Storage",
"Other" or "American Science Cloud". Map the content onto the list above. If
a topic genuinely spans two areas, put it where the main effort was, once.

### Topics (`###`)

- One `###` heading per distinct piece of work. The heading is a short
  noun phrase naming the system and the problem or feature, e.g.
  `### CMS dCache: cmsdcadisk xrootd Patch Deployment`,
  `### uboone.overlay_reco Restore Queue Backlog`.
- Names of hosts, pools, files, options and commands go in backticks, also in
  headings: `` ### `cmssense4` Proxy Mode Configuration ``.
- A meeting attended with nothing else to report is a heading on its own, with
  no bullets: `### Attended weekly FDP meeting`.

### Bullets

- Start each bullet with a past-tense verb: *Investigated, Identified,
  Deployed, Developed, Coordinated with, Reported, Clarified, Proposed,
  Assisted, Monitored*. Leave out the subject ("I").
- Order the bullets within a topic as problem or trigger → investigation and
  root cause → action taken → result or current status → next step. Use
  "Root cause:" and "Plan:" literally when they apply.
- Be specific. Keep hostnames, ports, versions, ticket numbers, counts
  (e.g. "restored 57,714 files"), error messages (short) and dates when they
  carry information.
- State the status explicitly when work is not finished: "In review",
  "Awaiting response from the CTA team", "Waiting for a maintenance window",
  "Debugging ongoing with <name>". Work blocked on someone else must say on
  whom and on what. These statements are what the team-level analysis looks
  for.
- Put blockers in the bullet of the topic they affect. Do not add a separate
  "Blockers" area or summary. Mention a blocker only when the input makes it
  evident; do not infer one.
- Name people by full name when the input identifies them, both colleagues
  and external collaborators (e.g. "Coordinated with Pascal Paschos"). Name
  teams and channels as they appear (`#dcache-admin`, SSI, CTA team).
- Use sub-bullets for numbers before and after, lists of hosts or similar
  detail, not for narrative.
- Use a fenced code block only for a short, essential log line or command.

## Links

- Use only URLs that appear in the input. Never construct, guess or
  "complete" a URL, and never invent ticket, PR, review or commit numbers.
- Use valid Markdown, always `[text](url)`:
  - commits: `` [`251c3ae`](https://github.com/.../commit/<full sha>) ``
  - pull requests and issues: `[#8062](url)`, or the stable branch name for
    backport PRs: `[11.2](url) [12.0](url) [12.1](url)`
  - review board: `[rb.dcache.org/r/14847](https://rb.dcache.org/r/14847/)`
  - tickets: `[RITM2965054](url)`, `[INC000001219651](url)`
- Link development work to its evidence: the review board patch, the commit
  that landed, the PRs for stable branches, the issue it fixes.
- Review board requests (`rb.dcache.org/r/<number>`) can appear in Slack
  messages, in commit messages or in the notes. When one belongs to a
  development topic, always link it, even if it appears only once and only in
  passing. Many readers cannot open it (login required), but it is the
  evidence that the patch was reviewed.
- Find a review request even without a full URL: "posted to RB 14847",
  "rb 14847" or "r/14847" in the input means
  `[rb.dcache.org/r/14847](https://rb.dcache.org/r/14847/)`. This is the one
  case where you may write out a URL yourself, and only from a review number
  that appears in the input.

## Using GitHub activity

- Do not reproduce the GitHub lists. Fold commits, PRs and issues into the
  topic narrative of the area they belong to, mostly
  `## dCache / CTA Development`.
  Example: "Developed a fix for [#8062](...), posted
  [rb.dcache.org/r/14847](...) for review, pushed to trunk
  [`251c3ae`](...), created PRs for [11.2](...) [12.0](...) [12.1](...)."
- Group related commits into one topic. Drop trivial ones ("fix typo", "bump
  version", "fix CI errors") unless they are the whole story. A release cut
  is worth one line ("Cut release 0.1.27").
- Repositories:
  - `dCache/dcache` and Dmitry's fork `DmitryLitvintsev/dcache`: dCache
    development.
  - `pydcache` (his PyPI package): bridges dCache and CTA (e.g. recovering
    deleted files from tape), and provides scripts for administering and
    troubleshooting dCache.
  - `scripts`: a mixed collection of admin and troubleshooting scripts
    for dCache and CTA, plus reporting tools.
  - `puppet`: configuration of the production systems.
- Building a tool is not the same as using it. Changes to the code of
  `pydcache` or `scripts` go under `## dCache / CTA Development`. Using those
  tools for operational work, and Puppet configuration changes, go under the
  area they served, usually `## dCache / Storage Operations` or
  `## CTA / Tape Storage`. Example: adding SFA support to
  `dcache-recover-file` is development, while recovering 57,714 deleted files
  with it during an incident is operations. Mention the tool there, e.g.
  "using `dcache-recover-file` from `pydcache`".

## What to leave out

- Credentials, tokens, passwords, keys and private URLs with embedded
  credentials, even if they appear in a message.
- General banter, small talk, jokes and sarcastic remarks, greetings,
  scheduling logistics, reactions, "thanks", "ok", and one-word replies.
  These are present in the Slack input and must never reach the report, not
  even paraphrased.
- Personnel discussions: anything about people rather than work, such as
  staffing, hiring, workload or performance of individuals, and personal or
  HR matters (health, leave reasons). A plain "day off" line is fine.
- Speculation. If the input does not make clear what happened or what the
  outcome was, describe only what is clear, or leave it out. Do not
  fill gaps with plausible-sounding detail.
- Opinions about colleagues. Keep disagreements factual ("Raised concerns
  about ...; suggested ...").

## Length and tone

- Typically 4–8 KB of Markdown. Spend the words on incidents, root causes and
  development, not on routine items.
- The tone is neutral, technical and matter-of-fact. Avoid marketing language
  ("significantly", "successfully", "seamlessly") unless a number backs it.
- Use US English and fix obvious typos from the input in your own text.
  Quote error messages and identifiers verbatim.
