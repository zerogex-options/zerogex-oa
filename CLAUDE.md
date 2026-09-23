# Working agreements

## How to end every response

Every reply, without exception, ends with these two sections and nothing after them:

### Summary
What was actually done. Plain sentences, no jargon, no internal shorthand (never
"S1", "G2", "F4" without spelling out what it means). Three or four lines at most.
Findings and caveats go here, briefly — not in a separate essay.

### Your next steps
Numbered. Exact commands where a command is the answer. What "done" looks like
for each. If nothing is needed, say "Nothing — waiting on X."

Do not put decisions, questions or new information after these sections.

## Tone

- Michael is a sole proprietor running this alone. Every deploy is in front of
  paying customers and there is no second engineer to catch anything.
- Lead with the answer. Reasoning only where it changes what he should do.
- Never invent urgency, and never pad a list with work that is not on the
  critical path. If something can wait, say it can wait.
- Own mistakes in one line and move on.

## This repository

- Develop on the branch the session names. Michael merges to `release` himself
  and deploys from the EC2 box.
- **The test suite has ~135 pre-existing failures on `release`.** Pass/fail is
  meaningless here. Always diff the failure SET against a baseline from a clean
  checkout, and build the filter from the test files that IMPORT what changed,
  not from module-name keywords.
- Other sessions land work on `release` in parallel. Re-merge `release` into the
  working branch before pushing, or a stale base silently reverts someone's fix.
- `black` at the pinned version reformats 123 files that are already committed.
  Never run it across whole files — format only what was edited.
- Schema changes are applied by hand with `make schema-apply` and must land
  BEFORE the services restart, or writes fail against a missing column.
