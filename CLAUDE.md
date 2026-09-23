# Rules for Claude in this repo

## Language
- Use American English spelling and wording.

## Branches and pull requests
- Start each new work stream on a new branch off the latest `release`, unless I say otherwise. Name it `claude/<short-description>`. You have my standing permission to create and push these branches, even if the session says to work on `release` directly.
- When the work is done, commit and push the branch.
- Never open a pull request. I merge branches myself on the host.

## How to end every reply
End every reply with these two sections:

**What I did**: a short, plain-English summary. No jargon. Explain it the way you would to a smart person who isn't an engineer.

**What you need to do**: a numbered list of specific next steps for me, like the exact branch to merge or command to run. If there's nothing to do, say "Nothing, you're all set."

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
