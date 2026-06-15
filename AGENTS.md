# Project Agent Instructions


## Camilo OS Handoff Protocol

This project must report progress back to Camilo OS.

At the end of every medium or major task, write a project handoff.

Create or update:

docs/status/latest-handoff.md
docs/status/handoff-log.md
local-data/project-status.json

The handoff must include:

Project name
Phase
Task name
Date
Changed files
What works
What remains placeholder
What is broken or risky
Current project status
Next recommended build
Suggested dashboard update
Confidence score
Portfolio readiness
Money potential
Maintenance burden
Blocked status
Blocker reason if any

Before writing the handoff, run the relevant build or test command.

For JavaScript, Vite, React, or TypeScript projects, run:

npm run build

Do not claim a feature works unless it was actually checked.

If build was not run, write:

Build not verified.

If browser checks were not performed, write:

Browser smoke check not verified.

The handoff should be both human readable and machine readable.

