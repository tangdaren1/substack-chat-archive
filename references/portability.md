# Portability

The skill is intentionally platform-neutral.

## Runtime

- Python 3.10+
- standard library only in v1
- no Node, Deno, browser automation, database, or product service dependency

## Codex

Run the CLI directly from the skill directory:

```bash
python3 scripts/substack_chat_archive.py validate --room <room-url> --cookie-file <path>
python3 scripts/substack_chat_archive.py sync --room <room-url> --archive-dir ./archive --cookie-file <path>
```

## Claude

Use the same CLI commands. If the runner supports local skills, point it at this directory and let the agent execute the Python script directly.

## Generic Agent Runner

A generic runner only needs to do three things:

1. provide room URLs
2. provide the cookie through env, file, or CLI flag
3. run the Python CLI and consume JSON stdout plus the archive files

No platform metadata is required. Optional metadata files can exist for one runner without changing the core skill behavior.
