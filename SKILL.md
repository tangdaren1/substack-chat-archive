---
name: substack-chat-archive
description: Archive Substack Chat rooms that the user already has access to by using an explicit room URL list plus the user's own Substack session cookie. Use this when the user wants to pull, sync, or locally archive Substack Chat threads, validate room access, or keep a reproducible local transcript of subscribed chats without relying on a product database or browser automation.
---

# Substack Chat Archive

Use this skill when the task is to validate access to one or more Substack Chat rooms and sync the currently visible chat threads into a deterministic local archive.

## Required Inputs

- One or more explicit Substack Chat room URLs
- A Substack session cookie supplied by the user
  - precedence: `SUBSTACK_COOKIE`, then `--cookie-file`, then `--cookie`
- For `sync`, an archive output directory

## Main Workflow

1. Validate and normalize the room URLs.
2. Normalize the cookie without storing it anywhere in the archive.
3. Run the CLI:

```bash
python3 scripts/substack_chat_archive.py validate \
  --room https://substack.com/chat/1234567 \
  --cookie-file /path/to/substack-cookie.txt
```

```bash
python3 scripts/substack_chat_archive.py sync \
  --room https://substack.com/chat/1234567 \
  --archive-dir ./archive \
  --cookie-file /path/to/substack-cookie.txt
```

```bash
python3 scripts/substack_chat_archive.py sync --config ./substack-chat-config.json
```

4. Review the generated archive and use the thread JSON or markdown files as the downstream source of truth.

## Output Contract Summary

- Archive root contains:
  - `manifest.json`
  - `rooms/<chat_id>/room.json`
  - `rooms/<chat_id>/state.json`
  - `rooms/<chat_id>/threads/<thread_id>.json`
  - `rooms/<chat_id>/threads/<thread_id>.md`
  - `rooms/<chat_id>/raw/`
- Sync is incremental:
  - unchanged threads are skipped
  - changed threads are rewritten in place
  - reruns do not duplicate thread files or replies
- Partial transcripts are marked explicitly in both JSON and markdown output.

## Failure And Status Summary

`validate` returns one of:

- `valid`
- `expired`
- `invalid_access`
- `auth_required`
- `parse_failed`
- `http_failed`

Treat expired sessions and valid-but-not-entitled access as different cases.

## References

- [references/archive-contract.md](references/archive-contract.md): archive layout and normalized thread schema
- [references/fetch-strategy.md](references/fetch-strategy.md): fetch order, pagination, and fallback rules
- [references/auth-and-cookie.md](references/auth-and-cookie.md): cookie precedence, normalization, and auth status meanings
- [references/portability.md](references/portability.md): how to run the skill from Codex, Claude, or a generic agent runner
