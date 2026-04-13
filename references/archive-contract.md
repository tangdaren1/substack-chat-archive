# Archive Contract

The archive root is the directory passed to `--archive-dir` or the config file.

## Layout

```text
<archive-dir>/
  manifest.json
  rooms/
    <chat_id>/
      room.json
      state.json
      threads/
        <thread_id>.json
        <thread_id>.md
      raw/
        room-posts-latest.json
        thread-<thread_id>-comments-page-0.json
        thread-<thread_id>-comments-page-1.json
        thread-<thread_id>.html
```

## `manifest.json`

- `schema_version`
- `skill`
- `generated_at`
- `rooms[]`
  - `room_id`
  - `room_url`
  - `status`
  - `path`
  - `last_sync_at`

## `room.json`

- `schema_version`
- `room_id`
- `room_url`
- `thread_count`
- `room_hash`
- `last_sync_at`
- `max_threads_per_room`
- `max_comment_pages`

## `state.json`

- `schema_version`
- `room_id`
- `room_url`
- `room_hash`
- `known_thread_ids[]`
- `threads`
  - per-thread key is `<thread_id>`
  - `thread_url`
  - `content_hash`
  - `reply_count`
  - `parsed_reply_count`
  - `partial_transcript`
  - `preview_text`
  - `last_synced_at`
- `last_sync_at`

## Thread JSON Schema

Each `threads/<thread_id>.json` file contains:

- `schema_version`
- `room_id`
- `thread_id`
- `thread_url`
- `root_author`
- `root_handle`
- `root_body`
- `root_attachments[]`
- `published_at`
- `reply_count`
- `parsed_reply_count`
- `partial_transcript`
- `replies[]`
  - `id`
  - `author`
  - `handle`
  - `timestamp`
  - `raw_time_text`
  - `body`
  - `attachments[]`
- `content_hash`
- `transcript_markdown`
- `source_notes[]`

`content_hash` is deterministic and is used to decide whether a thread needs to be rewritten on rerun.
