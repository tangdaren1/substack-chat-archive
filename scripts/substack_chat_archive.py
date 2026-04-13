#!/usr/bin/env python3
"""CLI entry point for Substack Chat archive sync."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from typing import Any

from substack_chat_core import (
    DEFAULT_MAX_COMMENT_PAGES,
    DEFAULT_TIMEOUT_SECONDS,
    SubstackArchiveError,
    load_config,
    resolve_cookie,
    resolve_room_targets,
    sync_rooms,
    validate_rooms,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="substack_chat_archive.py")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser("validate", help="Validate cookie access for one or more Substack chat rooms.")
    _add_common_room_args(validate_parser)

    sync_parser = subparsers.add_parser("sync", help="Sync one or more Substack chat rooms into a local archive.")
    _add_common_room_args(sync_parser)
    sync_parser.add_argument("--archive-dir", help="Archive root directory. Required unless provided in config.")
    sync_parser.add_argument("--max-threads-per-room", type=int, help="Optional cap on visible threads processed per room.")
    sync_parser.add_argument("--max-comment-pages", type=int, default=None, help="Backward comment pages to request per thread.")
    sync_parser.add_argument("--request-timeout-seconds", type=float, default=None, help="HTTP timeout per request.")
    return parser


def _add_common_room_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--room", action="append", default=[], help="Explicit Substack chat room URL. Repeat for multiple rooms.")
    parser.add_argument("--config", help="JSON or simple YAML config file with rooms and archive settings.")
    parser.add_argument("--cookie", help="Substack session cookie value or Cookie header.")
    parser.add_argument("--cookie-file", help="File containing the Substack cookie.")


def main(argv: list[str] | None = None, *, http_client: Any = None, env: dict[str, str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    environment = env if env is not None else os.environ

    try:
        config = load_config(args.config) if args.config else None
        room_targets = resolve_room_targets(args.room, config.rooms if config else [])
        cookie = resolve_cookie(cookie=args.cookie, cookie_file=args.cookie_file, env=environment)

        if args.command == "validate":
            results = validate_rooms(
                room_targets,
                cookie=cookie,
                request_timeout_seconds=config.request_timeout_seconds if config else DEFAULT_TIMEOUT_SECONDS,
                http_client=http_client,
            )
            print(json.dumps({"results": [asdict(result) for result in results]}, indent=2, ensure_ascii=True))
            return 0 if all(result.status == "valid" for result in results) else 1

        archive_dir = args.archive_dir or (config.archive_dir if config else None)
        if not archive_dir:
            parser.error("--archive-dir is required for sync unless provided in --config.")

        max_threads_per_room = (
            args.max_threads_per_room
            if args.max_threads_per_room is not None
            else (config.max_threads_per_room if config else None)
        )
        max_comment_pages = (
            args.max_comment_pages
            if args.max_comment_pages is not None
            else (config.max_comment_pages if config else DEFAULT_MAX_COMMENT_PAGES)
        )
        request_timeout_seconds = (
            args.request_timeout_seconds
            if args.request_timeout_seconds is not None
            else (config.request_timeout_seconds if config else DEFAULT_TIMEOUT_SECONDS)
        )
        results = sync_rooms(
            room_targets,
            archive_dir=archive_dir,
            cookie=cookie,
            max_threads_per_room=max_threads_per_room,
            max_comment_pages=max_comment_pages,
            request_timeout_seconds=request_timeout_seconds,
            http_client=http_client,
        )
        print(json.dumps({"results": [asdict(result) for result in results]}, indent=2, ensure_ascii=True))
        return 0 if all(result.status == "ok" for result in results) else 1
    except (ValueError, FileNotFoundError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except SubstackArchiveError as exc:
        print(json.dumps({"status": exc.status, "message": str(exc)}, ensure_ascii=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
