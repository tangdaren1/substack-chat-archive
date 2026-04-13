#!/usr/bin/env python3
"""Core fetch, normalization, and archive logic for Substack Chat."""

from __future__ import annotations

import json
import os
import re
import ssl
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from substack_chat_parsers import (
    SubstackThreadDetail,
    build_substack_room_hash,
    build_substack_thread_comments_api_url,
    build_substack_thread_transcript,
    build_substack_room_posts_api_url,
    build_thread_content_hash,
    evaluate_substack_room_auth,
    extract_substack_chat_id,
    get_oldest_substack_reply_cursor,
    merge_substack_replies,
    normalize_substack_chat_room_url,
    normalize_substack_cookie,
    parse_substack_chat_thread_detail,
    parse_substack_room_posts_api_payload,
    parse_substack_thread_comments_api_payload,
    serialize_dataclass,
    sort_substack_replies_ascending,
    strip_html_to_plain,
    to_substack_cursor_timestamp,
)


SCHEMA_VERSION = 1
DEFAULT_MAX_COMMENT_PAGES = 8
DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


@dataclass
class ArchiveConfig:
    rooms: list[str]
    archive_dir: str | None = None
    max_threads_per_room: int | None = None
    max_comment_pages: int = DEFAULT_MAX_COMMENT_PAGES
    request_timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS


@dataclass
class RoomTarget:
    room_id: str
    room_url: str


@dataclass
class HttpResponse:
    url: str
    final_url: str
    status_code: int
    headers: dict[str, str]
    text: str


@dataclass
class ValidationOutcome:
    room_url: str
    room_id: str | None
    status: str
    message: str
    checked_at: str
    discovered_threads: int = 0


@dataclass
class SyncRoomResult:
    room_url: str
    room_id: str | None
    status: str
    message: str
    checked_at: str
    discovered_threads: int
    fetched_threads: int
    created_threads: int
    updated_threads: int
    skipped_threads: int
    partial_threads: int


class HttpClient(Protocol):
    def get(self, url: str, headers: dict[str, str], timeout_seconds: float) -> HttpResponse:
        ...


class SubstackArchiveError(RuntimeError):
    def __init__(
        self,
        status: str,
        message: str,
        *,
        url: str | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.url = url
        self.http_status = http_status


class UrllibHttpClient:
    def get(self, url: str, headers: dict[str, str], timeout_seconds: float) -> HttpResponse:
        request = urllib.request.Request(url, headers=headers, method="GET")
        context = ssl.create_default_context()
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds, context=context) as response:
                body = response.read()
                return HttpResponse(
                    url=url,
                    final_url=response.geturl(),
                    status_code=getattr(response, "status", response.getcode()),
                    headers={key.lower(): value for key, value in response.headers.items()},
                    text=body.decode(_get_charset(response.headers), errors="replace"),
                )
        except urllib.error.HTTPError as exc:
            body = exc.read()
            return HttpResponse(
                url=url,
                final_url=exc.geturl(),
                status_code=exc.code,
                headers={key.lower(): value for key, value in exc.headers.items()},
                text=body.decode(_get_charset(exc.headers), errors="replace"),
            )
        except urllib.error.URLError as exc:
            raise SubstackArchiveError("http_failed", f"HTTP request failed: {exc.reason}", url=url) from exc


def _get_charset(headers: Any) -> str:
    charset = headers.get_content_charset() if hasattr(headers, "get_content_charset") else None
    return charset or "utf-8"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_substack_headers(cookie: str, referer_url: str) -> dict[str, str]:
    return {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": referer_url,
        "Origin": "https://substack.com",
        "Cookie": cookie,
    }


def build_substack_api_headers(cookie: str, referer_url: str) -> dict[str, str]:
    return {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": referer_url,
        "Origin": "https://substack.com",
        "X-Requested-With": "XMLHttpRequest",
        "Cookie": cookie,
    }


def parse_json_response(text: str) -> Any:
    trimmed = text.strip()
    if not trimmed:
        return None
    try:
        return json.loads(trimmed)
    except json.JSONDecodeError:
        return None


def load_config(path: str | Path) -> ArchiveConfig:
    config_path = Path(path)
    payload = _load_structured_config(config_path)
    if not isinstance(payload, dict):
        raise ValueError("Config must be a JSON or YAML object.")

    rooms = payload.get("rooms")
    if not isinstance(rooms, list) or not all(isinstance(item, str) for item in rooms):
        raise ValueError("Config field 'rooms' must be a list of room URLs.")

    archive_dir = payload.get("archive_dir")
    if archive_dir is not None and not isinstance(archive_dir, str):
        raise ValueError("Config field 'archive_dir' must be a string when provided.")
    if archive_dir:
        archive_dir = str((config_path.parent / archive_dir).resolve()) if not Path(archive_dir).is_absolute() else archive_dir

    return ArchiveConfig(
        rooms=rooms,
        archive_dir=archive_dir,
        max_threads_per_room=_coerce_optional_int(payload.get("max_threads_per_room"), "max_threads_per_room"),
        max_comment_pages=_coerce_int(payload.get("max_comment_pages", DEFAULT_MAX_COMMENT_PAGES), "max_comment_pages"),
        request_timeout_seconds=_coerce_float(
            payload.get("request_timeout_seconds", DEFAULT_TIMEOUT_SECONDS),
            "request_timeout_seconds",
        ),
    )


def resolve_room_targets(*sources: list[str]) -> list[RoomTarget]:
    targets: list[RoomTarget] = []
    seen: set[str] = set()
    invalid: list[str] = []

    for source in sources:
        for raw_room in source:
            normalized = normalize_substack_chat_room_url(raw_room)
            if not normalized:
                invalid.append(str(raw_room))
                continue
            if normalized in seen:
                continue
            room_id = extract_substack_chat_id(normalized)
            if not room_id:
                invalid.append(str(raw_room))
                continue
            targets.append(RoomTarget(room_id=room_id, room_url=normalized))
            seen.add(normalized)

    if invalid:
        invalid_list = ", ".join(invalid)
        raise ValueError(f"Invalid Substack room URL(s): {invalid_list}")

    if not targets:
        raise ValueError("At least one valid Substack room URL is required.")

    return targets


def resolve_cookie(
    *,
    cookie: str | None = None,
    cookie_file: str | None = None,
    env: dict[str, str] | None = None,
) -> str:
    environment = env if env is not None else os.environ
    raw_value = environment.get("SUBSTACK_COOKIE")
    if raw_value is None and cookie_file:
        raw_value = Path(cookie_file).read_text(encoding="utf-8")
    if raw_value is None and cookie:
        raw_value = cookie
    return normalize_substack_cookie(raw_value)


def validate_rooms(
    room_targets: list[RoomTarget],
    *,
    cookie: str,
    request_timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    http_client: HttpClient | None = None,
) -> list[ValidationOutcome]:
    client = http_client or UrllibHttpClient()
    checked_at = utc_now_iso()
    outcomes: list[ValidationOutcome] = []

    if not cookie:
        for target in room_targets:
            outcomes.append(
                ValidationOutcome(
                    room_url=target.room_url,
                    room_id=target.room_id,
                    status="auth_required",
                    message="A Substack session cookie is required.",
                    checked_at=checked_at,
                )
            )
        return outcomes

    for target in room_targets:
        outcomes.append(
            validate_room(
                target,
                cookie=cookie,
                request_timeout_seconds=request_timeout_seconds,
                http_client=client,
                checked_at=checked_at,
            )
        )
    return outcomes


def validate_room(
    target: RoomTarget,
    *,
    cookie: str,
    request_timeout_seconds: float,
    http_client: HttpClient,
    checked_at: str | None = None,
) -> ValidationOutcome:
    checked_at = checked_at or utc_now_iso()
    try:
        room_fetch = fetch_room_posts(
            target.room_url,
            cookie=cookie,
            request_timeout_seconds=request_timeout_seconds,
            http_client=http_client,
        )
        parsed_room = room_fetch["parsed_room"]
        if parsed_room.threads:
            probe_thread = parsed_room.threads[0]
            try:
                fetch_comments_payload(
                    probe_thread.thread_url,
                    cookie=cookie,
                    request_timeout_seconds=request_timeout_seconds,
                    http_client=http_client,
                )
                return ValidationOutcome(
                    room_url=target.room_url,
                    room_id=target.room_id,
                    status="valid",
                    message="Substack cookie validated against room posts and comments APIs.",
                    checked_at=checked_at,
                    discovered_threads=len(parsed_room.threads),
                )
            except SubstackArchiveError as exc:
                if exc.status in {"expired", "invalid_access"}:
                    return ValidationOutcome(
                        room_url=target.room_url,
                        room_id=target.room_id,
                        status=exc.status,
                        message=str(exc),
                        checked_at=checked_at,
                        discovered_threads=len(parsed_room.threads),
                    )
        html_response = fetch_substack_html(
            target.room_url,
            cookie=cookie,
            referer_url=target.room_url,
            request_timeout_seconds=request_timeout_seconds,
            http_client=http_client,
        )
        auth = evaluate_substack_room_auth(html_response.text, target.room_url, html_response.final_url)
        return ValidationOutcome(
            room_url=target.room_url,
            room_id=target.room_id,
            status=auth.status,
            message=auth.message,
            checked_at=checked_at,
            discovered_threads=max(len(parsed_room.threads), len(auth.parsed_room.threads)),
        )
    except SubstackArchiveError as exc:
        return ValidationOutcome(
            room_url=target.room_url,
            room_id=target.room_id,
            status=exc.status,
            message=str(exc),
            checked_at=checked_at,
        )


def sync_rooms(
    room_targets: list[RoomTarget],
    *,
    archive_dir: str | Path,
    cookie: str,
    max_threads_per_room: int | None = None,
    max_comment_pages: int = DEFAULT_MAX_COMMENT_PAGES,
    request_timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    http_client: HttpClient | None = None,
) -> list[SyncRoomResult]:
    if not cookie:
        raise SubstackArchiveError("auth_required", "A Substack session cookie is required.")

    archive_root = Path(archive_dir)
    archive_root.mkdir(parents=True, exist_ok=True)
    client = http_client or UrllibHttpClient()
    results: list[SyncRoomResult] = []

    for target in room_targets:
        result = sync_room(
            target,
            archive_root=archive_root,
            cookie=cookie,
            max_threads_per_room=max_threads_per_room,
            max_comment_pages=max_comment_pages,
            request_timeout_seconds=request_timeout_seconds,
            http_client=client,
        )
        results.append(result)

    write_manifest(archive_root, results)
    return results


def sync_room(
    target: RoomTarget,
    *,
    archive_root: Path,
    cookie: str,
    max_threads_per_room: int | None,
    max_comment_pages: int,
    request_timeout_seconds: float,
    http_client: HttpClient,
) -> SyncRoomResult:
    checked_at = utc_now_iso()
    room_fetch = fetch_room_posts(
        target.room_url,
        cookie=cookie,
        request_timeout_seconds=request_timeout_seconds,
        http_client=http_client,
    )
    parsed_room = room_fetch["parsed_room"]
    discovered_threads = parsed_room.threads[:max_threads_per_room] if max_threads_per_room else parsed_room.threads
    room_hash = build_substack_room_hash(discovered_threads)

    room_dir = archive_root / "rooms" / target.room_id
    threads_dir = room_dir / "threads"
    raw_dir = room_dir / "raw"
    normalized_room_path = room_dir / "room.json"
    state_path = room_dir / "state.json"

    threads_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    write_json(raw_dir / "room-posts-latest.json", room_fetch["payload"])

    state = read_json(state_path, default={})
    per_thread_state = state.get("threads") if isinstance(state.get("threads"), dict) else {}
    known_thread_ids = set(state.get("known_thread_ids") or [])

    created_threads = 0
    updated_threads = 0
    skipped_threads = 0
    fetched_threads = 0
    partial_threads = 0

    for preview in discovered_threads:
        previous_state = per_thread_state.get(preview.thread_id) if isinstance(per_thread_state, dict) else None
        hydrated = hydrate_thread(
            preview,
            room_id=target.room_id,
            room_url=target.room_url,
            cookie=cookie,
            max_comment_pages=max_comment_pages,
            request_timeout_seconds=request_timeout_seconds,
            http_client=http_client,
        )
        fetched_threads += 1
        detail = hydrated["detail"]
        thread_payload = build_thread_archive_payload(detail, source_notes=hydrated["source_notes"])
        detail.content_hash = thread_payload["content_hash"]
        content_hash = thread_payload["content_hash"]
        previous_hash = previous_state.get("content_hash") if isinstance(previous_state, dict) else None

        if detail.partial_transcript:
            partial_threads += 1

        thread_json_path = threads_dir / f"{detail.thread_id}.json"
        thread_md_path = threads_dir / f"{detail.thread_id}.md"
        if previous_hash != content_hash or not thread_json_path.exists() or not thread_md_path.exists():
            write_json(thread_json_path, thread_payload)
            write_text(thread_md_path, build_substack_thread_transcript(detail) + "\n")
            if previous_hash:
                updated_threads += 1
            else:
                created_threads += 1
        else:
            skipped_threads += 1

        for raw_name, raw_payload in hydrated["raw"].items():
            if raw_name.endswith(".json"):
                write_json(raw_dir / raw_name, raw_payload)
            else:
                write_text(raw_dir / raw_name, str(raw_payload))

        if not isinstance(per_thread_state, dict):
            per_thread_state = {}
        per_thread_state[detail.thread_id] = {
            "thread_url": detail.thread_url,
            "content_hash": content_hash,
            "reply_count": detail.reply_count,
            "parsed_reply_count": detail.parsed_reply_count,
            "partial_transcript": detail.partial_transcript,
            "preview_text": preview.preview_text,
            "last_synced_at": checked_at,
        }
        known_thread_ids.add(detail.thread_id)

    room_payload = {
        "schema_version": SCHEMA_VERSION,
        "room_id": target.room_id,
        "room_url": target.room_url,
        "thread_count": len(discovered_threads),
        "room_hash": room_hash,
        "last_sync_at": checked_at,
        "max_threads_per_room": max_threads_per_room,
        "max_comment_pages": max_comment_pages,
    }
    state_payload = {
        "schema_version": SCHEMA_VERSION,
        "room_id": target.room_id,
        "room_url": target.room_url,
        "room_hash": room_hash,
        "known_thread_ids": sorted(known_thread_ids),
        "threads": per_thread_state,
        "last_sync_at": checked_at,
    }
    write_json(normalized_room_path, room_payload)
    write_json(state_path, state_payload)

    message = (
        f"Synced {len(discovered_threads)} visible threads; "
        f"created {created_threads}, updated {updated_threads}, skipped {skipped_threads}."
    )
    return SyncRoomResult(
        room_url=target.room_url,
        room_id=target.room_id,
        status="ok",
        message=message,
        checked_at=checked_at,
        discovered_threads=len(discovered_threads),
        fetched_threads=fetched_threads,
        created_threads=created_threads,
        updated_threads=updated_threads,
        skipped_threads=skipped_threads,
        partial_threads=partial_threads,
    )


def fetch_room_posts(
    room_url: str,
    *,
    cookie: str,
    request_timeout_seconds: float,
    http_client: HttpClient,
) -> dict[str, Any]:
    api_url = build_substack_room_posts_api_url(room_url)
    if not api_url:
        raise SubstackArchiveError("parse_failed", f"Could not build room posts API URL for {room_url}", url=room_url)
    response = fetch_substack_json(
        api_url,
        cookie=cookie,
        referer_url=room_url,
        request_timeout_seconds=request_timeout_seconds,
        http_client=http_client,
    )
    payload = parse_json_response(response.text)
    parsed_room = parse_substack_room_posts_api_payload(payload, room_url)
    if parsed_room.chat_id is None:
        raise SubstackArchiveError("parse_failed", "Room posts payload did not contain a valid room identifier.", url=api_url)
    return {"response": response, "payload": payload, "parsed_room": parsed_room}


def fetch_comments_payload(
    thread_url: str,
    *,
    cookie: str,
    request_timeout_seconds: float,
    http_client: HttpClient,
    before: str | None = None,
    initial: bool = True,
) -> tuple[HttpResponse, Any]:
    api_url = build_substack_thread_comments_api_url(
        thread_url,
        order="desc" if before else "asc",
        initial=initial,
        before=before,
    )
    if not api_url:
        raise SubstackArchiveError("parse_failed", f"Could not build comments API URL for {thread_url}", url=thread_url)
    response = fetch_substack_json(
        api_url,
        cookie=cookie,
        referer_url=thread_url,
        request_timeout_seconds=request_timeout_seconds,
        http_client=http_client,
    )
    payload = parse_json_response(response.text)
    if payload is None:
        raise classify_non_json_response(response)
    return response, payload


def fetch_substack_html(
    url: str,
    *,
    cookie: str,
    referer_url: str,
    request_timeout_seconds: float,
    http_client: HttpClient,
) -> HttpResponse:
    response = http_client.get(url, build_substack_headers(cookie, referer_url), request_timeout_seconds)
    return classify_http_response(response)


def fetch_substack_json(
    url: str,
    *,
    cookie: str,
    referer_url: str,
    request_timeout_seconds: float,
    http_client: HttpClient,
) -> HttpResponse:
    response = http_client.get(url, build_substack_api_headers(cookie, referer_url), request_timeout_seconds)
    return classify_http_response(response)


def classify_http_response(response: HttpResponse) -> HttpResponse:
    final_url = response.final_url.lower()
    if "/signin" in final_url or "/login" in final_url or "/sign-in" in final_url:
        raise SubstackArchiveError("expired", "Substack redirected to sign in. Cookie may be expired.", url=response.url, http_status=response.status_code)
    if response.status_code in {401, 403}:
        raise SubstackArchiveError("expired", "Substack rejected the cookie. Session may be expired.", url=response.url, http_status=response.status_code)
    if response.status_code == 402:
        raise SubstackArchiveError("invalid_access", "Cookie is valid but does not have access to this Substack room or thread.", url=response.url, http_status=response.status_code)
    if response.status_code >= 400:
        raise SubstackArchiveError("http_failed", f"Substack request failed with HTTP {response.status_code}.", url=response.url, http_status=response.status_code)
    return response


def classify_non_json_response(response: HttpResponse) -> SubstackArchiveError:
    plain = strip_html_to_plain(response.text)[:2000]
    if re.search(r"sign in|log in|continue with email|continue with google", plain, flags=re.IGNORECASE):
        return SubstackArchiveError("expired", "Substack returned a login page instead of JSON.", url=response.url, http_status=response.status_code)
    if re.search(r"subscribe to continue|upgrade to continue|paid subscription", plain, flags=re.IGNORECASE):
        return SubstackArchiveError("invalid_access", "Substack returned an access-gated page instead of JSON.", url=response.url, http_status=response.status_code)
    return SubstackArchiveError("parse_failed", "Substack API response was not valid JSON.", url=response.url, http_status=response.status_code)


def hydrate_thread(
    preview: Any,
    *,
    room_id: str,
    room_url: str,
    cookie: str,
    max_comment_pages: int,
    request_timeout_seconds: float,
    http_client: HttpClient,
) -> dict[str, Any]:
    api_detail: SubstackThreadDetail | None = None
    html_detail: SubstackThreadDetail | None = None
    source_notes: list[str] = []
    raw_artifacts: dict[str, Any] = {}
    reply_total_hint = preview.displayed_reply_count or 0

    try:
        initial_response, initial_payload = fetch_comments_payload(
            preview.thread_url,
            cookie=cookie,
            request_timeout_seconds=request_timeout_seconds,
            http_client=http_client,
        )
        raw_artifacts[f"thread-{preview.thread_id}-comments-page-0.json"] = initial_payload
        parsed_comments = parse_substack_thread_comments_api_payload(initial_payload, preview.thread_url)
        merged_replies = sort_substack_replies_ascending(parsed_comments.replies)
        total_reply_count = parsed_comments.reply_count or preview.displayed_reply_count or parsed_comments.parsed_reply_count
        has_more = parsed_comments.has_more
        more_after = parsed_comments.more_after
        more_before = parsed_comments.more_before
        nested_reply_count = parsed_comments.nested_reply_count
        before_cursor = get_oldest_substack_reply_cursor(merged_replies)
        pages_fetched = 0

        while (
            before_cursor
            and len(merged_replies) < total_reply_count
            and pages_fetched < max_comment_pages
        ):
            page_response, page_payload = fetch_comments_payload(
                preview.thread_url,
                cookie=cookie,
                request_timeout_seconds=request_timeout_seconds,
                http_client=http_client,
                before=before_cursor,
                initial=False,
            )
            _ = page_response
            raw_artifacts[f"thread-{preview.thread_id}-comments-page-{pages_fetched + 1}.json"] = page_payload
            page_parsed = parse_substack_thread_comments_api_payload(page_payload, preview.thread_url)
            if not page_parsed.replies:
                break
            next_replies = merge_substack_replies(merged_replies, page_parsed.replies)
            next_before_cursor = get_oldest_substack_reply_cursor(next_replies)
            merged_replies = next_replies
            has_more = has_more or page_parsed.has_more
            more_after = more_after or page_parsed.more_after
            more_before = more_before or page_parsed.more_before
            nested_reply_count += page_parsed.nested_reply_count
            pages_fetched += 1
            if not next_before_cursor or next_before_cursor == before_cursor:
                break
            before_cursor = next_before_cursor

        api_detail = SubstackThreadDetail(
            room_id=room_id,
            thread_id=preview.thread_id,
            thread_url=preview.thread_url,
            root_author=parsed_comments.root_author or preview.author_name,
            root_handle=parsed_comments.root_handle or preview.author_handle,
            root_body=(parsed_comments.root_body or preview.root_body or preview.preview_text or "").strip(),
            root_attachments=parsed_comments.root_attachments or list(preview.root_attachments or []),
            published_at=parsed_comments.published_at or preview.published_at,
            reply_count=total_reply_count,
            parsed_reply_count=len(merged_replies),
            replies=merged_replies,
            raw_time_text=preview.last_activity_at,
            partial_transcript=(
                len(merged_replies) < total_reply_count
                or has_more
                or more_after
                or more_before
                or nested_reply_count > 0
            ),
        )
        source_notes.append("Primary hydration: comments API")
        if api_detail.partial_transcript:
            source_notes.append("Comments API transcript is partial; HTML fallback attempted.")
    except SubstackArchiveError as exc:
        if exc.status in {"expired", "invalid_access"}:
            raise
        source_notes.append(f"Comments API unavailable: {exc}")

    needs_html_fallback = (
        api_detail is None
        or api_detail.partial_transcript
        or not api_detail.root_body.strip()
        or not api_detail.root_author
    )
    if needs_html_fallback:
        try:
            html_response = fetch_substack_html(
                preview.thread_url,
                cookie=cookie,
                referer_url=room_url,
                request_timeout_seconds=request_timeout_seconds,
                http_client=http_client,
            )
            raw_artifacts[f"thread-{preview.thread_id}.html"] = html_response.text
            html_detail = parse_substack_chat_thread_detail(html_response.text, preview.thread_url)
            source_notes.append("Thread HTML fallback used.")
        except SubstackArchiveError as exc:
            if exc.status in {"expired", "invalid_access"}:
                raise
            if api_detail is None:
                raise
            source_notes.append(f"Thread HTML fallback unavailable: {exc}")

    detail = merge_thread_details(api_detail, html_detail, preview, room_id=room_id, reply_total_hint=reply_total_hint)
    if not detail.thread_id:
        raise SubstackArchiveError("parse_failed", f"Could not determine thread id for {preview.thread_url}", url=preview.thread_url)
    if detail.content_hash is None:
        detail.content_hash = build_thread_content_hash(detail)
    return {
        "detail": detail,
        "source_notes": source_notes,
        "raw": raw_artifacts,
    }


def merge_thread_details(
    api_detail: SubstackThreadDetail | None,
    html_detail: SubstackThreadDetail | None,
    preview: Any,
    *,
    room_id: str,
    reply_total_hint: int,
) -> SubstackThreadDetail:
    replies = []
    if api_detail:
        replies = merge_substack_replies(replies, api_detail.replies)
    if html_detail:
        replies = merge_substack_replies(replies, html_detail.replies)
    reply_count = (
        (api_detail.reply_count if api_detail else None)
        or preview.displayed_reply_count
        or (html_detail.reply_count if html_detail else None)
        or reply_total_hint
        or len(replies)
    )
    partial = False
    if reply_count is not None and len(replies) < reply_count:
        partial = True
    if api_detail and api_detail.partial_transcript:
        partial = True
    if html_detail and html_detail.partial_transcript and len(replies) < (html_detail.reply_count or len(replies)):
        partial = True

    detail = SubstackThreadDetail(
        room_id=room_id,
        thread_id=(api_detail.thread_id if api_detail else None) or (html_detail.thread_id if html_detail else None) or preview.thread_id,
        thread_url=preview.thread_url,
        root_author=(api_detail.root_author if api_detail else None) or (html_detail.root_author if html_detail else None) or preview.author_name,
        root_handle=(api_detail.root_handle if api_detail else None) or (html_detail.root_handle if html_detail else None) or preview.author_handle,
        root_body=(
            (api_detail.root_body.strip() if api_detail and api_detail.root_body else "")
            or (html_detail.root_body.strip() if html_detail and html_detail.root_body else "")
            or (preview.root_body or preview.preview_text or "").strip()
        ),
        root_attachments=(api_detail.root_attachments if api_detail and api_detail.root_attachments else [])
        or (html_detail.root_attachments if html_detail and html_detail.root_attachments else [])
        or list(preview.root_attachments or []),
        published_at=(api_detail.published_at if api_detail else None) or (html_detail.published_at if html_detail else None) or preview.published_at,
        reply_count=reply_count,
        parsed_reply_count=len(replies),
        replies=sort_substack_replies_ascending(replies),
        raw_time_text=(api_detail.raw_time_text if api_detail else None) or (html_detail.raw_time_text if html_detail else None) or preview.last_activity_at,
        partial_transcript=partial,
    )
    detail.content_hash = build_thread_content_hash(detail)
    return detail


def build_thread_archive_payload(detail: SubstackThreadDetail, *, source_notes: list[str]) -> dict[str, Any]:
    replies = []
    for reply in sort_substack_replies_ascending(detail.replies):
        replies.append(
            {
                "id": reply.comment_id,
                "author": reply.author_name,
                "handle": reply.author_handle,
                "timestamp": to_substack_cursor_timestamp(reply.raw_time_text),
                "raw_time_text": reply.raw_time_text,
                "body": reply.body_text,
                "attachments": serialize_dataclass(reply.attachments),
            }
        )
    transcript = build_substack_thread_transcript(detail)
    content_hash = build_thread_content_hash(detail)
    return {
        "schema_version": SCHEMA_VERSION,
        "room_id": detail.room_id,
        "thread_id": detail.thread_id,
        "thread_url": detail.thread_url,
        "root_author": detail.root_author,
        "root_handle": detail.root_handle,
        "root_body": detail.root_body,
        "root_attachments": serialize_dataclass(detail.root_attachments),
        "published_at": detail.published_at,
        "reply_count": detail.reply_count,
        "parsed_reply_count": detail.parsed_reply_count,
        "partial_transcript": detail.partial_transcript,
        "replies": replies,
        "content_hash": content_hash,
        "transcript_markdown": transcript,
        "source_notes": source_notes,
    }


def write_manifest(archive_root: Path, results: list[SyncRoomResult]) -> None:
    manifest_payload = {
        "schema_version": SCHEMA_VERSION,
        "skill": "substack-chat-archive",
        "generated_at": utc_now_iso(),
        "rooms": [
            {
                "room_id": result.room_id,
                "room_url": result.room_url,
                "status": result.status,
                "path": f"rooms/{result.room_id}/room.json" if result.room_id else None,
                "last_sync_at": result.checked_at,
            }
            for result in results
        ],
    }
    write_json(archive_root / "manifest.json", manifest_payload)


def read_json(path: str | Path, *, default: Any) -> Any:
    file_path = Path(path)
    if not file_path.exists():
        return default
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def write_json(path: str | Path, payload: Any) -> None:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=False) + "\n", encoding="utf-8")


def write_text(path: str | Path, content: str) -> None:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content, encoding="utf-8")


def _should_hydrate_thread(preview: Any, previous_state: Any) -> bool:
    if not isinstance(previous_state, dict):
        return True
    previous_reply_count = previous_state.get("reply_count")
    previous_preview = previous_state.get("preview_text") or ""
    previous_partial = previous_state.get("partial_transcript") is True
    if previous_partial:
        return True
    if preview.displayed_reply_count != previous_reply_count:
        return True
    if (preview.preview_text or "") != previous_preview:
        return True
    return False


def _coerce_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"Config field '{field_name}' must be an integer.")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    raise ValueError(f"Config field '{field_name}' must be an integer.")


def _coerce_optional_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    return _coerce_int(value, field_name)


def _coerce_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"Config field '{field_name}' must be numeric.")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value.strip())
        except ValueError as exc:
            raise ValueError(f"Config field '{field_name}' must be numeric.") from exc
    raise ValueError(f"Config field '{field_name}' must be numeric.")


def _load_structured_config(path: Path) -> Any:
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return _parse_simple_yaml(text)


def _parse_simple_yaml(text: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    current_key: str | None = None
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        stripped = line.strip()
        if stripped.startswith("- "):
            if current_key is None or not isinstance(result.get(current_key), list):
                raise ValueError("Unsupported YAML structure in config.")
            result[current_key].append(_parse_yaml_scalar(stripped[2:]))
            continue
        if ":" not in stripped:
            raise ValueError("Unsupported YAML structure in config.")
        key, raw_value = stripped.split(":", 1)
        key = key.strip()
        raw_value = raw_value.strip()
        if not raw_value:
            result[key] = []
            current_key = key
        else:
            result[key] = _parse_yaml_scalar(raw_value)
            current_key = None
    return result


def _parse_yaml_scalar(value: str) -> Any:
    trimmed = value.strip()
    if not trimmed or trimmed in {"null", "~"}:
        return None
    if trimmed.lower() == "true":
        return True
    if trimmed.lower() == "false":
        return False
    if (trimmed.startswith('"') and trimmed.endswith('"')) or (trimmed.startswith("'") and trimmed.endswith("'")):
        return trimmed[1:-1]
    if re.fullmatch(r"-?\d+", trimmed):
        return int(trimmed)
    if re.fullmatch(r"-?\d+\.\d+", trimmed):
        return float(trimmed)
    return trimmed
