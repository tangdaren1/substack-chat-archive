#!/usr/bin/env python3
"""Pure parsing helpers for Substack Chat room and thread content."""

from __future__ import annotations

import html
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any
from urllib.parse import urlencode, urlparse


PROFILE_LINK_REGEX = re.compile(
    r"""<a\b[^>]*href=(?:"((?:https?:\/\/substack\.com)?\/@[A-Za-z0-9_-]+)"|'((?:https?:\/\/substack\.com)?\/@[A-Za-z0-9_-]+)'|((?:https?:\/\/substack\.com)?\/@[A-Za-z0-9_-]+))[^>]*>([^<]{1,120})<\/a>""",
    re.IGNORECASE,
)
BODY_BLOCK_REGEX = re.compile(r"""<div\b[^>]*\bclass=(["'])[^"']*\bbody-[^"']*\1[^>]*>([\s\S]*?)<\/div>""", re.IGNORECASE)
COMMENT_ID_REGEX = re.compile(r"""id=(?:["'])?comment-([0-9a-f-]{36})\b""", re.IGNORECASE)
THREAD_ID_REGEX = re.compile(r"/chat/(\d+)/post/([0-9a-f-]{36})", re.IGNORECASE)
UUID_REGEX = re.compile(r"^[0-9a-f-]{36}$", re.IGNORECASE)
RELATIVE_TIME_REGEX = re.compile(r"\b(?:Today|Yesterday)(?:\s+at)?\s+\d{1,2}:\d{2}\b", re.IGNORECASE)
ABSOLUTE_TIME_REGEX = re.compile(r"\b[A-Z][a-z]{2,8}\s+\d{1,2}(?:,\s+\d{4})?(?:\s+at)?\s+\d{1,2}:\d{2}\b")


@dataclass
class SubstackAttachment:
    type: str | None
    url: str
    thumb_url: str | None = None
    width: int | None = None
    height: int | None = None
    explicit: bool | None = None
    caption: str | None = None


@dataclass
class SubstackRoomThreadPreview:
    thread_url: str
    thread_id: str
    author_name: str | None
    author_handle: str | None
    preview_text: str
    displayed_reply_count: int | None
    root_body: str | None = None
    root_attachments: list[SubstackAttachment] = field(default_factory=list)
    published_at: str | None = None
    last_activity_at: str | None = None


@dataclass
class SubstackRoomParseResult:
    chat_id: str | None
    threads: list[SubstackRoomThreadPreview]
    has_authenticated_markers: bool


@dataclass
class SubstackThreadReply:
    comment_id: str
    author_name: str | None
    author_handle: str | None
    body_text: str
    raw_time_text: str | None
    attachments: list[SubstackAttachment] = field(default_factory=list)


@dataclass
class SubstackThreadDetail:
    room_id: str | None
    thread_id: str | None
    thread_url: str
    root_author: str | None
    root_handle: str | None
    root_body: str
    root_attachments: list[SubstackAttachment]
    published_at: str | None
    reply_count: int | None
    parsed_reply_count: int
    replies: list[SubstackThreadReply]
    raw_time_text: str | None
    partial_transcript: bool
    content_hash: str | None = None


@dataclass
class SubstackThreadCommentsParseResult:
    thread_id: str | None
    root_author: str | None
    root_handle: str | None
    root_body: str
    root_attachments: list[SubstackAttachment]
    published_at: str | None
    reply_count: int | None
    replies: list[SubstackThreadReply]
    parsed_reply_count: int
    has_more: bool
    more_after: bool
    more_before: bool
    nested_reply_count: int


@dataclass
class SubstackAuthEvaluation:
    success: bool
    status: str
    message: str
    parsed_room: SubstackRoomParseResult


def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def decode_html_entities(value: str) -> str:
    return html.unescape(value or "").replace("\xa0", " ")


def is_record(value: Any) -> bool:
    return isinstance(value, dict)


def get_string(record: dict[str, Any], key: str) -> str | None:
    value = record.get(key)
    return value if isinstance(value, str) else None


def get_number(record: dict[str, Any], key: str) -> int | None:
    value = record.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def get_boolean(record: dict[str, Any], key: str) -> bool | None:
    value = record.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return None


def get_id_like(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if float(value).is_integer():
            return str(int(value))
        return str(value)
    return None


def strip_html_to_plain(value: str) -> str:
    normalized = decode_html_entities(
        (value or "")
        .replace("\r", "")
        .replace("<br/>", "\n")
        .replace("<br />", "\n")
        .replace("<br>", "\n")
    )
    normalized = re.sub(r"</p>", "\n", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"</div>", "\n", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"</li>", "\n", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"<li[^>]*>", "- ", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"<[^>]+>", " ", normalized)
    lines = []
    for line in normalized.split("\n"):
        cleaned = re.sub(r"\s+", " ", line).strip()
        if cleaned:
            lines.append(cleaned)
    return "\n".join(lines)


def get_first_non_empty_text(record: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        direct = get_string(record, key)
        if direct and strip_html_to_plain(direct).strip():
            return strip_html_to_plain(direct).strip()
        nested = record.get(key)
        if is_record(nested):
            nested_direct = get_string(nested, "text") or get_string(nested, "body") or get_string(nested, "content")
            if nested_direct and strip_html_to_plain(nested_direct).strip():
                return strip_html_to_plain(nested_direct).strip()
    return ""


def find_author_record(value: Any) -> dict[str, Any] | None:
    if not is_record(value):
        return None

    direct_candidates = [
        value.get("user"),
        value.get("author"),
        value.get("commenter"),
        value.get("owner"),
        value.get("actor"),
        value.get("profile"),
        value.get("participant"),
    ]
    for candidate in direct_candidates:
        if not is_record(candidate):
            continue
        if get_string(candidate, "name") or get_string(candidate, "handle") or get_string(candidate, "username"):
            return candidate
        nested_user = candidate.get("user")
        if is_record(nested_user) and (
            get_string(nested_user, "name") or get_string(nested_user, "handle") or get_string(nested_user, "username")
        ):
            return nested_user
    return None


def extract_substack_attachments(value: Any) -> list[SubstackAttachment]:
    if not isinstance(value, list):
        return []

    attachments: list[SubstackAttachment] = []
    seen: set[str] = set()
    for entry in value:
        if not is_record(entry):
            continue
        url = get_first_non_empty_text(entry, ["url", "href"])
        if not url or url in seen:
            continue
        attachments.append(
            SubstackAttachment(
                type=get_string(entry, "type") or get_string(entry, "content_type") or get_string(entry, "contentType"),
                url=url,
                thumb_url=get_string(entry, "thumb_url") or get_string(entry, "thumbUrl"),
                width=get_number(entry, "width") or get_number(entry, "image_width") or get_number(entry, "imageWidth"),
                height=get_number(entry, "height") or get_number(entry, "image_height") or get_number(entry, "imageHeight"),
                explicit=get_boolean(entry, "explicit"),
                caption=get_first_non_empty_text(
                    entry,
                    [
                        "caption",
                        "title",
                        "name",
                        "alt_text",
                        "altText",
                        "description",
                        "original_filename",
                        "originalFilename",
                    ],
                )
                or None,
            )
        )
        seen.add(url)
    return attachments


def extract_substack_attachment_text(value: Any) -> str:
    attachments = extract_substack_attachments(value)
    if not attachments:
        return ""
    for attachment in attachments:
        if attachment.caption:
            return attachment.caption
        if attachment.url:
            return attachment.url
    return ""


def extract_substack_record_attachments(record: dict[str, Any]) -> list[SubstackAttachment]:
    attachments: list[SubstackAttachment] = []
    seen: set[str] = set()
    for key in (
        "mediaAttachments",
        "media_assets",
        "mediaAssets",
        "threadMediaUploads",
        "media_uploads",
        "mediaUploads",
    ):
        for attachment in extract_substack_attachments(record.get(key)):
            if attachment.url in seen:
                continue
            attachments.append(attachment)
            seen.add(attachment.url)
    return attachments


def normalize_substack_display_body(
    body: str,
    attachments: list[SubstackAttachment],
    fallback_label: str = "Attachment",
) -> str:
    trimmed = body.strip()
    if not trimmed:
        return f"({fallback_label})" if attachments else ""
    if not attachments:
        return trimmed

    attachment_urls: set[str] = set()
    for attachment in attachments:
        attachment_urls.add(attachment.url)
        if attachment.thumb_url:
            attachment_urls.add(attachment.thumb_url)

    normalized_lines = [line.strip() for line in trimmed.split("\n") if line.strip()]
    if normalized_lines and all(line in attachment_urls for line in normalized_lines):
        all_images = True
        for attachment in attachments:
            attachment_type = (attachment.type or "").lower()
            if (
                "image" not in attachment_type
                and not re.search(r"\.(png|jpe?g|gif|webp|avif)(?:\?|$)", attachment.url, flags=re.IGNORECASE)
                and not (
                    attachment.thumb_url
                    and re.search(r"\.(png|jpe?g|gif|webp|avif)(?:\?|$)", attachment.thumb_url, flags=re.IGNORECASE)
                )
            ):
                all_images = False
                break
        return "(Image attachment)" if all_images else f"({fallback_label})"
    return trimmed


def extract_substack_record_text(record: dict[str, Any]) -> str:
    direct = get_first_non_empty_text(
        record,
        [
            "body",
            "raw_body",
            "rawBody",
            "text",
            "content",
            "message",
            "body_html",
            "bodyHtml",
            "body_text",
            "bodyText",
            "truncated_body_text",
            "truncatedBodyText",
            "preview_text",
            "previewText",
            "description",
            "title",
            "subject",
            "subtitle",
            "caption",
            "link_url",
            "linkUrl",
        ],
    )
    if direct:
        return direct
    return (
        extract_substack_attachment_text(record.get("mediaAttachments"))
        or extract_substack_attachment_text(record.get("media_assets"))
        or extract_substack_attachment_text(record.get("mediaAssets"))
        or extract_substack_attachment_text(record.get("threadMediaUploads"))
        or extract_substack_attachment_text(record.get("media_uploads"))
        or extract_substack_attachment_text(record.get("mediaUploads"))
    )


def extract_body_html(html_value: str) -> str:
    match = re.search(r"<body\b[^>]*>([\s\S]*?)</body>", html_value, flags=re.IGNORECASE)
    return match.group(1) if match else html_value


def extract_meta_content(html_value: str, attribute: str, key: str) -> str | None:
    pattern = re.compile(
        rf"""<meta\b[^>]*{attribute}=(["']){re.escape(key)}\1[^>]*content=(["'])(.*?)\2""",
        flags=re.IGNORECASE,
    )
    match = pattern.search(html_value)
    return match.group(3) if match else None


def extract_title_text(html_value: str) -> str | None:
    match = re.search(r"<title\b[^>]*>([\s\S]*?)</title>", html_value, flags=re.IGNORECASE)
    return _clean_text(decode_html_entities(match.group(1))) if match else None


def find_profile_matches(html_value: str) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for match in PROFILE_LINK_REGEX.finditer(html_value):
        href = match.group(1) or match.group(2) or match.group(3) or ""
        handle_match = re.search(r"/@([A-Za-z0-9_-]+)", href)
        matches.append(
            {
                "index": match.start(),
                "handle": handle_match.group(1) if handle_match else "",
                "name": strip_html_to_plain(match.group(4) or ""),
            }
        )
    return matches


def find_nearest_profile_before(html_value: str, cutoff: int) -> dict[str, str | None]:
    matches = [entry for entry in find_profile_matches(html_value) if entry["index"] < cutoff]
    if not matches:
        return {"author_name": None, "author_handle": None}
    selected = matches[-1]
    return {
        "author_name": selected["name"] or None,
        "author_handle": selected["handle"] or None,
    }


def extract_first_body_text(html_value: str) -> str:
    for match in BODY_BLOCK_REGEX.finditer(html_value):
        text = strip_html_to_plain(match.group(2) or "").strip()
        if text:
            return text
    return ""


def extract_reply_count_from_text(html_value: str) -> int | None:
    match = re.search(r"\b(\d+)\s+replies?\b", html_value, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def extract_visible_time_text(html_value: str) -> str | None:
    plain = strip_html_to_plain(html_value)
    relative = RELATIVE_TIME_REGEX.search(plain)
    if relative:
        return relative.group(0)
    absolute = ABSOLUTE_TIME_REGEX.search(plain)
    return absolute.group(0) if absolute else None


def normalize_substack_cookie(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    normalized = value.strip()
    normalized = re.sub(r"^cookie\s*:\s*", "", normalized, flags=re.IGNORECASE)
    normalized = normalized.replace("\r\n", ";").replace("\n", ";")
    normalized = re.sub(r";{2,}", ";", normalized).strip()
    if not normalized:
        return ""
    if "=" not in normalized:
        return f"substack.sid={normalized}"

    cookie_attributes = {
        "path",
        "domain",
        "expires",
        "max-age",
        "samesite",
        "priority",
        "secure",
        "httponly",
        "partitioned",
    }
    cookies: list[str] = []
    seen: set[str] = set()
    for part in [entry.strip() for entry in normalized.split(";") if entry.strip()]:
        separator_index = part.find("=")
        if separator_index <= 0:
            continue
        name = part[:separator_index].strip()
        lower_name = name.lower()
        if lower_name in cookie_attributes:
            continue
        if lower_name in seen:
            for index, existing in enumerate(cookies):
                if existing.lower().startswith(f"{lower_name}="):
                    cookies[index] = part
                    break
            continue
        seen.add(lower_name)
        cookies.append(part)
    return "; ".join(cookies)


def extract_substack_chat_id(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    if trimmed.isdigit():
        return trimmed
    try:
        parsed = urlparse(trimmed)
    except ValueError:
        return None
    match = re.search(r"^/chat/(\d+)(?:/|$)", parsed.path, flags=re.IGNORECASE)
    return match.group(1) if match else None


def normalize_substack_chat_room_url(value: Any) -> str | None:
    chat_id = extract_substack_chat_id(value)
    return f"https://substack.com/chat/{chat_id}" if chat_id else None


def normalize_substack_thread_url(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    try:
        parsed = urlparse(trimmed)
    except ValueError:
        return None
    match = re.search(r"^/chat/(\d+)/post/([0-9a-f-]{36})(?:/|$)", parsed.path, flags=re.IGNORECASE)
    return f"https://substack.com/chat/{match.group(1)}/post/{match.group(2)}" if match else None


def extract_substack_thread_id(value: Any) -> str | None:
    normalized_url = normalize_substack_thread_url(value)
    if not normalized_url:
        return None
    match = THREAD_ID_REGEX.search(normalized_url)
    return match.group(2) if match else None


def build_substack_room_posts_api_url(room_url_or_id: Any) -> str | None:
    chat_id = extract_substack_chat_id(room_url_or_id)
    return f"https://substack.com/api/v1/community/publications/{chat_id}/posts" if chat_id else None


def build_substack_thread_comments_api_url(
    thread_url_or_id: Any,
    *,
    order: str = "asc",
    initial: bool = True,
    before: str | None = None,
    after: str | None = None,
) -> str | None:
    thread_id = extract_substack_thread_id(thread_url_or_id)
    if not thread_id and isinstance(thread_url_or_id, str) and UUID_REGEX.match(thread_url_or_id.strip()):
        thread_id = thread_url_or_id.strip()
    if not thread_id:
        return None
    params: dict[str, str] = {"order": order}
    if initial:
        params["initial"] = "true"
    if before:
        params["before"] = before
    if after:
        params["after"] = after
    return f"https://substack.com/api/v1/community/posts/{thread_id}/comments?{urlencode(params)}"


def parse_substack_chat_room(html_value: str, room_url: str) -> SubstackRoomParseResult:
    normalized_room_url = normalize_substack_chat_room_url(room_url)
    chat_id = extract_substack_chat_id(normalized_room_url) if normalized_room_url else None
    body_html = extract_body_html(html_value)
    if not chat_id:
        return SubstackRoomParseResult(chat_id=None, threads=[], has_authenticated_markers=False)

    thread_regex = re.compile(rf"(?:https?:\/\/substack\.com)?\/chat\/{re.escape(chat_id)}\/post\/([0-9a-f-]{{36}})", re.IGNORECASE)
    first_seen_by_url: dict[str, int] = {}
    for match in thread_regex.finditer(body_html):
        thread_url = f"https://substack.com/chat/{chat_id}/post/{match.group(1)}"
        first_seen_by_url.setdefault(thread_url, match.start())

    entries = sorted(
        [{"thread_url": thread_url, "index": index} for thread_url, index in first_seen_by_url.items()],
        key=lambda item: item["index"],
    )

    threads: list[SubstackRoomThreadPreview] = []
    for index, entry in enumerate(entries):
        next_index = entries[index + 1]["index"] if index + 1 < len(entries) else len(body_html)
        segment_start = max(0, entry["index"] - 1400)
        segment_end = min(len(body_html), max(next_index, entry["index"] + 2500))
        segment = body_html[segment_start:segment_end]
        entry_offset = entry["index"] - segment_start
        preview_text = extract_first_body_text(segment[max(0, entry_offset):])
        author = find_nearest_profile_before(segment, entry_offset + 50)
        displayed_reply_count = extract_reply_count_from_text(segment)
        thread_match = THREAD_ID_REGEX.search(entry["thread_url"])
        threads.append(
            SubstackRoomThreadPreview(
                thread_url=entry["thread_url"],
                thread_id=thread_match.group(2) if thread_match else "",
                author_name=author["author_name"],
                author_handle=author["author_handle"],
                preview_text=preview_text,
                displayed_reply_count=displayed_reply_count,
            )
        )

    has_authenticated_markers = bool(
        threads
        or re.search(r"Start a new thread", body_html, flags=re.IGNORECASE)
        or re.search(r"Drop file here to upload", body_html, flags=re.IGNORECASE)
    )
    return SubstackRoomParseResult(chat_id=chat_id, threads=threads, has_authenticated_markers=has_authenticated_markers)


def is_substack_room_posts_api_payload(payload: Any) -> bool:
    return is_record(payload) and isinstance(payload.get("threads"), list)


def parse_substack_room_posts_api_payload(payload: Any, room_url: str) -> SubstackRoomParseResult:
    normalized_room_url = normalize_substack_chat_room_url(room_url)
    chat_id = extract_substack_chat_id(normalized_room_url) if normalized_room_url else None
    if not chat_id or not is_substack_room_posts_api_payload(payload):
        return SubstackRoomParseResult(chat_id=chat_id, threads=[], has_authenticated_markers=False)

    threads: list[SubstackRoomThreadPreview] = []
    seen: set[str] = set()
    for raw_entry in payload["threads"]:
        entry = raw_entry if is_record(raw_entry) else None
        if not entry:
            continue
        post = entry.get("communityPost") if is_record(entry.get("communityPost")) else entry.get("post") if is_record(entry.get("post")) else entry
        if not is_record(post):
            continue
        thread_id = get_string(post, "id")
        if not thread_id or not UUID_REGEX.match(thread_id) or thread_id in seen:
            continue
        user = find_author_record(post) or find_author_record(entry)
        author_name = (get_string(user, "name") or get_string(user, "display_name")) if user else None
        author_handle = (get_string(user, "handle") or get_string(user, "username")) if user else None
        root_attachments = extract_substack_record_attachments(post) + extract_substack_record_attachments(entry)
        root_body = normalize_substack_display_body(extract_substack_record_text(post) or extract_substack_record_text(entry), root_attachments)
        if not root_body and not author_name and not author_handle:
            continue
        threads.append(
            SubstackRoomThreadPreview(
                thread_url=f"https://substack.com/chat/{chat_id}/post/{thread_id}",
                thread_id=thread_id,
                author_name=_clean_text(author_name) or None,
                author_handle=_clean_text(author_handle) or None,
                preview_text=root_body,
                displayed_reply_count=get_number(post, "comment_count") or get_number(post, "commentCount"),
                root_body=root_body,
                root_attachments=root_attachments,
                published_at=get_string(post, "created_at") or get_string(post, "createdAt"),
                last_activity_at=get_string(post, "max_comment_created_at")
                or get_string(post, "maxCommentCreatedAt")
                or get_string(post, "updated_at")
                or get_string(post, "updatedAt"),
            )
        )
        seen.add(thread_id)
    return SubstackRoomParseResult(chat_id=chat_id, threads=threads, has_authenticated_markers=bool(threads))


def parse_substack_chat_thread_detail(html_value: str, thread_url: str) -> SubstackThreadDetail:
    normalized_thread_url = normalize_substack_thread_url(thread_url) or thread_url.strip()
    thread_match = THREAD_ID_REGEX.search(normalized_thread_url)
    room_id = thread_match.group(1) if thread_match else None
    thread_id = thread_match.group(2) if thread_match else None
    body_html = extract_body_html(html_value)
    root_index = body_html.rfind(normalized_thread_url)
    root_window_start = max(0, root_index - 3000) if root_index >= 0 else 0
    root_window_end = min(len(body_html), root_index + 6500) if root_index >= 0 else min(len(body_html), 12000)
    root_window = body_html[root_window_start:root_window_end]
    root_offset = root_index - root_window_start if root_index >= 0 else 0

    root_author = find_nearest_profile_before(root_window, root_offset + 100)
    root_body = extract_first_body_text(root_window[max(0, root_offset):]) or extract_first_body_text(root_window)
    published_at = extract_meta_content(html_value, "property", "og:published_time")
    meta_reply_count = extract_meta_content(html_value, "name", "twitter:data2")
    reply_count_from_meta = int(meta_reply_count) if meta_reply_count and meta_reply_count.isdigit() else None
    reply_count_from_body = extract_reply_count_from_text(root_window)
    reply_count = reply_count_from_meta if reply_count_from_meta is not None else reply_count_from_body

    replies: list[SubstackThreadReply] = []
    comment_positions = [{"comment_id": match.group(1), "index": match.start()} for match in COMMENT_ID_REGEX.finditer(body_html)]
    for index, entry in enumerate(comment_positions):
        next_index = comment_positions[index + 1]["index"] if index + 1 < len(comment_positions) else len(body_html)
        block_start = max(0, entry["index"] - 900)
        block_end = min(len(body_html), max(next_index, entry["index"] + 2200))
        block = body_html[block_start:block_end]
        block_offset = entry["index"] - block_start
        author = find_nearest_profile_before(block, block_offset)
        body_text = extract_first_body_text(block[max(0, block_offset):]) or extract_first_body_text(block)
        if not body_text:
            continue
        replies.append(
            SubstackThreadReply(
                comment_id=entry["comment_id"],
                author_name=author["author_name"],
                author_handle=author["author_handle"],
                body_text=body_text,
                raw_time_text=extract_visible_time_text(block),
                attachments=[],
            )
        )

    parsed_reply_count = len(replies)
    partial_transcript = parsed_reply_count < reply_count if reply_count is not None else parsed_reply_count > 0
    return SubstackThreadDetail(
        room_id=room_id,
        thread_id=thread_id,
        thread_url=normalized_thread_url,
        root_author=root_author["author_name"],
        root_handle=root_author["author_handle"],
        root_body=root_body,
        root_attachments=[],
        published_at=published_at,
        reply_count=reply_count,
        parsed_reply_count=parsed_reply_count,
        replies=replies,
        raw_time_text=extract_visible_time_text(root_window),
        partial_transcript=partial_transcript,
    )


def parse_substack_thread_comments_api_payload(payload: Any, thread_url: str) -> SubstackThreadCommentsParseResult:
    thread_id = extract_substack_thread_id(thread_url)
    replies: list[SubstackThreadReply] = []
    seen: set[str] = set()
    payload_record = payload if is_record(payload) else None
    post_wrapper = payload_record.get("post") if payload_record and is_record(payload_record.get("post")) else None
    community_post = post_wrapper.get("communityPost") if is_record(post_wrapper) and is_record(post_wrapper.get("communityPost")) else None
    root_user = find_author_record(community_post) or find_author_record(post_wrapper)
    root_author = (get_string(root_user, "name") or get_string(root_user, "display_name")) if root_user else None
    root_handle = (get_string(root_user, "handle") or get_string(root_user, "username")) if root_user else None
    root_attachments = extract_substack_record_attachments(community_post) if is_record(community_post) else []
    root_body = (
        normalize_substack_display_body(
            extract_substack_record_text(community_post) or extract_substack_record_text(post_wrapper or {}),
            root_attachments,
        )
        if is_record(community_post)
        else ""
    )
    published_at = (
        get_string(community_post, "created_at") or get_string(community_post, "createdAt")
    ) if is_record(community_post) else None
    reply_count = (
        get_number(community_post, "comment_count") or get_number(community_post, "commentCount")
    ) if is_record(community_post) else None
    has_more = payload_record.get("more") is True if payload_record else False
    more_after = payload_record.get("moreAfter") is True if payload_record else False
    more_before = payload_record.get("moreBefore") is True if payload_record else False
    nested_reply_count = 0

    def push_reply(
        comment_record: dict[str, Any] | None,
        user_record: dict[str, Any] | None,
        fallback_record: dict[str, Any] | None = None,
    ) -> bool:
        nonlocal nested_reply_count
        if not comment_record:
            return False
        candidate_id = (
            get_id_like(comment_record.get("id"))
            or get_id_like(comment_record.get("comment_id"))
            or get_id_like(comment_record.get("commentId"))
            or get_id_like(comment_record.get("community_post_comment_id"))
            or get_id_like(comment_record.get("communityPostCommentId"))
        )
        candidate_status = get_string(comment_record, "status") or (get_string(fallback_record, "status") if fallback_record else None)
        candidate_attachments = extract_substack_record_attachments(comment_record)
        candidate_body = normalize_substack_display_body(extract_substack_record_text(comment_record), candidate_attachments)
        candidate_user = user_record or find_author_record(comment_record) or find_author_record(fallback_record)
        candidate_author_name = (
            get_string(candidate_user, "name") or get_string(candidate_user, "display_name")
        ) if candidate_user else None
        candidate_author_handle = (
            get_string(candidate_user, "handle") or get_string(candidate_user, "username")
        ) if candidate_user else None
        if (
            not candidate_id
            or candidate_id == thread_id
            or candidate_id in seen
            or candidate_status == "deleted"
            or not candidate_body
            or (not candidate_author_name and not candidate_author_handle)
        ):
            return False
        nested_reply_count += get_number(comment_record, "reply_count") or get_number(comment_record, "replyCount") or 0
        replies.append(
            SubstackThreadReply(
                comment_id=candidate_id,
                author_name=_clean_text(candidate_author_name) or None,
                author_handle=_clean_text(candidate_author_handle) or None,
                body_text=candidate_body,
                raw_time_text=get_string(comment_record, "created_at")
                or get_string(comment_record, "createdAt")
                or get_string(comment_record, "updated_at")
                or get_string(comment_record, "updatedAt")
                or (get_string(fallback_record, "created_at") if fallback_record else None)
                or (get_string(fallback_record, "createdAt") if fallback_record else None),
                attachments=candidate_attachments,
            )
        )
        seen.add(candidate_id)
        return True

    def visit(value: Any, depth: int) -> None:
        if depth > 8 or value is None:
            return
        if isinstance(value, list):
            for entry in value:
                visit(entry, depth + 1)
            return
        if not is_record(value):
            return
        if push_reply(
            value.get("comment") if is_record(value.get("comment")) else None,
            value.get("user") if is_record(value.get("user")) else value.get("author") if is_record(value.get("author")) else None,
            value,
        ):
            return
        candidate_id = (
            get_id_like(value.get("id"))
            or get_id_like(value.get("comment_id"))
            or get_id_like(value.get("commentId"))
            or get_id_like(value.get("community_post_comment_id"))
            or get_id_like(value.get("communityPostCommentId"))
        )
        if candidate_id and push_reply(value, find_author_record(value), value):
            return
        for nested in value.values():
            visit(nested, depth + 1)

    visit(payload, 0)
    return SubstackThreadCommentsParseResult(
        thread_id=thread_id,
        root_author=_clean_text(root_author) or None,
        root_handle=_clean_text(root_handle) or None,
        root_body=root_body,
        root_attachments=root_attachments,
        published_at=published_at,
        reply_count=reply_count,
        replies=replies,
        parsed_reply_count=len(replies),
        has_more=has_more,
        more_after=more_after,
        more_before=more_before,
        nested_reply_count=nested_reply_count,
    )


def build_substack_thread_transcript(detail: SubstackThreadDetail) -> str:
    replies = sort_substack_replies_ascending(detail.replies)
    lines = ["# Thread", "", "## Root Post", f"Author: {detail.root_author or 'Unknown'}"]
    if detail.root_handle:
        lines.append(f"Handle: @{detail.root_handle}")
    if detail.published_at:
        lines.append(f"Posted: {detail.published_at}")
    elif detail.raw_time_text:
        lines.append(f"Posted: {detail.raw_time_text}")
    lines.append(f"Partial transcript: {'yes' if detail.partial_transcript else 'no'}")
    if detail.root_attachments:
        lines.append(f"Attachments: {len(detail.root_attachments)}")
    lines.append("")
    lines.append(detail.root_body or "(No root post content parsed)")
    lines.append("")
    total_replies = detail.reply_count if detail.reply_count is not None else detail.parsed_reply_count
    lines.append(f"# Replies (total {total_replies}, parsed {detail.parsed_reply_count})")
    if not replies:
        lines.extend(["", "(No replies parsed)"])
        return "\n".join(lines).strip()
    for reply in replies:
        lines.extend(["", f"## {reply.author_name or 'Unknown'}"])
        if reply.author_handle:
            lines.append(f"Handle: @{reply.author_handle}")
        if reply.raw_time_text:
            lines.append(f"Time: {reply.raw_time_text}")
        if reply.attachments:
            lines.append(f"Attachments: {len(reply.attachments)}")
        lines.extend(["", reply.body_text])
    return "\n".join(lines).strip()


def to_substack_cursor_timestamp(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed_timestamp = datetime.strptime(value, "%b %d, %Y at %H:%M")
            parsed = parsed_timestamp.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sort_substack_replies_ascending(replies: list[SubstackThreadReply]) -> list[SubstackThreadReply]:
    return sorted(
        replies,
        key=lambda reply: (
            to_substack_cursor_timestamp(reply.raw_time_text) or "",
            reply.comment_id,
        ),
    )


def merge_substack_replies(current: list[SubstackThreadReply], incoming: list[SubstackThreadReply]) -> list[SubstackThreadReply]:
    by_id: dict[str, SubstackThreadReply] = {}
    for reply in current + incoming:
        by_id[reply.comment_id] = reply
    return sort_substack_replies_ascending(list(by_id.values()))


def get_oldest_substack_reply_cursor(replies: list[SubstackThreadReply]) -> str | None:
    for reply in sort_substack_replies_ascending(replies):
        cursor = to_substack_cursor_timestamp(reply.raw_time_text)
        if cursor:
            return cursor
    return None


def build_substack_room_hash(threads: list[SubstackRoomThreadPreview]) -> str:
    payload = "\n".join(
        "|".join(
            [
                thread.thread_url,
                thread.author_name or "",
                str(thread.displayed_reply_count or ""),
                thread.preview_text,
            ]
        )
        for thread in threads[:20]
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def build_thread_content_hash(detail: SubstackThreadDetail) -> str:
    transcript = build_substack_thread_transcript(detail)
    total_replies = detail.reply_count if detail.reply_count is not None else detail.parsed_reply_count
    return sha256(f"{transcript}\nreply_count={total_replies}".encode("utf-8")).hexdigest()


def serialize_dataclass(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    if isinstance(value, list):
        return [serialize_dataclass(item) for item in value]
    if isinstance(value, dict):
        return {key: serialize_dataclass(item) for key, item in value.items()}
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(serialize_dataclass(value), sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def evaluate_substack_room_auth(html_value: str, room_url: str, final_url: str | None = None) -> SubstackAuthEvaluation:
    parsed_room = parse_substack_chat_room(html_value, room_url)
    normalized_room_url = normalize_substack_chat_room_url(room_url)
    normalized_final_room_url = normalize_substack_chat_room_url(final_url) if final_url else None
    normalized_final_url = (final_url or "").lower()
    if "/signin" in normalized_final_url or "/login" in normalized_final_url or "/sign-in" in normalized_final_url:
        return SubstackAuthEvaluation(
            success=False,
            status="expired",
            message="Substack redirected to sign in. Cookie may be expired.",
            parsed_room=parsed_room,
        )

    title = extract_title_text(html_value)
    og_title = extract_meta_content(html_value, "property", "og:title")
    plain = strip_html_to_plain(html_value)[:4000]
    looks_like_javascript_shell = bool(
        re.search(r"this site requires javascript to run correctly", plain, flags=re.IGNORECASE)
        or re.search(r"enable javascript", plain, flags=re.IGNORECASE)
        or re.search(r"unblock scripts", plain, flags=re.IGNORECASE)
    )
    looks_like_chat_title = any(value and re.search(r"\bchat\b", value, flags=re.IGNORECASE) for value in (title, og_title))
    stayed_on_requested_room = bool(normalized_room_url and normalized_final_room_url and normalized_room_url == normalized_final_room_url)

    if parsed_room.has_authenticated_markers:
        return SubstackAuthEvaluation(success=True, status="valid", message="Cookie validated", parsed_room=parsed_room)
    if stayed_on_requested_room and looks_like_javascript_shell and looks_like_chat_title:
        return SubstackAuthEvaluation(
            success=True,
            status="valid",
            message="Cookie validated (Substack returned a JavaScript shell for this chat room).",
            parsed_room=parsed_room,
        )
    if re.search(r"sign in|log in|continue with email|continue with google|subscribe to continue", plain, flags=re.IGNORECASE):
        return SubstackAuthEvaluation(
            success=False,
            status="expired",
            message="Substack requires a valid logged-in session to access this chat room.",
            parsed_room=parsed_room,
        )
    return SubstackAuthEvaluation(
        success=False,
        status="parse_failed",
        message="Substack room HTML did not match an authenticated chat page.",
        parsed_room=parsed_room,
    )
