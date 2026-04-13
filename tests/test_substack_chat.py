#!/usr/bin/env python3
"""Tests for the standalone substack-chat-archive skill."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path


SKILL_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = SKILL_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from substack_chat_archive import main as cli_main  # noqa: E402
from substack_chat_core import (  # noqa: E402
    HttpResponse,
    resolve_cookie,
    resolve_room_targets,
    sync_rooms,
    validate_rooms,
)
from substack_chat_parsers import (  # noqa: E402
    build_substack_room_posts_api_url,
    build_substack_thread_comments_api_url,
    build_substack_thread_transcript,
    evaluate_substack_room_auth,
    normalize_substack_cookie,
    parse_substack_chat_room,
    parse_substack_chat_thread_detail,
    parse_substack_room_posts_api_payload,
    parse_substack_thread_comments_api_payload,
)


ROOM_HTML = """
<html>
  <body>
    <div>
      <a href="https://substack.com/@ming371794">ming</a>
      <a href="https://substack.com/chat/1899793/post/ede81b81-6a77-492d-a0f7-421c02d6a0c7">
        <div class="body-SxXE9l"><span>supplychain in Asia doing hella fine today!</span></div>
      </a>
      <span>3 replies</span>
    </div>
    <div>
      <a href="/@davve">Davve</a>
      <a href="/chat/1899793/post/4bccc325-7375-4136-a408-49dbd7f7ea48">
        <div class="body-SxXE9l"><span>WTI up ~75% YTD and stocks basically flat...</span></div>
      </a>
      <span>26 replies</span>
    </div>
    <button>Start a new thread</button>
    <div>Drop file here to upload</div>
  </body>
</html>
""".strip()

THREAD_HTML = """
<html>
  <head>
    <meta property="og:published_time" content="2026-03-17T17:25:36.652Z" />
    <meta name="twitter:data2" content="26" />
  </head>
  <body>
    <div>Today 09:28</div>
    <a href="https://substack.com/@davve">Davve</a>
    <a href="https://substack.com/chat/1899793/post/4bccc325-7375-4136-a408-49dbd7f7ea48">Permalink</a>
    <div class="body-obaIpu">
      <span>WTI up ~75% YTD and stocks basically flat. Feels like market pricing in that this all magically gets resolved.</span>
    </div>

    <div>Today 09:28</div>
    <a href="https://substack.com/@ming371794">ming</a>
    <div id="comment-8e93d359-18fd-4c61-9fc5-c47cdaf6e4cd">
      <div class="body-obaIpu"><span>100% they have notorious history for setting lofty guides and missing</span></div>
    </div>

    <div>Today 09:29</div>
    <a href="/@terminaljunkie">Terminal Junkie</a>
    <div id="comment-be4dde92-ea1e-4579-879c-13d1e86b3f4d">
      <div class="body-obaIpu"><span>Why not 20x?</span></div>
    </div>
  </body>
</html>
""".strip()

ROOM_API_PAYLOAD = {
    "threads": [
        {"communityPost": {"id": "11111111-1111-1111-1111-111111111111", "comment_count": 2}},
        {
            "communityPost": {
                "id": "ede81b81-6a77-492d-a0f7-421c02d6a0c7",
                "body": "supplychain in Asia doing hella fine today!",
                "comment_count": 3,
                "created_at": "2026-03-17T16:21:00.000Z",
                "updated_at": "2026-03-17T16:31:00.000Z",
                "max_comment_created_at": "2026-03-17T16:30:00.000Z",
                "user": {"name": "ming", "handle": "ming371794"},
            }
        },
        {
            "communityPost": {
                "id": "22222222-2222-2222-2222-222222222222",
                "raw_body": "raw body only thread",
                "comment_count": 1,
                "created_at": "2026-03-17T16:45:00.000Z",
                "user": {"name": "rawbodyuser", "handle": "rawbodyhandle"},
            }
        },
        {
            "communityPost": {
                "id": "4bccc325-7375-4136-a408-49dbd7f7ea48",
                "body": "WTI up ~75% YTD and stocks basically flat...",
                "comment_count": 26,
                "created_at": "2026-03-17T17:25:36.652Z",
                "updated_at": "2026-03-17T18:34:16.945Z",
                "max_comment_created_at": "2026-03-17T18:34:16.945Z",
                "user": {"name": "Davve", "handle": "davve"},
            }
        },
    ]
}

COMMENTS_API_PAYLOAD = {
    "post": {
        "communityPost": {
            "id": "49a2363b-b1a2-4efa-8ac8-86e14834511e",
            "body": "Any brief thoughts from the group on AMKR here?",
            "comment_count": 4,
            "user": {"name": "ADhar", "handle": "arundhar"},
        }
    },
    "replies": [
        {
            "comment": {
                "id": "5f009eaa-9514-4b90-818f-aa8ceefbbdf0",
                "body": "Trades like an AI stock without any AI exposure",
                "raw_body": "Trades like an AI stock without any AI exposure",
                "status": "published",
                "created_at": "2026-03-18T15:45:26.299Z",
            },
            "user": {"name": "BIk", "handle": "brianincognito"},
        },
        {
            "comment": {
                "id": "2c1a0b32-4c38-4fea-8347-bcd7aca4e943",
                "body": "Thanks. Because you think the packaging exposure is mostly mid nodes / trailing nodes?",
                "raw_body": "Thanks. Because you think the packaging exposure is mostly mid nodes / trailing nodes?",
                "status": "published",
                "created_at": "2026-03-18T15:48:11.372Z",
            },
            "user": {"name": "ADhar", "handle": "arundhar"},
        },
        {
            "comment": {
                "id": "b3cf6690-e8e8-4763-b830-dd880c134afa",
                "body": None,
                "raw_body": None,
                "status": "deleted",
                "created_at": "2026-03-18T15:48:41.976Z",
            },
            "user": {"name": "ADhar", "handle": "arundhar"},
        },
        {
            "comment": {
                "id": "e76e271a-ac17-4e79-9504-5ae10f4ca8af",
                "body": "I recall that majority of their revenue are packaging within the cyclical industries",
                "raw_body": "I recall that majority of their revenue are packaging within the cyclical industries",
                "status": "published",
                "created_at": "2026-03-18T15:55:45.380Z",
            },
            "user": {"name": "Nn", "handle": "deepdivex"},
        },
        {
            "comment": {
                "id": "media-only-comment",
                "body": None,
                "raw_body": "",
                "status": "published",
                "created_at": "2026-03-18T15:56:10.000Z",
                "mediaAttachments": [{"type": "image", "url": "https://example.com/comment-image.png"}],
            },
            "user": {"name": "ImageUser", "handle": "imageuser"},
        },
    ],
    "comments": [
        {
            "comment": {
                "id": "legacy-comment-shape",
                "message": "legacy message field",
                "status": "published",
                "created_at": "2026-03-18T15:59:00.000Z",
            },
            "commenter": {"name": "Legacy User", "username": "legacyuser"},
        },
        {
            "commentId": "be4dde92-ea1e-4579-879c-13d1e86b3f4d",
            "message": "Why not 20x?",
            "created_at": "2026-03-17T17:29:00.000Z",
            "commenter": {"name": "Terminal Junkie", "username": "terminaljunkie"},
        },
        {
            "id": 101,
            "body": "100% they have notorious history for setting lofty guides and missing",
            "created_at": "2026-03-17T17:28:00.000Z",
            "author": {"user": {"name": "ming", "handle": "ming371794"}},
        },
    ],
}


class FakeHttpClient:
    def __init__(self, routes):
        self.routes = routes

    def get(self, url, headers, timeout_seconds):
        response = self.routes.get(url)
        if response is None:
            raise AssertionError(f"Unexpected URL: {url}")
        return response


def json_response(url, payload, status=200):
    return HttpResponse(
        url=url,
        final_url=url,
        status_code=status,
        headers={"content-type": "application/json"},
        text=json.dumps(payload),
    )


def html_response(url, payload, status=200):
    return HttpResponse(
        url=url,
        final_url=url,
        status_code=status,
        headers={"content-type": "text/html"},
        text=payload,
    )


class ParserTests(unittest.TestCase):
    def test_parse_room_html(self):
        parsed = parse_substack_chat_room(ROOM_HTML, "https://substack.com/chat/1899793")
        self.assertEqual(parsed.chat_id, "1899793")
        self.assertTrue(parsed.has_authenticated_markers)
        self.assertEqual(len(parsed.threads), 2)
        self.assertEqual(
            parsed.threads[0].thread_url,
            "https://substack.com/chat/1899793/post/ede81b81-6a77-492d-a0f7-421c02d6a0c7",
        )
        self.assertEqual(parsed.threads[0].displayed_reply_count, 3)
        self.assertIn("WTI up ~75% YTD", parsed.threads[1].preview_text)

    def test_validate_room_auth(self):
        result = evaluate_substack_room_auth(
            ROOM_HTML,
            "https://substack.com/chat/1899793",
            "https://substack.com/chat/1899793",
        )
        self.assertTrue(result.success)
        self.assertEqual(result.status, "valid")

    def test_room_posts_api_helpers(self):
        self.assertEqual(
            build_substack_room_posts_api_url("https://substack.com/chat/1899793"),
            "https://substack.com/api/v1/community/publications/1899793/posts",
        )
        self.assertEqual(
            build_substack_thread_comments_api_url("https://substack.com/chat/1899793/post/4bccc325-7375-4136-a408-49dbd7f7ea48"),
            "https://substack.com/api/v1/community/posts/4bccc325-7375-4136-a408-49dbd7f7ea48/comments?order=asc&initial=true",
        )
        self.assertEqual(
            build_substack_thread_comments_api_url(
                "https://substack.com/chat/1899793/post/4bccc325-7375-4136-a408-49dbd7f7ea48",
                order="desc",
                initial=False,
                before="2026-03-18T22:09:34.166Z",
            ),
            "https://substack.com/api/v1/community/posts/4bccc325-7375-4136-a408-49dbd7f7ea48/comments?order=desc&before=2026-03-18T22%3A09%3A34.166Z",
        )
        parsed = parse_substack_room_posts_api_payload(ROOM_API_PAYLOAD, "https://substack.com/chat/1899793")
        self.assertEqual(parsed.chat_id, "1899793")
        self.assertEqual(len(parsed.threads), 3)
        self.assertEqual(parsed.threads[0].author_handle, "ming371794")
        self.assertEqual(parsed.threads[1].preview_text, "raw body only thread")
        self.assertEqual(parsed.threads[2].displayed_reply_count, 26)

    def test_cookie_normalization(self):
        normalized = normalize_substack_cookie(
            "Cookie: substack.sid=abc; cf_clearance=def; Path=/; Secure; HttpOnly; SameSite=Lax; xsrf-token=ghi"
        )
        self.assertEqual(normalized, "substack.sid=abc; cf_clearance=def; xsrf-token=ghi")
        self.assertEqual(normalize_substack_cookie("just-the-substack-sid-value"), "substack.sid=just-the-substack-sid-value")

    def test_parse_thread_html_and_transcript(self):
        parsed = parse_substack_chat_thread_detail(
            THREAD_HTML,
            "https://substack.com/chat/1899793/post/4bccc325-7375-4136-a408-49dbd7f7ea48",
        )
        self.assertEqual(parsed.room_id, "1899793")
        self.assertEqual(parsed.thread_id, "4bccc325-7375-4136-a408-49dbd7f7ea48")
        self.assertEqual(parsed.published_at, "2026-03-17T17:25:36.652Z")
        self.assertEqual(parsed.reply_count, 26)
        self.assertEqual(parsed.root_author, "Davve")
        self.assertEqual(parsed.root_handle, "davve")
        self.assertIn("WTI up ~75% YTD", parsed.root_body)
        self.assertEqual(parsed.parsed_reply_count, 2)
        self.assertTrue(parsed.partial_transcript)
        self.assertEqual(parsed.replies[0].author_handle, "ming371794")
        transcript = build_substack_thread_transcript(parsed)
        self.assertIn("## Root Post", transcript)
        self.assertIn("Partial transcript: yes", transcript)
        self.assertIn("# Replies (total 26, parsed 2)", transcript)

    def test_parse_comments_payload(self):
        parsed = parse_substack_thread_comments_api_payload(
            COMMENTS_API_PAYLOAD,
            "https://substack.com/chat/1899793/post/49a2363b-b1a2-4efa-8ac8-86e14834511e",
        )
        self.assertEqual(parsed.thread_id, "49a2363b-b1a2-4efa-8ac8-86e14834511e")
        self.assertEqual(parsed.root_author, "ADhar")
        self.assertEqual(parsed.root_handle, "arundhar")
        self.assertEqual(parsed.root_body, "Any brief thoughts from the group on AMKR here?")
        self.assertEqual(parsed.reply_count, 4)
        self.assertEqual(parsed.parsed_reply_count, 7)
        self.assertFalse(parsed.has_more)
        self.assertTrue(any(reply.body_text == "Why not 20x?" for reply in parsed.replies))
        self.assertTrue(any(reply.body_text == "(Image attachment)" for reply in parsed.replies))
        self.assertFalse(any(reply.comment_id == "b3cf6690-e8e8-4763-b830-dd880c134afa" for reply in parsed.replies))


class ArchiveTests(unittest.TestCase):
    def build_client(self, *, comment_body="Trades like an AI stock without any AI exposure"):
        room_url = "https://substack.com/chat/1899793"
        first_thread_url = "https://substack.com/chat/1899793/post/ede81b81-6a77-492d-a0f7-421c02d6a0c7"
        thread_url = "https://substack.com/chat/1899793/post/49a2363b-b1a2-4efa-8ac8-86e14834511e"
        room_api_url = "https://substack.com/api/v1/community/publications/1899793/posts"
        first_comments_api_url = (
            "https://substack.com/api/v1/community/posts/ede81b81-6a77-492d-a0f7-421c02d6a0c7/comments?order=asc&initial=true"
        )
        comments_api_url = (
            "https://substack.com/api/v1/community/posts/49a2363b-b1a2-4efa-8ac8-86e14834511e/comments?order=asc&initial=true"
        )
        paginated_comments_api_url = (
            "https://substack.com/api/v1/community/posts/49a2363b-b1a2-4efa-8ac8-86e14834511e/comments?order=desc&before=2026-03-17T17%3A28%3A00Z"
        )
        payload = json.loads(json.dumps(COMMENTS_API_PAYLOAD))
        payload["replies"][0]["comment"]["body"] = comment_body
        payload["replies"][0]["comment"]["raw_body"] = comment_body
        payload["post"]["communityPost"]["comment_count"] = 9
        paginated_payload = {
            "post": payload["post"],
            "replies": [],
            "comments": [],
            "more": False,
            "moreAfter": False,
            "moreBefore": False,
        }
        first_payload = {
            "post": {
                "communityPost": {
                    "id": "ede81b81-6a77-492d-a0f7-421c02d6a0c7",
                    "body": "supplychain in Asia doing hella fine today!",
                    "comment_count": 0,
                    "user": {"name": "ming", "handle": "ming371794"},
                }
            },
            "replies": [],
            "comments": [],
            "more": False,
            "moreAfter": False,
            "moreBefore": False,
        }
        return FakeHttpClient(
            {
                room_api_url: json_response(room_api_url, {"threads": [ROOM_API_PAYLOAD["threads"][1], {"communityPost": {"id": "49a2363b-b1a2-4efa-8ac8-86e14834511e", "body": "Any brief thoughts from the group on AMKR here?", "comment_count": 9, "created_at": "2026-03-18T15:40:00.000Z", "user": {"name": "ADhar", "handle": "arundhar"}}}]}),
                first_comments_api_url: json_response(first_comments_api_url, first_payload),
                comments_api_url: json_response(comments_api_url, payload),
                paginated_comments_api_url: json_response(paginated_comments_api_url, paginated_payload),
                room_url: html_response(room_url, ROOM_HTML),
                first_thread_url: html_response(first_thread_url, ROOM_HTML),
                thread_url: html_response(thread_url, THREAD_HTML),
            }
        )

    def test_first_sync_creates_archive(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            results = sync_rooms(
                resolve_room_targets(["https://substack.com/chat/1899793"]),
                archive_dir=temp_dir,
                cookie="substack.sid=test",
                http_client=self.build_client(),
            )
            self.assertEqual(results[0].status, "ok")
            room_root = Path(temp_dir) / "rooms" / "1899793"
            self.assertTrue((Path(temp_dir) / "manifest.json").exists())
            self.assertTrue((room_root / "room.json").exists())
            self.assertTrue((room_root / "state.json").exists())
            thread_files = sorted((room_root / "threads").glob("*.json"))
            self.assertEqual(len(thread_files), 2)

    def test_rerun_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            targets = resolve_room_targets(["https://substack.com/chat/1899793"])
            sync_rooms(targets, archive_dir=temp_dir, cookie="substack.sid=test", http_client=self.build_client())
            second = sync_rooms(targets, archive_dir=temp_dir, cookie="substack.sid=test", http_client=self.build_client())
            self.assertEqual(second[0].created_threads, 0)
            self.assertEqual(second[0].updated_threads, 0)
            thread_files = sorted((Path(temp_dir) / "rooms" / "1899793" / "threads").glob("*.json"))
            self.assertEqual(len(thread_files), 2)

    def test_changed_thread_rewrites_only_affected_thread(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            targets = resolve_room_targets(["https://substack.com/chat/1899793"])
            sync_rooms(targets, archive_dir=temp_dir, cookie="substack.sid=test", http_client=self.build_client())
            second = sync_rooms(
                targets,
                archive_dir=temp_dir,
                cookie="substack.sid=test",
                http_client=self.build_client(comment_body="Updated body from second sync"),
            )
            self.assertEqual(second[0].updated_threads, 1)

    def test_partial_transcript_flag_persists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            targets = resolve_room_targets(["https://substack.com/chat/1899793"])
            sync_rooms(targets, archive_dir=temp_dir, cookie="substack.sid=test", http_client=self.build_client())
            thread_path = Path(temp_dir) / "rooms" / "1899793" / "threads" / "49a2363b-b1a2-4efa-8ac8-86e14834511e.json"
            payload = json.loads(thread_path.read_text(encoding="utf-8"))
            self.assertTrue(payload["partial_transcript"])


class CliTests(unittest.TestCase):
    def test_missing_cookie_fails_clearly(self):
        exit_code = cli_main(["validate", "--room", "https://substack.com/chat/1899793"], env={})
        self.assertEqual(exit_code, 1)

    def test_invalid_room_url_fails_clearly(self):
        exit_code = cli_main(["validate", "--room", "https://example.com/not-substack"], env={})
        self.assertEqual(exit_code, 2)

    def test_repeated_room_and_config_normalize_to_same_targets(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text(
                json.dumps({"rooms": ["https://substack.com/chat/1899793", "https://substack.com/chat/1899793/"]}),
                encoding="utf-8",
            )
            cli_targets = resolve_room_targets(["https://substack.com/chat/1899793", "https://substack.com/chat/1899793/"])
            config_targets = resolve_room_targets(json.loads(config_path.read_text(encoding="utf-8"))["rooms"])
            self.assertEqual([target.room_url for target in cli_targets], [target.room_url for target in config_targets])

    def test_cookie_precedence_prefers_environment(self):
        with tempfile.NamedTemporaryFile("w+", delete=False) as cookie_file:
            cookie_file.write("substack.sid=file-cookie")
            cookie_file.flush()
            resolved = resolve_cookie(
                cookie="substack.sid=flag-cookie",
                cookie_file=cookie_file.name,
                env={"SUBSTACK_COOKIE": "substack.sid=env-cookie"},
            )
        os.unlink(cookie_file.name)
        self.assertEqual(resolved, "substack.sid=env-cookie")


@unittest.skipUnless(
    os.getenv("SUBSTACK_CHAT_LIVE_SMOKE") and os.getenv("SUBSTACK_COOKIE") and os.getenv("SUBSTACK_ROOM_URL"),
    "Live smoke test disabled.",
)
class LiveSmokeTests(unittest.TestCase):
    def test_live_smoke_sync(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            exit_code = cli_main(
                [
                    "sync",
                    "--room",
                    os.environ["SUBSTACK_ROOM_URL"],
                    "--archive-dir",
                    temp_dir,
                ],
                env=dict(os.environ),
            )
            self.assertEqual(exit_code, 0)


if __name__ == "__main__":
    unittest.main()
