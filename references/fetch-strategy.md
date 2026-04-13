# Fetch Strategy

## Fixed Order

Use this fetch order for every room:

1. Room posts API for thread discovery
2. Comments API for reply hydration and backward pagination
3. Thread HTML fallback if the API path fails or remains incomplete for non-auth reasons

## Room Discovery

Primary endpoint:

```text
https://substack.com/api/v1/community/publications/<chat_id>/posts
```

This is the canonical discovery surface for visible threads in v1. The skill does not try to discover all subscribed chats.

## Reply Hydration

Primary endpoint:

```text
https://substack.com/api/v1/community/posts/<thread_id>/comments?order=asc&initial=true
```

If parsed replies are below the reported reply count, page backward with:

```text
https://substack.com/api/v1/community/posts/<thread_id>/comments?order=desc&before=<cursor>
```

Pagination rules:

- stop when reply count is satisfied
- stop when no older replies are returned
- stop when the cursor no longer advances
- cap page count with `max_comment_pages`

## HTML Fallback

Fetch thread HTML only after a non-auth API failure or when the API result is still partial. HTML is used to recover:

- root author and handle
- root body
- visible replies that were missing from API parsing
- published metadata exposed in page tags

The HTML fallback is supplemental. It does not promise full historical recovery.

## History Policy

The skill is recent-visible plus incremental:

- first sync archives the currently visible room threads
- reruns detect visible changes and rewrite only affected thread files
- the skill does not claim to reconstruct all historical chat content
