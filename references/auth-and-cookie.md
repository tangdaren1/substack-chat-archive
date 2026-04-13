# Auth And Cookie

## Cookie Precedence

Use this order exactly:

1. `SUBSTACK_COOKIE`
2. `--cookie-file`
3. `--cookie`

## Cookie Normalization

Accepted inputs:

- full `Cookie:` header
- raw cookie string with multiple pairs
- bare `substack.sid` value

Normalization rules:

- strip the leading `Cookie:` label if present
- convert a bare cookie value into `substack.sid=<value>`
- split multiline input into `;`
- strip cookie attributes such as `Path`, `Domain`, `Expires`, `HttpOnly`, `Secure`, `SameSite`, `Priority`, and `Partitioned`
- preserve actual cookie name/value pairs
- never persist the raw cookie into archive files, state files, or logs

## Validation Statuses

- `valid`: cookie is accepted and the room is accessible
- `expired`: login is missing or session expired
- `invalid_access`: cookie is valid but the user does not have access to the room or thread
- `auth_required`: no cookie was provided
- `parse_failed`: Substack responded, but the payload or HTML could not be parsed into the expected room or thread shape
- `http_failed`: request failed at the transport or HTTP layer

## Secret Handling

- keep cookies in environment variables or local files outside version control
- never write cookies into JSON, markdown, manifest, or raw snapshots
- avoid passing cookies through shell history when a file or environment variable is available
