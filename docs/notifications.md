# Send RSS notifications

The built-in RSS Source supports RSS 2.0, Atom, and RSS 1.0. It fetches the feed through the safe Web transport, rejects unsafe XML, and projects entry markup to text or Markdown before storing Items. RSS entries expose `title`, `link`, and `content` when those fields are available.

[examples/rss-notifications.yml](../examples/rss-notifications.yml) connects an RSS Source to Telegram, SMTP email, and a Feishu custom bot. All three Sinks have default senders and can run from the CLI without Python client injection. The render Stage produces plain text using `{title}\n{link}`; the renderer's historical `render_telegram` name does not restrict its string output to Telegram.

## Configure destinations

This example sends real messages. A first collection can publish every entry currently in the feed, producing one message per entry per destination. Use a small feed for the first run and remove Delivery nodes for channels you do not want to contact.

Keep secrets in environment variables, not literal YAML: authored configuration is retained in SQLite. The senders resolve `env:NAME` when sending and do not write resolved credentials or vendor error bodies to execution errors. Environment variables must be available to the process running `tick` or the UI.

| Channel | Configuration | Behavior |
| --- | --- | --- |
| Telegram | `bot_token`, `chat_id` | Calls the fixed HTTPS Bot API `sendMessage` endpoint; sends plain text without `parse_mode`; returns `message_id`. Quote a numeric chat ID in YAML or use an environment variable. |
| Email | `smtp_host`, `from`, `to`; optional `username`, `password`, `subject`, `smtp_port`, `security` | Sends UTF-8 plain text over SMTP. `security: starttls` defaults to port 587; `security: ssl` defaults to port 465. Certificate verification is enabled; plaintext SMTP is not supported. Authentication is used when `username` is present and then requires `password`. |
| Feishu | `webhook`; optional `secret` | Sends a custom bot text message to `https://open.feishu.cn/open-apis/bot/v2/hook/...`. If signing is enabled on the bot, set `secret`; otherwise omit it. A successful response requires `code: 0`. This is a Feishu custom bot, not an application bot or Lark endpoint. |

All channels accept `timeout_seconds` (default 30, greater than 0 and at most 300). Telegram and email also accept legacy `target` in place of `chat_id` and `to`, respectively. Email defaults to subject `Sluice notification`.

Supply the environment variables used in the example through your shell or secret manager:

```text
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
SMTP_HOST
SMTP_USERNAME
SMTP_PASSWORD
MAIL_FROM
MAIL_TO
FEISHU_WEBHOOK
FEISHU_SECRET
```

The Telegram bot must have permission to send to the target chat. Feishu keyword and IP restrictions still apply, and signed requests require an accurate system clock. Configure the SMTP host and credentials for your mail service.

## Run and inspect

From the repository root, after the source installation steps in the [README](../README.md#from-source):

```sh
.venv/bin/sluice tick examples/rss-notifications.yml \
  --at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

The default senders need only core dependencies; no channel SDK is required. Inspect the state rather than relying on process exit alone:

```sh
.venv/bin/python - <<'PY'
import sqlite3
with sqlite3.connect('examples/rss-notifications.db') as db:
    for row in db.execute('SELECT node_name, state, error FROM deliveries'):
        print(row)
PY
```

Each destination has its own durable Delivery state and retry policy. A failure in Feishu does not turn an already successful email Delivery into a failure. Missing environment values, HTTP errors, vendor rejection, or SMTP errors mark that destination's attempt failed.

## Limits and retry behavior

Telegram messages exceeding 4096 UTF-16 units and Feishu request bodies exceeding 20000 UTF-8 bytes are rejected before sending. Messages are not silently truncated or split: splitting introduces partial delivery and duplicate-retry semantics. Use the short title/link template or summarize long content before delivery. Email sends plain text, not attachments or HTML.

Delivery retries are not exactly-once external delivery. If a remote server accepts a message but the response is lost, retrying can send it again. SMTP uses a stable `Message-ID` derived from the Delivery key, but mail servers are not required to deduplicate it. Telegram and Feishu custom webhook APIs do not use the Sluice Delivery key as an idempotency key. Partial SMTP recipient acceptance can also produce duplicates on retry; use one recipient per Delivery for independent outcomes.

Protocol tests use HTTP and SMTP fakes, including signed Feishu bodies, environment-secret handling, vendor failures, and three sibling Deliveries without injected clients. Live delivery still needs validation with your accounts; no credentials were available during implementation.

References: [Telegram Bot API](https://core.telegram.org/bots/api#sendmessage), [Feishu custom bots](https://open.feishu.cn/document/client-docs/bot-v3/add-custom-bot?lang=zh-CN).

For the generic webhook and file Sinks, and for non-RSS Sources, see [Sources and Sinks reference](sources-and-sinks.md).

To deliver an LLM-generated digest instead of raw feed text, see [Summarize with an LLM](llm.md).
