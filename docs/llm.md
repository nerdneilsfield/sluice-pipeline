# Summarize with an LLM

[examples/rss-llm-digest.yml](../examples/rss-llm-digest.yml) runs RSS → fetch → `llm_extract` → `llm_summary` → console. The top-level `llm` block configures one OpenAI-compatible `/chat/completions` endpoint; `tick` and `prefect_tick` build the client from it at startup.

## Configure the endpoint

```yaml
llm:
  base_url: https://api.example.com/v1   # http(s) with a host; no userinfo, query or fragment
  api_key: env:LLM_API_KEY               # optional; exact env:NAME only, literal keys are rejected
  timeout_seconds: 60                    # optional, > 0 and <= 3600
  max_attempts: 3                        # optional, integer >= 1
```

| Field | Required | Default | Constraint |
| --- | --- | --- | --- |
| `base_url` | yes | none | `http` or `https` URL with a host; no userinfo, query, or fragment. |
| `api_key` | no | none | Exact `env:NAME` reference only. Literal keys are rejected. |
| `timeout_seconds` | no | 60 | Finite number greater than 0 and at most 3600. |
| `max_attempts` | no | 3 | Integer, 1 or greater. |

The App resolves the `env:NAME` reference when it starts.

## Run the example

Edit `base_url` and the Stage `model` values in the YAML for your provider, then export the key and run `tick` from the repository root:

```sh
export LLM_API_KEY=...
PYTHONPATH="$PWD" .venv/bin/sluice tick examples/rss-llm-digest.yml \
  --at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

`PYTHONPATH` makes the example's console Sink importable. Internet access is required. Inspect `executions` and `deliveries` in `examples/rss-llm-digest.db` as in the [webpage example](../README.md#run-the-webpage-example); Stage and Delivery states should be `succeeded`.

## Startup rules

- An unset `LLM_API_KEY` fails startup, even if no Stage uses an LLM Task. An empty value is accepted and sends no `Authorization` header.
- A definition that uses `llm_extract` or `llm_summary` without an `llm` block fails at startup.
- Retained queued, running or waiting LLM work also needs a client. Terminal history opens without one, but `rebuild` of it fails.

## Cache identity

The endpoint is not part of cache identity. Changing `base_url` or the key reuses existing results for the same model, prompt and output shape. Use `rebuild` or bump the Stage `cache_version` to recompute. See [Operate Sluice](operations.md#rebuild-a-stage-execution) for `rebuild`.

## What the model sees

The model sees only the projected Item payload: Markdown text for `llm_extract`, and a JSON array of the extract results for `llm_summary`. Title, link and `metadata.web` are not sent, so templates after `llm_extract` cannot use `{title}` or `{link}`.

The client sends no `response_format`; the prompts ask for bare JSON. Model content that is not valid JSON counts as invalid structured output, so `invalid: drop` drops it like any other schema mismatch. A non-JSON HTTP response body is an endpoint failure and is never dropped.

## Errors

Error messages never contain the vendor response body. Non-retryable HTTP errors keep the status code, and retry exhaustion reports the attempt count.

The client does not follow redirects, so the API key is only ever sent to the configured `base_url`. Any 3xx response fails immediately as `HTTP <code>` without retry; point `base_url` at the final endpoint.

## Deliver to another channel

To send the digest to Telegram, email or Feishu instead of the console, replace the `console` node with the Sink from [notification setup](notifications.md).
