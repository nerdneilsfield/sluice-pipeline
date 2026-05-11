# Plan 4b Prefect 3 Concurrency Findings

Checked against local Prefect `3.6.28` because Context7 resolution failed with a fetch error.

## Available Strategies

`prefect.client.schemas.objects.ConcurrencyLimitStrategy` exposes:

- `ENQUEUE`
- `CANCEL_NEW`

For the process work-pool path used by Sluice deployments, the strategy is carried by the
deployment's `concurrency_limit` configuration rather than by a separate work-pool setting.

## API Surface

`Flow.to_deployment(...)` accepts `concurrency_limit`. In Prefect 3.6.28 this parameter can be
an integer or a `prefect.client.schemas.objects.ConcurrencyLimitConfig`.

`prefect.deploy(...)` still receives the prepared deployment objects and does not take a separate
collision strategy kwarg.

## SDK Call

Use:

```python
from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy

flow.to_deployment(
    ...,
    concurrency_limit=ConcurrencyLimitConfig(
        limit=1,
        collision_strategy=ConcurrencyLimitStrategy.ENQUEUE,
    ),
)
```

Sluice maps `queue` and `enqueue` to `ENQUEUE`, and maps `cancel_new` to `CANCEL_NEW`.
