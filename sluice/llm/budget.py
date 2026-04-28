from dataclasses import dataclass


@dataclass
class CallCost:
    total_usd: float
    prompt_tokens: int
    completion_tokens: int


class RunBudget:
    def __init__(self, max_calls: int, max_usd: float):
        self.max_calls = max_calls
        self.max_usd = max_usd
        self.calls = 0
        self.spent_usd = 0.0

    def record(self, cost: CallCost) -> None:
        self.calls += 1
        self.spent_usd += cost.total_usd

    def project(self, projected_calls: int, projected_usd: float) -> bool:
        """Return True if the projected spend fits within budget."""
        if self.max_calls > 0 and self.calls + projected_calls > self.max_calls:
            return False
        if self.max_usd > 0 and self.spent_usd + projected_usd > self.max_usd:
            return False
        return True


def compute_cost(ep, prompt_tokens: int, completion_tokens: int) -> CallCost:
    m = ep.model_entry
    in_cost = (prompt_tokens / 1000.0) * m.input_price_per_1k
    out_cost = (completion_tokens / 1000.0) * m.output_price_per_1k
    return CallCost(
        total_usd=in_cost + out_cost,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
    )
