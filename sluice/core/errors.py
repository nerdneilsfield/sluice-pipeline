class SluiceError(Exception):
    pass


class ConfigError(SluiceError):
    pass


class StageError(SluiceError):
    pass


class SinkError(SluiceError):
    pass


class BudgetExceeded(SluiceError):
    pass


class FetchError(SluiceError):
    pass


class AllFetchersFailed(FetchError):
    def __init__(self, url: str, attempts: list[str]):
        super().__init__(f"all fetchers failed for {url}: tried {attempts}")
        self.url = url
        self.attempts = attempts


class LLMError(SluiceError):
    pass


class RateLimitError(LLMError):
    pass


class QuotaExhausted(LLMError):
    pass


class AllProvidersExhausted(LLMError):
    def __init__(self, model_chain: list[str]):
        super().__init__(f"all tiers exhausted: {model_chain}")
        self.model_chain = model_chain
