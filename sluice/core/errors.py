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
    def __init__(self, url: str, attempts: list[str], details: list[str] | None = None):
        message = f"all fetchers failed for {url}: tried {attempts}"
        if details:
            message = f"{message}; details: {details}"
        super().__init__(message)
        self.url = url
        self.attempts = attempts
        self.details = details or []


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
