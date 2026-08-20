"""Immutable contracts used by Sluice's web content pipeline.

The package intentionally keeps the contract types in a small dependency-free
module.  Network transports, extractors, and configuration expansion are
implemented by later layers and depend on these types rather than the other
way around.
"""

from .contracts import (
    DocumentArtifact,
    ExtractorBinding,
    FetchChainExhausted,
    FetchRequest,
    NO_MATCH,
    PrivateWebInvocation,
    ProviderBinding,
    QualityPolicy,
    WebAttemptCancelled,
    WebAttemptFailure,
    WebBinding,
    WebExecutionProbe,
    WebStageDeadlineExpired,
    WEB_EXECUTION_PROBE,
    current_web_execution_probe,
    get_web_execution_probe,
    reset_web_execution_probe,
    set_web_execution_probe,
)
from .client import WebContentClient
from .providers import (
    CORE_PROVIDER_BUILDERS,
    PROVIDER_BUILDERS,
    CurlCffiProvider,
    HTTPXProvider,
    StartupHTTPProvider,
    TrafilaturaProvider,
    build_curl_cffi_provider,
    build_extractor,
    build_httpx_provider,
    build_provider,
    build_startup_http_provider,
    build_trafilatura_provider,
)
from .registry import ProviderLimiter, WebRuntimeCatalog

__all__ = [
    "DocumentArtifact",
    "ExtractorBinding",
    "FetchChainExhausted",
    "FetchRequest",
    "NO_MATCH",
    "PrivateWebInvocation",
    "ProviderBinding",
    "QualityPolicy",
    "WebAttemptCancelled",
    "WebAttemptFailure",
    "WebBinding",
    "WebExecutionProbe",
    "WebStageDeadlineExpired",
    "WEB_EXECUTION_PROBE",
    "current_web_execution_probe",
    "get_web_execution_probe",
    "reset_web_execution_probe",
    "set_web_execution_probe",
    "WebContentClient",
    "CORE_PROVIDER_BUILDERS",
    "PROVIDER_BUILDERS",
    "CurlCffiProvider",
    "HTTPXProvider",
    "StartupHTTPProvider",
    "TrafilaturaProvider",
    "build_curl_cffi_provider",
    "build_extractor",
    "build_httpx_provider",
    "build_provider",
    "build_startup_http_provider",
    "build_trafilatura_provider",
    "ProviderLimiter",
    "WebRuntimeCatalog",
]
