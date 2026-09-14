"""Startup-bound sequential web provider/extractor coordinator."""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from collections.abc import Iterable, Mapping
from dataclasses import replace

from ..errors import ExecutionError, WebSafetyError
from .contracts import (
    NO_MATCH,
    DocumentArtifact,
    ExtractorBinding,
    FetchChainExhausted,
    FetchRequest,
    PrivateWebInvocation,
    ProviderBinding,
    WebAttemptCancelled,
    WebAttemptFailure,
    WebStageDeadlineExpired,
    current_web_execution_probe,
)
from .extractors import build_extractor, extract_identity
from .feed import validate_raw_feed
from .quality import QualityEvaluation, QualityMetrics, evaluate_quality
from .sanitize import sanitize_web_error
from .select import select_list_records

_MIME_TYPE = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+/[!#$%&'*+.^_`|~0-9A-Za-z-]+")


def _json_value(value: object) -> object:
    """Thaw immutable JSON views without falling back to representation text."""

    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise TypeError("value is not JSON-compatible")


class WebContentClient:
    """Resolve one immutable request by trying its chains in declared order.

    The client owns no definition-specific state. Every call receives the
    expanded provider and extractor bindings, so retained Stage/Source
    snapshots cannot accidentally reuse another definition's chain.
    """

    def __init__(self, catalog: object | None = None) -> None:
        self.catalog = catalog
        self._closed = False

    @staticmethod
    def _name(binding: object, fallback: str = "provider") -> str:
        value = getattr(binding, "name", None)
        if isinstance(value, str) and value:
            return value
        if isinstance(binding, str) and binding:
            return binding
        return fallback

    @staticmethod
    def _type(binding: object, fallback: str = "provider") -> str:
        for attribute in ("type", "provider_type", "extractor_type"):
            value = getattr(binding, attribute, None)
            if isinstance(value, str) and value:
                return value
        return fallback

    @staticmethod
    def _capabilities(binding: object) -> frozenset[str]:
        value = getattr(binding, "capabilities", ())
        try:
            return frozenset(str(item) for item in value)
        except TypeError:
            return frozenset()

    @staticmethod
    def _elapsed_ms(started: float) -> int:
        return max(0, int((time.monotonic() - started) * 1000))

    @staticmethod
    def _safe_failure(
        binding: object,
        ordinal: int,
        reason: str,
        *,
        started: float,
        artifact: DocumentArtifact | None = None,
        status_code: int | None = None,
        status_kind: str | None = None,
        detail_code: str | None = None,
    ) -> WebAttemptFailure:
        stable = {
            "bad_url",
            "ssrf_blocked",
            "timeout",
            "network",
            "rate_limited",
            "provider_busy",
            "http_status",
            "bad_content_type",
            "body_too_large",
            "unsafe_xml",
            "challenge",
            "empty_shell",
            "empty",
            "too_short",
            "extract_failed",
            "missing_capability",
            "provider_error",
        }
        safe_reason = reason if reason in stable else "provider_error"
        if status_code is None and artifact is not None:
            status_code = artifact.status_code
        if status_kind not in {None, "service", "target"}:
            status_kind = None
        if status_kind is None and status_code is not None and artifact is not None:
            status_kind = "target"
        return WebAttemptFailure(
            provider_instance=WebContentClient._name(binding),
            provider_type=WebContentClient._type(binding),
            attempt_ordinal=ordinal,
            reason=safe_reason,
            status_code=status_code,
            status_kind=status_kind,
            elapsed_ms=WebContentClient._elapsed_ms(started),
            detail_code=detail_code,
        )

    @staticmethod
    def _check_probe() -> None:
        probe = current_web_execution_probe()
        if probe is not None:
            probe.check()

    @staticmethod
    def _check_deadline(request: FetchRequest) -> None:
        if (
            request.stage_deadline is not None
            and time.monotonic() >= request.stage_deadline
        ):
            raise WebStageDeadlineExpired()

    @staticmethod
    def _provider_timeout(binding: object) -> float | None:
        value = getattr(binding, "timeout_seconds", None)
        if value is None:
            timeout = getattr(binding, "timeout", None)
            total_seconds = getattr(timeout, "total_seconds", None)
            if callable(total_seconds):
                value = total_seconds()
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
        ):
            return None
        return float(value)

    @classmethod
    def _provider_deadlines(
        cls,
        binding: object,
        request: FetchRequest,
        provider_attempt_started: float,
    ) -> tuple[float | None, float | None]:
        timeout = cls._provider_timeout(binding)
        provider_deadline = (
            None if timeout is None else provider_attempt_started + timeout
        )
        stage_deadline = request.stage_deadline
        if provider_deadline is None:
            return None, stage_deadline
        if stage_deadline is None:
            return provider_deadline, provider_deadline
        return provider_deadline, min(stage_deadline, provider_deadline)

    @staticmethod
    def _check_provider_deadline(
        request: FetchRequest,
        provider_deadline: float | None,
    ) -> None:
        now = time.monotonic()
        if request.stage_deadline is not None and now >= request.stage_deadline:
            raise WebStageDeadlineExpired()
        if provider_deadline is not None and now >= provider_deadline:
            raise WebSafetyError("timeout")

    def _provider(self, binding: object) -> object:
        catalog = self.catalog
        if catalog is not None:
            resolver = getattr(catalog, "provider", None)
            if callable(resolver):
                return resolver(binding)
        fetch = getattr(binding, "fetch", None)
        if callable(fetch):
            return binding
        raise ExecutionError("web provider catalog is unavailable")

    def _limiter(self, binding: object) -> object | None:
        catalog = self.catalog
        if catalog is None:
            return None
        resolver = getattr(catalog, "limiter", None)
        if callable(resolver):
            return resolver(binding)
        limiters = getattr(catalog, "limiters", None)
        if isinstance(limiters, Mapping):
            try:
                return limiters.get(self._name(binding))
            except Exception:
                return None
        return None

    @staticmethod
    def _in_cooldown(limiter: object | None) -> bool:
        if limiter is None:
            return False
        checker = getattr(limiter, "in_cooldown", None)
        if callable(checker):
            try:
                return bool(checker())
            except Exception:
                return False
        until = getattr(limiter, "cooldown_until", None)
        try:
            return (
                not isinstance(until, bool)
                and isinstance(until, (int, float))
                and math.isfinite(until)
                and time.monotonic() < until
            )
        except Exception:
            return False

    def _acquire(
        self,
        binding: object,
        effective_deadline: float | None,
    ) -> tuple[object | None, bool, bool]:
        limiter = self._limiter(binding)
        if limiter is None:
            return None, True, False
        if self._in_cooldown(limiter):
            return limiter, False, True
        acquire = getattr(limiter, "acquire", None)
        if not callable(acquire):
            return None, True, False
        timeout: float | None = None
        if effective_deadline is not None:
            timeout = max(0.0, effective_deadline - time.monotonic())
        try:
            acquired = acquire(timeout=timeout)
        except TypeError:
            acquired = acquire(timeout)
        return (
            limiter,
            bool(acquired),
            (not bool(acquired) and self._in_cooldown(limiter)),
        )

    @staticmethod
    def _cooldown(limiter: object | None, seconds: object = None) -> None:
        if limiter is None:
            return
        callback = getattr(limiter, "cooldown", None)
        if not callable(callback):
            return
        value: float | None = None
        if (
            not isinstance(seconds, bool)
            and isinstance(seconds, (int, float))
            and math.isfinite(seconds)
            and seconds >= 0
        ):
            value = float(seconds)
        try:
            if value is None:
                callback()
            else:
                callback(value)
        except Exception:
            pass

    @staticmethod
    def _release(limiter: object | None) -> None:
        if limiter is None:
            return
        release = getattr(limiter, "release", None)
        if callable(release):
            try:
                release()
            except Exception:
                pass

    def _extractor(self, binding: object) -> object:
        catalog = self.catalog
        if catalog is not None:
            resolver = getattr(catalog, "extractor", None)
            if callable(resolver):
                return resolver(binding)
        extract = getattr(binding, "extract", None)
        if callable(extract):
            return binding
        if isinstance(binding, str):
            return build_extractor(binding)
        if isinstance(binding, ExtractorBinding):
            return build_extractor(binding)
        raise ExecutionError("web extractor catalog is unavailable")

    @staticmethod
    def _extract_binding_digest(
        extract_chain: tuple[object, ...], request: FetchRequest
    ) -> str:
        members: list[dict[str, object]] = []
        for binding in extract_chain:
            options = getattr(binding, "options", {})
            try:
                options = _json_value(options)
            except Exception:
                options = {}
            members.append(
                {
                    "name": WebContentClient._name(binding, "extractor"),
                    "type": WebContentClient._type(binding, "extractor"),
                    "options": options,
                }
            )
        quality = {
            "min_chars": request.quality.min_chars,
            "min_words": request.quality.min_words,
            "max_link_ratio": request.quality.max_link_ratio,
            "reject_challenges": request.quality.reject_challenges,
            "max_body_bytes": request.quality.max_body_bytes,
        }
        payload = json.dumps(
            {"extractors": members, "quality": quality},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def _with_attempts(
        artifact: DocumentArtifact,
        attempts: tuple[WebAttemptFailure, ...],
    ) -> DocumentArtifact:
        return replace(artifact, attempts=attempts)

    @staticmethod
    def _source_artifact(artifact: DocumentArtifact, mode: str) -> DocumentArtifact:
        """Retain only the representation that can cross a Source boundary."""

        common: dict[str, object] = {
            "selected_format": None,
            "extraction": None,
            "extract_binding_digest": None,
            "quality": None,
        }
        if mode == "list_html":
            common.update(
                raw_source=None,
                text=None,
                markdown=None,
                capabilities=frozenset({"raw_html"}),
            )
        elif mode == "raw_source":
            common.update(
                raw_html=None,
                text=None,
                markdown=None,
                title=None,
                metadata={},
                capabilities=frozenset({"raw_source"}),
            )
        return replace(artifact, **common)

    @staticmethod
    def _artifact_consistency_failure(artifact: DocumentArtifact) -> str | None:
        """Require capabilities to describe every returned representation exactly."""

        fields = {
            "raw_html": artifact.raw_html,
            "raw_source": artifact.raw_source,
            "text": artifact.text,
            "markdown": artifact.markdown,
        }
        for capability, value in fields.items():
            if (value is not None) != (capability in artifact.capabilities):
                return "missing_capability"
        selected = artifact.selected_format
        if selected is not None and (
            selected not in artifact.capabilities or fields.get(selected) is None
        ):
            return "missing_capability"
        return None

    @classmethod
    def _provider_artifact(
        cls,
        artifact: object,
        binding: object,
        request: FetchRequest,
    ) -> DocumentArtifact:
        if not isinstance(artifact, DocumentArtifact):
            if artifact is NO_MATCH:
                raise WebSafetyError("extract_failed")
            raise WebSafetyError("provider_error")
        if isinstance(binding, DocumentArtifact):
            provider_instance = (
                binding.provider_instance or artifact.provider_instance or "provider"
            )
            provider_type = (
                binding.provider_type or artifact.provider_type or "provider"
            )
        else:
            provider_instance = cls._name(binding)
            provider_type = cls._type(binding)
        return replace(
            artifact,
            source_url=artifact.source_url or request.url,
            final_url=artifact.final_url or request.url,
            provider_instance=provider_instance,
            provider_type=provider_type,
            attempts=(),
        )

    @staticmethod
    def _field_for(artifact: DocumentArtifact, selected: str) -> object:
        return {
            "raw_html": artifact.raw_html,
            "text": artifact.text,
            "markdown": artifact.markdown,
        }.get(selected)

    @staticmethod
    def _quality_view(artifact: DocumentArtifact) -> DocumentArtifact:
        selected = artifact.selected_format
        if selected == "raw_html":
            return replace(artifact, content_type="text/html", text=None, markdown=None)
        if selected == "text":
            return replace(
                artifact, content_type="text/plain", raw_html=None, markdown=None
            )
        if selected == "markdown":
            return replace(
                artifact, content_type="text/markdown", raw_html=None, text=None
            )
        return artifact

    @classmethod
    def _evaluate_selected(
        cls, artifact: DocumentArtifact, request: FetchRequest
    ) -> QualityEvaluation:
        selected = artifact.selected_format
        if selected not in request.acceptable or selected not in artifact.capabilities:
            return QualityEvaluation(
                QualityMetrics(reason="missing_capability"), artifact
            )
        if not isinstance(cls._field_for(artifact, selected), str):
            return QualityEvaluation(QualityMetrics(reason="empty"), artifact)
        return evaluate_quality(cls._quality_view(artifact), request.quality)

    @classmethod
    def _finalize_article(
        cls,
        candidate: DocumentArtifact,
        request: FetchRequest,
        extract_chain: tuple[object, ...],
    ) -> DocumentArtifact:
        if candidate.selected_format is None:
            raise WebSafetyError("extract_failed")
        quality = evaluate_quality(
            cls._quality_view(candidate), request.quality
        ).quality
        return replace(
            candidate,
            quality=quality,
            extract_binding_digest=cls._extract_binding_digest(extract_chain, request),
        )

    def _article_candidate(
        self,
        artifact: DocumentArtifact,
        request: FetchRequest,
        extract_chain: tuple[object, ...],
    ) -> tuple[DocumentArtifact | None, str]:
        status = artifact.status_code
        if status is not None and (status < 200 or status >= 300 or status == 204):
            return None, "http_status"
        body_sizes = [
            len(value) for value in (artifact.raw_source,) if isinstance(value, bytes)
        ]
        body_sizes.extend(
            len(value.encode("utf-8"))
            for value in (artifact.raw_html, artifact.text, artifact.markdown)
            if isinstance(value, str)
        )
        if body_sizes and max(body_sizes) > request.quality.max_body_bytes:
            return None, "body_too_large"

        content_type = (artifact.content_type or "").split(";", 1)[0].strip().casefold()
        if content_type:
            if content_type not in {
                "text/html",
                "application/xhtml+xml",
                "text/plain",
                "text/markdown",
            }:
                return None, "bad_content_type"
            if (
                artifact.raw_html is not None
                and "raw_html" in artifact.capabilities
                and content_type not in {"text/html", "application/xhtml+xml"}
            ):
                return None, "bad_content_type"
        else:
            sniffed = evaluate_quality(artifact, request.quality)
            if sniffed.reason == "bad_content_type":
                return None, "bad_content_type"

        last_reason = "missing_capability"

        # Evaluate an advertised raw HTML body for safety diagnostics before
        # handing it to local extractors.  A challenge/empty-shell rejection
        # must remain the attempt reason even when every extractor returns
        # ``NO_MATCH``; it is never a valid empty article candidate.
        if artifact.raw_html is not None and "raw_html" in artifact.capabilities:
            raw_probe = replace(
                artifact,
                selected_format="raw_html",
                text=None,
                markdown=None,
                raw_source=None,
            )
            raw_evaluation = evaluate_quality(raw_probe, request.quality)
            if not raw_evaluation.passed:
                last_reason = raw_evaluation.reason or last_reason
                if last_reason in {
                    "http_status",
                    "bad_content_type",
                    "body_too_large",
                    "challenge",
                    "empty_shell",
                }:
                    return None, last_reason

        # Provider-original Markdown/text can be accepted without asking the
        # catalog for a second identity extractor.  This is the fast path for
        # reader providers and, importantly, prevents a provider-supplied
        # quality-passing representation from being processed twice.  The
        # shared identity implementation still sanitizes Markdown and records
        # the selected representation deterministically.
        #
        # Request preference orders provider originals.  The provider's
        # selected format is only a fallback when the request did not order it,
        # followed by a stable default for acceptable-but-unpreferred fields.
        identity_order = dict.fromkeys(
            (*request.preferred, artifact.selected_format, "markdown", "text")
        )
        for representation in identity_order:
            if (
                representation not in {"markdown", "text"}
                or representation not in request.acceptable
                or representation not in artifact.capabilities
            ):
                continue
            value = self._field_for(artifact, representation)
            if not isinstance(value, str) or not value.strip():
                last_reason = "empty"
                continue

            identity_input = replace(
                artifact,
                markdown=value if representation == "markdown" else None,
                text=value if representation == "text" else None,
                selected_format=representation,
                capabilities=frozenset(
                    capability
                    for capability in artifact.capabilities
                    if capability not in {"markdown", "text"}
                    or capability == representation
                ),
            )
            try:
                fast = extract_identity(identity_input)
            except Exception as error:
                last_reason = sanitize_web_error(error)
                continue
            if not isinstance(fast, DocumentArtifact):
                last_reason = "extract_failed"
                continue

            fast = replace(
                fast,
                markdown=(
                    fast.markdown if representation == "markdown" else artifact.markdown
                ),
                text=fast.text if representation == "text" else artifact.text,
                capabilities=artifact.capabilities,
            )
            if fast.selected_format != representation:
                last_reason = "extract_failed"
                continue
            evaluation = self._evaluate_selected(fast, request)
            if evaluation.passed:
                return (
                    self._finalize_article(
                        replace(fast, quality=evaluation.quality),
                        request,
                        extract_chain,
                    ),
                    "",
                )
            last_reason = evaluation.reason or "too_short"

        original = artifact
        if (
            "raw_html" not in request.acceptable
            or "raw_html" not in artifact.capabilities
            or not isinstance(artifact.raw_html, str)
        ):
            return None, last_reason
        # Expanded chains are identity-first.  The pure candidate loop above
        # owns that identity step; only the remaining local extractors run.
        for extractor_binding in extract_chain[1:]:
            self._check_probe()
            try:
                extractor = self._extractor(extractor_binding)
                produced = extractor.extract(original)
                self._check_probe()
            except (WebAttemptCancelled, WebStageDeadlineExpired):
                raise
            except Exception as error:
                last_reason = sanitize_web_error(error)
                continue
            if produced is NO_MATCH or produced is None:
                if last_reason not in {"challenge", "empty_shell", "bad_content_type"}:
                    last_reason = "extract_failed"
                continue
            if not isinstance(produced, DocumentArtifact):
                last_reason = "extract_failed"
                continue
            produced = self._provider_artifact(produced, original, request)
            selected = produced.selected_format
            if selected not in request.acceptable:
                last_reason = "missing_capability"
                continue
            evaluation = self._evaluate_selected(produced, request)
            if evaluation.passed:
                return (
                    self._finalize_article(
                        replace(produced, quality=evaluation.quality),
                        request,
                        extract_chain,
                    ),
                    "",
                )
            last_reason = evaluation.reason or "extract_failed"
        return None, last_reason

    @staticmethod
    def _validate_list_artifact(
        artifact: DocumentArtifact, request: FetchRequest
    ) -> str | None:
        if not isinstance(artifact.raw_html, str) or not artifact.raw_html.strip():
            return "empty"
        if "raw_html" not in artifact.capabilities:
            return "missing_capability"
        status = artifact.status_code
        if status is not None and (status < 200 or status >= 300 or status == 204):
            return "http_status"
        if len(artifact.raw_html.encode("utf-8")) > request.quality.max_body_bytes:
            return "body_too_large"
        content_type = (artifact.content_type or "").split(";", 1)[0].strip().casefold()
        if content_type and content_type not in {"text/html", "application/xhtml+xml"}:
            return "bad_content_type"
        policy = replace(
            request.quality, min_chars=0, min_words=None, max_link_ratio=None
        )
        evaluation = evaluate_quality(
            replace(
                artifact,
                selected_format="raw_html",
                raw_source=None,
                text=None,
                markdown=None,
            ),
            policy,
        )
        return evaluation.reason

    @staticmethod
    def _validate_raw_artifact(
        artifact: DocumentArtifact, request: FetchRequest
    ) -> str | None:
        if not isinstance(artifact.raw_source, bytes):
            return "missing_capability"
        if "raw_source" not in artifact.capabilities:
            return "missing_capability"
        status = artifact.status_code
        if status is not None and (status < 200 or status >= 300 or status == 204):
            return "http_status"
        if len(artifact.raw_source) > request.quality.max_body_bytes:
            return "body_too_large"
        content_type = (artifact.content_type or "").split(";", 1)[0].strip().casefold()
        xml_type = content_type in {
            "application/xml",
            "text/xml",
        } or content_type.endswith("+xml")
        malformed_type = (
            bool(content_type) and _MIME_TYPE.fullmatch(content_type) is None
        )
        text_or_generic_type = (
            malformed_type
            or content_type.startswith("text/")
            or content_type
            in {
                "",
                "application/octet-stream",
                "binary/octet-stream",
            }
        )
        if content_type in {"text/html", "application/xhtml+xml", "text/markdown"}:
            return "bad_content_type"
        if not xml_type and not text_or_generic_type:
            return "bad_content_type"
        return (
            None if validate_raw_feed(artifact.raw_source) is not None else "unsafe_xml"
        )

    def resolve(
        self,
        request: FetchRequest,
        fetch_chain: Iterable[ProviderBinding],
        extract_chain: Iterable[ExtractorBinding] | None,
    ) -> DocumentArtifact:
        """Acquire providers sequentially and resolve one mode-specific winner."""

        if self._closed:
            raise ExecutionError("web content client is closed")
        if not isinstance(request, FetchRequest):
            raise TypeError("request must be a FetchRequest")
        providers = tuple(fetch_chain)
        if not providers:
            raise ExecutionError("web fetch chain is empty")
        extractors = tuple(extract_chain or ())
        if request.mode == "article":
            identity_count = sum(
                self._type(binding, "extractor") == "identity" for binding in extractors
            )
            if (
                not extractors
                or self._type(extractors[0], "extractor") != "identity"
                or identity_count != 1
            ):
                raise ExecutionError(
                    "article mode requires an identity-first extractor chain"
                )
        elif extractors:
            raise ExecutionError(f"{request.mode} mode forbids an extractor chain")
        attempts: list[WebAttemptFailure] = []
        empty_list_candidate: DocumentArtifact | None = None

        for ordinal, binding in enumerate(providers, 1):
            self._check_deadline(request)
            self._check_probe()
            eligibility_started = time.monotonic()
            capabilities = self._capabilities(binding)
            if not capabilities.intersection(request.acceptable):
                attempts.append(
                    self._safe_failure(
                        binding,
                        ordinal,
                        "missing_capability",
                        started=eligibility_started,
                        detail_code="static_capability",
                    )
                )
                continue

            # The provider clock starts immediately before limiter acquisition;
            # static capability skips consume no paid provider budget.
            started = time.monotonic()
            provider_deadline, effective_deadline = self._provider_deadlines(
                binding,
                request,
                started,
            )
            provider_request = (
                request
                if effective_deadline == request.stage_deadline
                else replace(request, stage_deadline=effective_deadline)
            )
            limiter: object | None = None
            acquired = False
            try:
                self._check_probe()
                limiter, acquired, cooling_down = self._acquire(
                    binding,
                    effective_deadline,
                )
                if not acquired:
                    # Stage ownership always dominates a skip.  Provider
                    # contention itself remains provider_busy, while a live
                    # cooldown is rate_limited.
                    self._check_deadline(request)
                    self._check_probe()
                    attempts.append(
                        self._safe_failure(
                            binding,
                            ordinal,
                            "rate_limited" if cooling_down else "provider_busy",
                            started=started,
                            detail_code=("cooldown" if cooling_down else "limiter"),
                        )
                    )
                    continue
                self._check_provider_deadline(request, provider_deadline)
                self._check_probe()
                provider = self._provider(binding)
                fetch = getattr(provider, "fetch", None)
                if not callable(fetch):
                    raise WebSafetyError("provider_error")
                fetched = fetch(provider_request)
                self._check_provider_deadline(request, provider_deadline)
                self._check_probe()
                artifact = self._provider_artifact(fetched, binding, request)

                consistency_failure = self._artifact_consistency_failure(artifact)
                if (
                    consistency_failure is None
                    and not artifact.capabilities <= capabilities
                ):
                    consistency_failure = "missing_capability"
                if consistency_failure is not None:
                    attempts.append(
                        self._safe_failure(
                            binding,
                            ordinal,
                            consistency_failure,
                            started=started,
                            artifact=artifact,
                        )
                    )
                    continue

                if request.mode == "list_html":
                    reason = self._validate_list_artifact(artifact, request)
                    if reason is not None:
                        if reason == "http_status" and artifact.status_code == 429:
                            reason = "rate_limited"
                            self._cooldown(limiter)
                        attempts.append(
                            self._safe_failure(
                                binding,
                                ordinal,
                                reason,
                                started=started,
                                artifact=artifact,
                            )
                        )
                        continue
                    try:
                        matches = select_list_records(
                            artifact.raw_html or "",
                            artifact.final_url or request.url,
                            request.list_selector or "",
                        )
                        self._check_provider_deadline(request, provider_deadline)
                        self._check_probe()
                    except (WebAttemptCancelled, WebStageDeadlineExpired):
                        raise
                    except Exception as error:
                        reason = sanitize_web_error(error)
                        attempts.append(
                            self._safe_failure(
                                binding,
                                ordinal,
                                reason,
                                started=started,
                                artifact=artifact,
                            )
                        )
                        continue
                    if matches:
                        self._check_probe()
                        return self._with_attempts(
                            self._source_artifact(artifact, request.mode),
                            tuple(attempts),
                        )
                    if empty_list_candidate is None:
                        empty_list_candidate = self._source_artifact(
                            artifact, request.mode
                        )
                    continue

                if request.mode == "raw_source":
                    reason = self._validate_raw_artifact(artifact, request)
                    if reason is None:
                        self._check_probe()
                        return self._with_attempts(
                            self._source_artifact(artifact, request.mode),
                            tuple(attempts),
                        )
                    if reason == "http_status" and artifact.status_code == 429:
                        reason = "rate_limited"
                        self._cooldown(limiter)
                    attempts.append(
                        self._safe_failure(
                            binding,
                            ordinal,
                            reason,
                            started=started,
                            artifact=artifact,
                        )
                    )
                    continue

                candidate, reason = self._article_candidate(
                    artifact, request, extractors
                )
                self._check_provider_deadline(request, provider_deadline)
                self._check_probe()
                if candidate is not None:
                    return self._with_attempts(candidate, tuple(attempts))
                if reason == "http_status" and artifact.status_code == 429:
                    reason = "rate_limited"
                    self._cooldown(limiter)
                attempts.append(
                    self._safe_failure(
                        binding,
                        ordinal,
                        reason,
                        started=started,
                        artifact=artifact,
                    )
                )
            except (WebAttemptCancelled, WebStageDeadlineExpired):
                raise
            except Exception as error:
                # A transport can report its effective timeout at the same
                # instant that the owning Stage/probe becomes stale.  Durable
                # Stage ownership dominates an ordinary provider failure.
                self._check_deadline(request)
                self._check_probe()
                reason = sanitize_web_error(error)
                if reason == "rate_limited":
                    self._cooldown(
                        limiter,
                        getattr(error, "retry_after_seconds", None),
                    )
                status_code = getattr(error, "status_code", None)
                if isinstance(status_code, bool) or not isinstance(status_code, int):
                    status_code = None
                status_kind = getattr(error, "status_kind", None)
                attempts.append(
                    self._safe_failure(
                        binding,
                        ordinal,
                        reason,
                        started=started,
                        status_code=status_code,
                        status_kind=status_kind,
                    )
                )
            finally:
                if acquired:
                    self._release(limiter)

        self._check_deadline(request)
        self._check_probe()
        if request.mode == "list_html" and empty_list_candidate is not None:
            return self._with_attempts(empty_list_candidate, tuple(attempts))
        if request.mode == "article":
            raise FetchChainExhausted(tuple(attempts))
        raise ExecutionError("web source provider chain exhausted")

    def close(self) -> None:
        self._closed = True

    def run_web(self, invocation: PrivateWebInvocation, input: object) -> object:
        del input
        if self._closed:
            raise ExecutionError("web content client is closed")
        if not isinstance(invocation, PrivateWebInvocation):
            raise ExecutionError("invalid private web invocation")
        raise ExecutionError("web task coordinator is not configured")

    def collect_web(
        self, invocation: PrivateWebInvocation, checkpoint: object
    ) -> object:
        del checkpoint
        if self._closed:
            raise ExecutionError("web content client is closed")
        if not isinstance(invocation, PrivateWebInvocation):
            raise ExecutionError("invalid private web invocation")
        raise ExecutionError("web source coordinator is not configured")


__all__ = ["WebContentClient"]
