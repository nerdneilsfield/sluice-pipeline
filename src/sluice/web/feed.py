"""Bounded, source-only RSS/Atom XML validation.

This module deliberately returns a parsed root only after the bytes have
passed the same XML safety boundary used by the web Source.  It never follows
external references and never accepts an arbitrary XML document merely
because it happens to contain an ``item`` or ``entry`` element.
"""

from __future__ import annotations

from typing import Any
from xml.parsers import expat

from defusedxml import ElementTree
from defusedxml.common import DefusedXmlException

from .contracts import ValidatedFeedRoot


MAX_FEED_BYTES = 10_485_760
_RSS1_NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
_XINCLUDE_NAMESPACE = "http://www.w3.org/2001/XInclude"


def _local_name(tag: Any) -> str:
    if not isinstance(tag, str):
        return ""
    return tag.rsplit("}", 1)[-1]


def _namespace(tag: Any) -> str:
    if not isinstance(tag, str) or not tag.startswith("{"):
        return ""
    namespace, _, _local = tag[1:].partition("}")
    return namespace


class _UnsafeXML(Exception):
    """Private expat signal used before the tree-building parse."""


def _reject_unsafe_declarations(raw_source: bytes) -> bool:
    """Reject DTD/entity constructs without assuming the document encoding.

    This bounded expat pass runs before the required defusedxml tree parser and
    avoids byte-substring checks that can miss UTF-16 declarations or reject
    harmless text containing words such as ``include``.
    """

    parser = expat.ParserCreate()

    def reject(*_args: object) -> None:
        raise _UnsafeXML

    parser.StartDoctypeDeclHandler = reject
    parser.EntityDeclHandler = reject
    parser.UnparsedEntityDeclHandler = reject
    parser.ExternalEntityRefHandler = reject
    parser.SkippedEntityHandler = reject
    try:
        parser.Parse(raw_source, True)
    except (_UnsafeXML, expat.ExpatError, ValueError, TypeError):
        return True
    return False


def _tree_contains_xinclude(root: Any) -> bool:
    try:
        iterator = root.iter()
    except Exception:
        return True
    for element in iterator:
        if _namespace(getattr(element, "tag", None)) == _XINCLUDE_NAMESPACE:
            return True
    return False


def _has_direct_child(root: Any, wanted: str) -> bool:
    try:
        return any(_local_name(getattr(child, "tag", None)) == wanted for child in list(root))
    except Exception:
        return False


def validate_raw_feed(raw_source: bytes | bytearray | memoryview) -> ValidatedFeedRoot | None:
    """Return a validated RSS2/RSS1/Atom root, or ``None`` for any rejection.

    The body must already be a decompressed in-memory response.  Size is
    checked before invoking the parser, and every parser/security failure is
    intentionally collapsed to ``None`` so callers can advance to the next
    provider without exposing parser or entity details.
    """

    if not isinstance(raw_source, (bytes, bytearray, memoryview)):
        return None
    raw = bytes(raw_source)
    if not raw or len(raw) > MAX_FEED_BYTES or _reject_unsafe_declarations(raw):
        return None
    try:
        root = ElementTree.fromstring(
            raw,
            forbid_dtd=True,
            forbid_entities=True,
            forbid_external=True,
        )
    except (DefusedXmlException, ElementTree.ParseError, ValueError, TypeError):
        return None
    except Exception:
        # Different defusedxml releases expose a few parser-limit exceptions
        # outside DefusedXmlException; none are safe to propagate here.
        return None
    if _tree_contains_xinclude(root):
        return None

    local = _local_name(getattr(root, "tag", None))
    namespace = _namespace(getattr(root, "tag", None))
    if local == "rss":
        # RSS 2.x requires a channel element.  Namespaces on the channel are
        # tolerated because feeds in the wild commonly qualify extensions.
        if not _has_direct_child(root, "channel"):
            return None
        return ValidatedFeedRoot(root=root, kind="rss2")
    if local == "feed":
        return ValidatedFeedRoot(root=root, kind="atom")
    if local == "RDF" and namespace == _RSS1_NAMESPACE:
        return ValidatedFeedRoot(root=root, kind="rss1")
    return None


__all__ = ["MAX_FEED_BYTES", "ValidatedFeedRoot", "validate_raw_feed"]
