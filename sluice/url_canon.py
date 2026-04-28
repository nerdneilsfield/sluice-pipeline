from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
    "ref",
    "ref_src",
    "mc_cid",
    "mc_eid",
}


def canonical_url(url: str) -> str:
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    query = "&".join(
        f"{k}={v}"
        for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if k not in TRACKING_PARAMS
    )
    return urlunsplit((scheme, netloc, parts.path, query, ""))
