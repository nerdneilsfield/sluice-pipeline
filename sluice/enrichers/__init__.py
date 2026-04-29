from sluice.registry import register_enricher_lazy

register_enricher_lazy("hn_comments", "sluice.enrichers.hn_comments:HnCommentsEnricher")
