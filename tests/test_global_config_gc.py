from sluice.config import GlobalConfig


def test_global_config_has_gc_defaults():
    g = GlobalConfig()
    assert g.gc.failed_items_older_than == "90d"
    assert g.gc.url_cache_max_rows == 50000
