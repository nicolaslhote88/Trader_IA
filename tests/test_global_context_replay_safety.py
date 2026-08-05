from pathlib import Path


def test_replay_is_read_only_and_has_no_order_transport():
    path = Path("outils/scripts/replay_ag1_global_context.py")
    text = path.read_text(encoding="utf-8")
    assert "read_only=True" in text
    assert "requests." not in text
    assert "httpx." not in text
    assert "/orders" not in text
    assert "place_orders" not in text
    assert "historique_sans_contexte" in text
    assert "contexte_ag5_ag8" in text
    assert "contexte_ag5_ag9" in text
