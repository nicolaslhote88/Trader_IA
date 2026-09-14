import unittest

from app_modules.navigation import DEFAULT_NAV_SECTION, NAV_SECTIONS, page_labels, resolve_page


class NavigationContractTests(unittest.TestCase):
    def test_default_section_is_available(self):
        self.assertIn(DEFAULT_NAV_SECTION, NAV_SECTIONS)

    def test_visible_labels_are_unique_inside_each_section(self):
        for section in NAV_SECTIONS:
            labels = page_labels(section)
            self.assertEqual(len(labels), len(set(labels)), section)

    def test_all_historical_page_ids_are_reachable_once(self):
        page_ids = [
            page_id
            for config in NAV_SECTIONS.values()
            for _, page_id in config["pages"]
        ]
        self.assertEqual(9, len(page_ids))
        self.assertEqual(len(page_ids), len(set(page_ids)))

    def test_business_labels_resolve_to_existing_page_ids(self):
        self.assertEqual("System Health (Monitoring)", resolve_page("Pilotage global", "État du système"))
        self.assertEqual("Dashboard Trading", resolve_page("Actions / ETF", "Portefeuille actions"))
        self.assertEqual("Three Pillars Monitor", resolve_page("Forex", "Contexte & piliers"))


if __name__ == "__main__":
    unittest.main()
