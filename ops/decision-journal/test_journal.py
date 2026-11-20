import sqlite3
import tempfile
from pathlib import Path
import unittest

from evidence import covers, protect
from journal import Journal
from ledger import fill_links, order_links


class EvidenceTests(unittest.TestCase):
    def test_defaults_and_graph_drift(self):
        self.assertTrue(covers({"a":[{"value":0}]},{"a":[{"value":0,"default":1}]}))
        self.assertFalse(covers({"a":0},{"a":False}))
        self.assertFalse(covers({"a":["ask"]},{"a":["changed"]}))

    def test_partial_execution_in_another_run(self):
        orders=[dict(order_id="O1",broker_order_id="B1",client_id="C1")]
        fills=[dict(order_id=None,run_id="other",broker_order_id="B1",client_id=None,fill_id="F"+i,execution_id=i) for i in ("E1","E2","E2")]
        unresolved=fill_links(orders,fills)
        self.assertEqual([x["execution_id"] for x in orders[0]["fills"]],["E1","E2"])
        self.assertEqual(unresolved[0]["reason"],"duplicate_execution_reference")

    def test_conflicting_broker_id(self):
        orders=[dict(order_id="O1",broker_order_id="B1",client_id=None)]
        f=dict(order_id="O1",broker_order_id="B2",execution_id="E",fill_id="F",client_id=None)
        self.assertEqual(fill_links(orders,[f])[0]["reason"],"broker_order_id_conflict")
        self.assertFalse(orders[0]["fills"])

    def test_ambiguous_and_hold(self):
        common=dict(run_id="R",symbol="S",side="BUY",action="OPEN",intent="long",status="CONSENSUS_APPROVED")
        orders=[dict(order_id=o,symbol="S",side="BUY") for o in ("one","two")]
        hold=dict(common,action="HOLD",side=None,status="NO_CONSENSUS")
        d=dict(common)
        order_links([d,hold],orders)
        self.assertEqual(d["link_method"],"ambigu")
        self.assertFalse(d["linked_order_ids"])
        self.assertEqual(hold["link_method"],"aucun_ordre_attribué")

    def test_archive_rollback_replay_and_late_fill(self):
        with tempfile.TemporaryDirectory() as temp:
            j=Journal(Path(temp)/"test.sqlite")
            with j.db:
                j.put("ledger","r",{"fills":[]})
            before=j.counts()
            with self.assertRaises(sqlite3.OperationalError), j.db:
                j.put("ledger","r",{"fills":["f"]})
                j.db.execute("insert into absent values (1)")
            self.assertEqual(j.counts(),before)
            self.assertEqual(j.get("ledger","r"),{"fills":[]})
            with j.db:
                self.assertTrue(j.put("ledger","r",{"fills":["f"]}))
                self.assertFalse(j.put("ledger","r",{"fills":["f"]}))
            self.assertEqual(j.counts(),{"refs":1,"objects":2,"revisions":2})
            j.db.close()

    def test_mask_private_fields_and_preserve_numeric_text(self):
        self.assertEqual(protect({"model":"abc", "account":"hidden","prompt":"U123456789 00012"}),{"model":"abc","prompt":"[masqué] 00012"})


if __name__ == "__main__":
    unittest.main()
