import os
import sys

os.environ["OBB_DISABLE_SCHEDULER"] = "1"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))

import import_manual_shipments as ims  # noqa: E402


def test_kit_code_maps_to_sku():
    assert ims.kit_sku_for_code("CQ41") == "OBB-CQ-41 KITS"
    assert ims.kit_sku_for_code(" cq31 ") == "OBB-CQ-31 KITS"
    assert ims.kit_sku_for_code("FAMILUS+115HACKS") is None
    assert ims.kit_sku_for_code("") is None


def test_parse_sheet_sections_and_column_a(tmp_path):
    hdr = ",".join(["KIT ASSIGNMENT + KIT SKU", "ORDER NUMBER", "ORDER_EMAIL"] + [f"c{i}" for i in range(3, 23)])
    blank = [""] * 23

    def row(a, num, email, oid):
        r = list(blank)
        r[0], r[1], r[2], r[4], r[13] = a, num, email, "Jane Doe", oid
        return ",".join(r)

    p = tmp_path / "sept.csv"
    p.write_text("\n".join([
        hdr,
        ",".join(["CQ41"] + blank[1:]),
        row("", "#OBB-1", "A@x.com ", "111"),
        row("YESTO+WATERMELONMASK", "#OBB-2", "b@x.com", "222"),
        ",".join(blank),
        ",".join(["CQ31"] + blank[1:]),
        row("", "#OBB-3", "c@x.com", "333"),
    ]), encoding="utf-8")
    rows = ims.parse_sheet(str(p))
    assert [(r["kit_code"], r["order_name"], r["email"], r["order_id"], r["col_a"]) for r in rows] == [
        ("CQ41", "#OBB-1", "a@x.com", "111", ""),
        ("CQ41", "#OBB-2", "b@x.com", "222", "YESTO+WATERMELONMASK"),
        ("CQ31", "#OBB-3", "c@x.com", "333", ""),
    ]
