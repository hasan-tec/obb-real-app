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


def test_manifest_matches_kit_items_ignoring_punctuation():
    kit = ["Yes To - Watermelon Mask", "Familus - 115 Hacks & Hacktivities for Parents of Mini Humans",
           "Starlabs Beauty - Deep Wave 80 Hourly Hydro Boost Serum"]
    manifest = [ims.norm_item(x) for x in ("YESTO+WATERMELONMASK ", "FAMILUS+115HACKS&HACKTIVITIESFORPARENTSOFMINIHUMANS",
                                           "STARLABSBEAUTY+DEEPWAVE80HOURHYDROBOOSTSERUM")]
    assert ims.manifest_mismatches(manifest, kit) == []
    assert ims.manifest_mismatches([ims.norm_item("TUMMYTAPE+SINGLEPLAYFULPINK")], kit) == ["tummytapesingleplayfulpink"]


def test_section_manifests_groups_column_a_by_kit():
    rows = [{"kit_code": "CQ41", "col_a": "YESTO+WATERMELONMASK"}, {"kit_code": "CQ41", "col_a": ""},
            {"kit_code": "CQ31", "col_a": "BABYSHOWERANNOUNCEMENTCARDS"}]
    assert ims.section_manifests(rows) == {"CQ41": ["yestowatermelonmask"], "CQ31": ["babyshowerannouncementcards"]}


def test_manual_reason_says_staff_shipped_it_and_why():
    r = ims.manual_reason("#OBB-1", "OBB-CQ-41 KITS", None,
                          "[Bulk-re-curated] All T4 kits have duplicate items with customer history", ["Yes To - Watermelon Mask"])
    assert r.startswith("[Manually processed] Staff shipped OBB-CQ-41 KITS by hand outside the engine")
    assert "engine did not assign a kit (All T4 kits have duplicate items" in r
    assert "1 item(s) already received before: Yes To - Watermelon Mask" in r
    assert "engine assigned OBB-CQ-21 KITS" in ims.manual_reason("#OBB-2", "OBB-CQ-31 KITS", "OBB-CQ-21 KITS", "", [])
