from src.related_work_implem.adapted_lrl import *


def test01_get_stable_ases():
    pfx_ipt = [{"origin_asn": 202214, "prefixes": ["190.210.210.0/24", "190.210.211.0/24", "143.84.135.0/24"]},
               {"origin_asn": 202214, "prefixes": ["190.210.210.0/24", "190.210.211.0/24"]},
               {"origin_asn": 202214, "prefixes": ["190.210.210.0/24"]},
               {"origin_asn": 3215, "prefixes": ["190.210.210.0/24", "190.210.211.0/24"]},
               {"origin_asn": 3215, "prefixes": ["190.210.211.0/24"]}]
    res = get_stable_sets(pfx_ipt)
    assert res == {"190.210.210.0/24": {202214}, "190.210.211.0/24": {202214, 3215}}


def test02_get_unfiltered_conflicts():
    cfl_ipt = [{"origin": {"prefix": "67.128.0.0/13", "asn": 209},
                "hijacker": {"prefix": "67.132.207.0/24", "asn": 32291},
                "type": "DIRECT"},
               {"origin": {"prefix": "104.194.192.0/19", "asn": 22400},
                "hijacker": {"prefix": "104.194.192.0/22", "asn": 8100},
                "type": "ABNORMAL"},
               {"origin": {"prefix": "190.210.210.0/24", "asn": 1},
                "hijacker": {"prefix": "190.210.210.0/24", "asn": 2},
                "type": "ABNORMAL"},
               {"origin": {"prefix": "104.194.192.0/22", "asn": 202214},
                "hijacker": {"prefix": "104.194.192.0/22", "asn": 3215},
                "type": "ABNORMAL"},
               {"origin": {"prefix": "104.194.192.0/22", "asn": 202214},
                "hijacker": {"prefix": "104.194.192.0/22", "asn": 10},
                "type": "ABNORMAL"},
               {"origin": {"prefix": "104.194.192.0/22", "asn": 12322},
                "hijacker": {"prefix": "104.194.192.0/22", "asn": 15557},
                "type": "RELATION"}]
    res = get_filtered_origin_changes(cfl_ipt)
    assert res == {"104.194.192.0/22": {202214, 3215, 10}, "190.210.210.0/24": {1, 2}}


def test03_get_conflicts():
    origin_changes = {"104.194.192.0/22": {202214, 3215, 10}, "190.210.210.0/24": {1, 2}}
    stable_sets = {"104.194.192.0/22": {202214}, "190.210.210.0/24": {1, 2}}
    res = get_conflicts(origin_changes, stable_sets)
    assert res == {"104.194.192.0/22": {"stable_ases": {202214}, "conflicting_ases": {3215, 10}},
                   "190.210.210.0/24": {"stable_ases": {1, 2}, "conflicting_ases": set()}}


def test04_get_lrl():
    conflicts = {"104.194.192.0/22": {"stable_ases": {202214}, "conflicting_ases": {3215, 10}},
                 "190.210.210.0/23": {"stable_ases": {1, 2}, "conflicting_ases": {3215}},
                 "190.210.210.0/24": {"stable_ases": {1, 2}, "conflicting_ases": set()}}
    res = get_lrl(conflicts, threshold=2)
    assert res == {3215: {(1, 2), (202214,)}}
