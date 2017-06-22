# -*- coding: utf-8 -*-
# This file is part of the tabi project licensed under the MIT license.
# Copyright (C) 2017 ANSSI

import argparse
from collections import defaultdict
import json
from itertools import chain

from agnostic_loader import DataLoader
import os
from radix import Radix
from src.related_work_implem.tools import set_default


def get_stable_sets(pfx_ipt):
    _stable_sets = {}
    for elt in pfx_ipt:
        if elt:
            asn = elt["origin_asn"]
            for pfx in elt["prefixes"]:
                _stable_sets[pfx] = _stable_sets.get(pfx, {})
                _stable_sets[pfx][asn] = _stable_sets[pfx].get(asn, 0) + 1
    stable_sets = {}
    for pfx in _stable_sets:
        stables = set([asn for asn in _stable_sets[pfx] if _stable_sets[pfx][asn] > 1])
        if stables:
            stable_sets[pfx] = stables
    return stable_sets


def get_filtered_origin_changes(cfl_ipt):
    origin_changes = {}
    for elt in cfl_ipt:
        if elt:
            pfx = elt["origin"]["prefix"]
            if elt["type"] == "ABNORMAL" and pfx == elt["hijacker"]["prefix"]:
                origin_changes[pfx] = origin_changes.get(pfx, set())
                origin_changes[pfx].update({elt["origin"]["asn"], elt["hijacker"]["asn"]})
    return {pfx: ases for pfx, ases in origin_changes.iteritems() if len(ases) > 1}


def has_announced_bigger(asn_stable_pfx, pfx):
    rad_tree = Radix()
    rad_tree.add(pfx)
    for p in asn_stable_pfx:
        if rad_tree.search_covered(p):
            return True
    return False

def is_related_ixp(ixp, asn, pfx_stable_set):
    """
    Check if asn is part of the same IXP as at least one of pfx stable ASes.
    """
    if asn not in ixp:
        return False
    return set(ixp[asn]) & set(chain.from_iterable(
        ixp.get(int(stable_asn), []) for stable_asn in pfx_stable_set))


def get_conflicts(origin_changes, stable_sets):
    ixp_file = os.path.join(os.path.dirname(__file__), "ixp.json")
    assert os.path.isfile(ixp_file), "Run ixp_parser first"
    with open(ixp_file, "r") as f:
        ixp = {int(asn): set(v) for asn, v in json.loads(f.read()).iteritems()}

    conflicts = {}
    ases_stable_prefixes = {}
    for pfx in stable_sets:
        for asn in stable_sets[pfx]:
            ases_stable_prefixes[asn] = ases_stable_prefixes.get(asn, set())
            ases_stable_prefixes[asn].add(pfx)
    for pfx, ases in origin_changes.iteritems():
        stable_ases = stable_sets.get(pfx, [])
        conflicts[pfx] = {"stable_ases": set(), "conflicting_ases": set()}
        for asn in ases:
            if asn in stable_ases:
                conflicts[pfx]["stable_ases"].add(asn)
            elif not has_announced_bigger(ases_stable_prefixes.get(asn, []), pfx) \
                    and not is_related_ixp(ixp, asn, stable_ases):
                conflicts[pfx]["conflicting_ases"].add(asn)
    return conflicts


def get_lrl(conflicts, threshold=10):
    offense = defaultdict(set)
    for pfx in conflicts:
        for asn in conflicts[pfx]["conflicting_ases"]:
            offense[asn].add(tuple(sorted(conflicts[pfx]["stable_ases"])))
    for asn in offense.keys():
        if len(offense[asn]) < threshold:
            del offense[asn]
    return offense


def main(pfx_ipt, cfl_ipt, threshold=5):
    stable_sets = get_stable_sets(pfx_ipt)
    origin_changes = get_filtered_origin_changes(cfl_ipt)
    conflicts = get_conflicts(origin_changes, stable_sets)
    return get_lrl(conflicts, threshold)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--out_dir", default=None,
                        help="output directory - current directory if not given")
    subparsers = parser.add_subparsers(dest="command")
    subparser = subparsers.add_parser("origin_changes")
    subparser.add_argument("conflicts_dir")
    subparser = subparsers.add_parser("stable_set")
    subparser.add_argument("prefixes_dir")
    subparser = subparsers.add_parser("conflicts")
    subparser.add_argument("changes_file")
    subparser.add_argument("stable_sets_file")
    subparser = subparsers.add_parser("lrl")
    subparser.add_argument("input_data", help="bgp_dumps_dir or conflicts_file")
    subparser.add_argument("--full_stack", default=False,
                           help="if specified, will run origin changes, conflicts and lrl "
                                "- input_data must be bgp_dumps_dir")
    subparser.add_argument("--threshold", default=None)

    args = parser.parse_args()

    out_dir = args.out_dir or os.path.dirname(__file__)

    if args.command == "origin_changes":
        CFL_LOADER = DataLoader(args.conflicts_dir)
        CFL_LOADER.filter_on_filename = lambda x: x.startswith("2016")
        oc = get_filtered_origin_changes(CFL_LOADER.load())
        with open(os.path.join(out_dir, "origin_changes_adapted_algo.json"), "w") as f:
            f.write(json.dumps(oc, default=set_default))

    if args.command == "stable_set":
        PFX_LOADER = DataLoader(args.prefixes_dir)
        PFX_LOADER.filter_on_filename = lambda x: x.startswith("2016")
        stable_sets = get_stable_sets(PFX_LOADER.load())
        with open(os.path.join(out_dir, "stable_sets_adapted_algo.json"), "w") as f:
            f.write(json.dumps(stable_sets, default=set_default))

    if args.command == "conflicts":
        with open(args.changes_file) as f:
            changes = json.loads(f.read())
        with open(args.stable_sets_file) as f:
            stable_sets = json.loads(f.read())
        conflicts = get_conflicts(changes, stable_sets)
        with open(os.path.join(out_dir, "conflicts_adapted_algo.json"), "w") as f:
            f.write(json.dumps(conflicts, default=set_default))

    if args.command == "lrl":
        if args.full_stack:
            raise ValueError("Not supported yet")
        else:
            with open(args.input_data, "r") as f:
                conflicts = json.loads(f.read())
        lrl_args = [conflicts]
        if args.threshold:
            lrl_args.append(args.threshold)
        with open(os.path.join(out_dir, "lrl_adapted_algo.json"), "w") as f:
            f.write(json.dumps(get_lrl(*lrl_args), default=set_default))

