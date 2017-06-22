# Guillaume Valadon <guillaume.valadon@ssi.gouv.fr>

"""
deroleru - Detect Route Leaks in Rust - reformat.py
"""

import argparse
import json


def aggregate(values, filename):
    """
    Read data from filename, and store it in values.
    """

    for line in open(filename):
        if "start_date" in line:
            continue

        # Get JSON and build the key
        data = json.loads(line)
        key = data.keys()[0]

        # Store data in the dictionary
        asn = int(key)
        values[asn] = data[key]


if __name__ == "__main__":

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="reformat datasets")
    parser.add_argument("--directory", default="../../../data/",
                        help="Directory containing datasets")
    parser.add_argument("year", type=int, help="Year")
    args = parser.parse_args()

    # Load datasets
    all_values_prefixes = dict()
    all_values_conflicts = dict()
    aggregate(all_values_prefixes, "%s/prefixes_%s.json" % (args.directory, args.year))
    aggregate(all_values_conflicts, "%s/conflicts_%s.json" % (args.directory, args.year))

    # Merge AS numbers
    ases = set(all_values_prefixes.keys() + all_values_conflicts.keys())

    # Aggregate data
    all_values = dict()
    for asn in ases:
        prefixes = tuple(all_values_prefixes.get(asn, [0]*365))
        conflicts = tuple(all_values_conflicts.get(asn, [0]*365))

        key = (prefixes, conflicts)
        all_values[key] = all_values.get(key, list()) + [asn]

    # Dump data
    for values, ases in all_values.iteritems():
        doc = {"ases": ases, "prefixes": values[0], "conflicts": values[1]}
        print json.dumps(doc)
