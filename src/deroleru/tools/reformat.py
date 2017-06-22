import json


all_values_prefixes = dict()
all_values_conflicts = dict()


def aggregate(all_values, filename):
    for line in open(filename):
        if "start_date" in line:
            continue

        data = json.loads(line)
        key = data.keys()[0]

        asn = int(key)
        all_values[asn] = data[key]

aggregate(all_values_prefixes, "data/pfx_2015_all_by_month.json")
aggregate(all_values_conflicts, "data/cfl_2015_all_by_month.json")

ases = set(all_values_prefixes.keys() + all_values_conflicts.keys())

all_values = dict()
for asn in ases:
    prefixes = tuple(all_values_prefixes.get(asn, [0]*365))
    conflicts = tuple(all_values_conflicts.get(asn, [0]*365))

    key = (prefixes, conflicts)
    all_values[key] = all_values.get(key, list()) + [asn]

for values, ases in all_values.iteritems():
    doc = {"ases": ases, "prefixes": values[0], "conflicts": values[1]}
    print json.dumps(doc)
