# Guillaume Valadon <guillaume.valadon@ssi.gouv.fr>


from deroleru import process_data
import json

default_params = [10, 5, 0.9, 2, 0.9]

for line in open("../data/pfx_cfl_2016.json"):
    data = json.loads(line)
    prefixes = data["prefixes"]
    conflicts = data["conflicts"]
    result = process_data(prefixes, conflicts, *default_params)
    if result:
        for asn in data["ases"]:
            for leak in result:
                print asn, leak
