# -*- coding: utf-8 -*-
# This file is part of the tabi project licensed under the MIT license.
# Copyright (C) 2017 ANSSI

from collections import defaultdict
import json
import os
import requests
from bs4 import BeautifulSoup


def get_ixp_list():
    main = requests.get("https://www.euro-ix.net/ixps/list-ixps/")
    ixp = defaultdict(set)
    parser = BeautifulSoup(main.content)
    for line in parser.find_all("tr"):
        link = line.findChild("a")
        if link:
            details = requests.get("https://www.euro-ix.net%s" % link.get("href"))
            links = BeautifulSoup(details.content).find_all("a")
            for l in links:
                if l.get("href") and "peeringdb" in l.get("href"):
                    ixp[l.string].add(link.string)
    with open(os.path.join(os.path.dirname(__file__), "ixp__.json"), "w") as f:
        f.write(json.dumps({k: list(v) for k, v in ixp.iteritems()}))


if __name__ == '__main__':
    get_ixp_list()