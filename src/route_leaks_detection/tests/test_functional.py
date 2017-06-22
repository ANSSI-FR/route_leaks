import json

import os
from route_leaks_detection.heuristics.detect_route_leaks import main as h_main
from route_leaks_detection.classification.classification import main as ml_main

PFX_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
                        "data", "prefixes_2015.json")

CFL_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
                        "data", "conflicts_2015.json")


def test01_heuristics():
    output_file = os.path.join(os.path.dirname(__file__),
                               "h_out.json")
    if os.path.exists(output_file):
        os.remove(output_file)
    args = [PFX_FILE, CFL_FILE, "--out", output_file, "--fit_params"]
    h_main(args)
    with open(output_file, "r") as f:
        res = json.loads(f.read())
    assert len(res) == 36
    os.remove(output_file)


def test02_heuristics_stdout(capsys):
    output_file = os.path.join(os.path.dirname(__file__),
                               "h_out.json")
    if os.path.exists(output_file):
        os.remove(output_file)
    args = [PFX_FILE, CFL_FILE, "--fit_params"]
    h_main(args)
    out, err = capsys.readouterr()
    out = [json.loads(line) for line in out.split("\n")[:-1]]
    assert len(out) == 36


def test03_classifier():
    output_file = os.path.join(os.path.dirname(__file__),
                               "ml_out.json")
    if os.path.exists(output_file):
        os.remove(output_file)
    args = [PFX_FILE, CFL_FILE, "--out", output_file]
    ml_main(args)
    with open(output_file, "r") as f:
        res = json.loads(f.read())
    assert len(res) == 72
    os.remove(output_file)


def test04_classifier_stdout(capsys):
    output_file = os.path.join(os.path.dirname(__file__),
                               "ml_out.json")
    if os.path.exists(output_file):
        os.remove(output_file)
    args = [PFX_FILE, CFL_FILE]
    ml_main(args)
    out, err = capsys.readouterr()
    out = out.split("\n")[:-1]
    assert len(out) == 72
