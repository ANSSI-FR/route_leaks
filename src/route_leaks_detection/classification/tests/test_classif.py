from itertools import product

import pytest
from route_leaks_detection.classification.classification import *
from route_leaks_detection.classification.classification import _AsnPrefOrConfData

DEBUG = False

PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources")

functions_usage = {"_get_model_svm_classifier": False, "_fast_get_model_svm_classifier": False,
                   "_very_fast_get_model_svm_classifier": False}


def trace_calls(frame, event, arg):
    if event != 'call':
        return
    co = frame.f_code
    func_filename = co.co_filename
    if "classification" not in func_filename:
        return
    func_name = co.co_name
    if func_name in functions_usage:
        functions_usage[func_name] = True
    return


@pytest.fixture(scope="function")
def reset_functions_usage():
    for k in functions_usage:
        functions_usage[k] = False


@pytest.fixture(scope="module")
def init_trace():
    sys.settrace(trace_calls)


def test01_create_var_input():
    raw_data = {12322: [5, 5, 25, 5, 5, 0],
                3215: [5, 5, 25, 5, 25, 5],
                202214: [5, 5, 5, 5, 5, 5],
                123: [5, 5, 5, 5, 50, 5],
                10: [5, 5, 5, 5, 50, 5],
                100: [5, 5, 50, 5, 50, 5]}
    maker = CreateClassifBaseData(raw_data, "pfx", data_already_processed=True)
    maker.create_var_input()
    expected = {12322: [0, 20, -20, 0, 0],
                100: [0, 45, -45, 45, -45],
                202214: [0, 0, 0, 0, 0],
                10: [0, 0, 0, 45, -45],
                3215: [0, 20, -20, 20, -20],
                123: [0, 0, 0, 45, -45]}
    assert maker.var_data == expected


def test02_create_normalized_var_input():
    raw_data = {12322: [5, 5, 25, 5, 5, 5],
                3215: [5, 5, 25, 5, 25, 5],
                202214: [5, 5, 5, 5, 5, 5],
                123: [5, 5, 5, 5, 50, 5],
                10: [5, 5, 30, 5, 50, 5],
                100: [5, 1, 50, 5, 50, 5]}
    maker = CreateClassifBaseData(raw_data, "pfx", data_already_processed=True)
    maker.create_normalized_var_input()
    expected = {12322: [0, 1, -1, 0, 0],
                100: [-4. / 49, 1, -45. / 49, 45. / 49, -45. / 49],
                202214: [0, 0, 0, 0, 0],
                10: [0, 25. / 45, -25. / 45, 1, -1],
                3215: [0, 1, -1, 1, -1],
                123: [0, 0, 0, 1, -1]}
    assert maker.normalized_var_data == expected


def test03_get_max_var_index():
    # maker = _AsnPrefOrConfData(dict(), dict(), "pfx")
    assert _AsnPrefOrConfData._get_exact_max_indexes([0, 0, 0, 0, 0]) == [1, 2, 3]
    assert _AsnPrefOrConfData._get_exact_max_indexes([0, 0, 1, 0, 0]) == [2]
    assert _AsnPrefOrConfData._get_exact_max_indexes([0, 0, 1, 0, 1]) == [2]
    assert _AsnPrefOrConfData._get_exact_max_indexes([0, 1, 0, 1, 0]) == [1, 3]
    assert _AsnPrefOrConfData._get_exact_max_indexes([1, 1, 1, 0, 0]) == [1, 2]
    assert _AsnPrefOrConfData._get_exact_max_indexes([0, 0, 0, 0, 1]) == [1, 2, 3]


def test04_get_max_impact_on_std():
    assert AttributeMakers.get_max_impact_on_std([2, 2, 2, 2, 2, 10, 2, 2], 5) == 0
    assert AttributeMakers.get_max_impact_on_std([2, 2, 3, 2, 2, 10, 2, 2], 5) == .1336088764892297
    assert AttributeMakers.get_max_impact_on_std([2, 2, 2, 2, 2, 2, 2, 2], 5) == 1


def test05_create_svm_input():
    raw_data = {12322: [5, 5, 25, 5, 5, 5],
                3215: [5, 5, 25, 5, 25, 5],
                202214: [5, 5, 5, 5, 5, 5],
                123: [5, 5, 5, 5, 50, 5],
                10: [5, 5, 5, 5, 50, 5],
                100: [5, 5, 50, 5, 50, 5]}
    cfl_raw_data = {12322: [5, 5, 5, 5, 10, 5],
                    3215: [5, 5, 25, 5, 25, 5],
                    202214: [5, 5, 5, 5, 10, 5],
                    123: [5, 5, 5, 50, 5, 5],
                    100: [5, 5, 50, 5, 50, 5]}
    maker = CreateClassificationAttributes(raw_data, cfl_raw_data, data_already_processed=True)
    res = maker.create_svm_input(lambda x: "AS%s" % x)
    expected = {"AS12322": [1.0, -1.0, 0.0, 0.0, 0.16666666666666666, 0.6846531968814575,
                            1.1180339887498949, 0.60000000000000009, 0.2, 0.0, 1.0, 0.8,
                            2.995732273553991,

                            1.0, -1.0, 0.0, 0.0, 0.16666666666666666, 0.6846531968814575,
                            1.1180339887498949, 0.60000000000000009, 0.2, 0.0, 0.8, 0.8,
                            1.6094379124341003,

                            0.0, 1.0, 0,
                            1.1180339887498949, 0.0, 1.0, 0.0, 1.0, 1.0],

                "AS100": [1.0, -1.0, 1.0, -1.0, 0.3333333333333333, 0.9270248108869579,
                          0.92702481088695787, 1.0, 0.6, 1.0, 0.6, 0.6, 3.8066624897703196,

                          1.0, -1.0, 1.0, -1.0, 0.3333333333333333, 0.9270248108869579,
                          0.92702481088695787, 1.0, 0.6, 1.0, 0.6, 0.6, 3.8066624897703196,

                          1.0, 0.8, 7.613324979540639,
                          0.92702481088695787, 1.0, 0.8, 1.0, 0.8, 0.8],

                "AS3215": [1.0, -1.0, 1.0, -1.0, 0.3333333333333333, 0.9270248108869579,
                           0.92702481088695787, 1.0, 0.6, 1.0, 0.6, 0.6, 2.995732273553991,

                           1.0, -1.0, 1.0, -1.0, 0.3333333333333333, 0.9270248108869579,
                           0.92702481088695787, 1.0, 0.6, 1.0, 0.6, 0.6, 2.995732273553991,

                           1.0, 0.8, 5.991464547107982,
                           0.92702481088695787, 1.0, 0.8, 1.0, 0.8, 0.8],

                "AS123": [1.0, -1.0, -1.0, 0.0, 0.16666666666666666, 0.6846531968814575,
                          1.118033988749895, 0.60000000000000009, 0.2, 0.0, 0.8, 0.8,
                          3.8066624897703196,

                          1.0, -1.0, 0.0, 1.0, 0.16666666666666666, 0.6846531968814575,
                          0.6846531968814575, 0.60000000000000009, 0.2, 0.0, 1.0, 0.8,
                          3.8066624897703196,

                          0.0, 0.8, 0.0,
                          1.1180339887498949, 0.0, 1.0, 0.0, 1.0, 0.8]}

    for asn in res:
        if res[asn] != expected[asn]:
            print asn
            print res[asn]
            print expected[asn]
            assert res[asn] == expected[asn]
    for asn in expected:
        if res[asn] != expected[asn]:
            print asn
            print res[asn]
            print expected[asn]
            assert res[asn] == expected[asn]

    assert res == expected


def test06_classif_result_using_model(init_trace, reset_functions_usage):
    if DEBUG:
        pytest.skip()
    pfx_file14 = os.path.join(PATH, "input", "short_pfx_2014_all_by_month.json")
    cfl_file14 = os.path.join(PATH, "input", "cfl_2014_all_by_month.json")

    classifier = ApplyModel()

    classifier.get_model_svm_classifier()

    assert functions_usage == {"_get_model_svm_classifier": False,
                               "_fast_get_model_svm_classifier": False,
                               "_very_fast_get_model_svm_classifier": True}

    res = classifier.get_classification_result(pfx_file14, cfl_file14, open, True)

    assert len(res["PEAK"]) == 50
    assert len(res["NORMAL"]) == 267
    for label, data_type in product(["PEAK", "NORMAL"], ["prefixes", "conflicts"]):
        assert data_type in res[label].values()[0]
        assert len(res[label].values()[0][data_type]) == 365


def test07_classif_result_using_input_data(init_trace, reset_functions_usage):
    if DEBUG:
        pytest.skip()
    pfx_file14 = os.path.join(PATH, "input", "short_pfx_2014_all_by_month.json")
    cfl_file14 = os.path.join(PATH, "input", "cfl_2014_all_by_month.json")

    classifier = ApplyModel()
    classifier.model_svm_file = "nothing"

    classifier.get_model_svm_classifier()

    assert functions_usage == {"_get_model_svm_classifier": False,
                               "_fast_get_model_svm_classifier": True,
                               "_very_fast_get_model_svm_classifier": False}

    res = classifier.get_classification_result(pfx_file14, cfl_file14, open, True)

    assert len(res["PEAK"]) == 50
    assert len(res["NORMAL"]) == 267
    for label, data_type in product(["PEAK", "NORMAL"], ["prefixes", "conflicts"]):
        assert data_type in res[label].values()[0]
        assert len(res[label].values()[0][data_type]) == 365


def test08_classif_result_using_raw_data(init_trace, reset_functions_usage):
    if DEBUG:
        pytest.skip()
    pfx_file14 = os.path.join(PATH, "input", "short_pfx_2014_all_by_month.json")
    cfl_file14 = os.path.join(PATH, "input", "cfl_2014_all_by_month.json")

    classifier = ApplyModel()
    classifier.model_svm_file = "nothing"
    classifier.model_svm_input_file = "nothing"

    classifier.get_model_svm_classifier()

    assert functions_usage == {"_get_model_svm_classifier": True,
                               "_fast_get_model_svm_classifier": False,
                               "_very_fast_get_model_svm_classifier": False}

    res = classifier.get_classification_result(pfx_file14, cfl_file14, open, True)

    assert len(res["PEAK"]) == 50
    assert len(res["NORMAL"]) == 267
    for label, data_type in product(["PEAK", "NORMAL"], ["prefixes", "conflicts"]):
        assert data_type in res[label].values()[0]
        assert len(res[label].values()[0][data_type]) == 365


def test09_classif_result_no_data():
    if DEBUG:
        pytest.skip()
    pfx_file14 = os.path.join(PATH, "input", "empty.json")
    cfl_file14 = os.path.join(PATH, "input", "empty.json")

    classifier = ApplyModel()

    res = classifier.get_classification_result(pfx_file14, cfl_file14, open, True)

    assert res == {"PEAK": {}}


def test10_main(capsys):
    pfx_file14 = os.path.join(PATH, "input", "short_pfx_2014_all_by_month.json")
    cfl_file14 = os.path.join(PATH, "input", "cfl_2014_all_by_month.json")

    args = [pfx_file14, cfl_file14]

    main(args)
    out, err = capsys.readouterr()
    out = [json.loads(line) for line in out.split("\n")[:-1]]
    assert len(out) == 50
