import pytest

from route_leaks_detection.heuristics.detect_route_leaks import *
from route_leaks_detection.heuristics.detect_route_leaks import _RuFindRouteLeaks, _PyFindRouteLeaks

PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources")

import route_leaks_detection.heuristics.detect_route_leaks

route_leaks_detection.heuristics.detect_route_leaks.MIN_NB_DAYS = 3

RUST = route_leaks_detection.heuristics.detect_route_leaks.USE_RUST
if not RUST:
    print >> sys.stderr, "WARNING: Rust version not found"

DEBUG = False

def run_twice(test_ft):
    def wrapper(*args, **kwargs):
        if RUST:
            route_leaks_detection.heuristics.detect_route_leaks.USE_RUST = True
            test_ft(*args, **kwargs)
        route_leaks_detection.heuristics.detect_route_leaks.USE_RUST = False
        try:
            test_ft(*args, **kwargs)
        finally:
            route_leaks_detection.heuristics.detect_route_leaks.USE_RUST = RUST

    return wrapper


def use_rust(test_ft):
    def wrapper(*args, **kwargs):
        save = route_leaks_detection.heuristics.detect_route_leaks.USE_RUST
        route_leaks_detection.heuristics.detect_route_leaks.USE_RUST = True
        try:
            test_ft(*args, **kwargs)
        finally:
            route_leaks_detection.heuristics.detect_route_leaks.USE_RUST = save

    return wrapper


def use_python(test_ft):
    def wrapper(*args, **kwargs):
        save = route_leaks_detection.heuristics.detect_route_leaks.USE_RUST
        route_leaks_detection.heuristics.detect_route_leaks.USE_RUST = False
        try:
            test_ft(*args, **kwargs)
        finally:
            route_leaks_detection.heuristics.detect_route_leaks.USE_RUST = save

    return wrapper


# PEAK DETECTION

@pytest.mark.skipif(DEBUG, reason="speed up during debug")
def test00_speculate_missing_values():
    original_data = [5, 5, 5, 0, 5, 5]
    peak_finder = FindPeaks(original_data)
    peak_finder.speculate_missing_values()
    assert peak_finder.data == [5, 5, 5, 5, 5, 5]

    original_data = [5, 5, 15, 0, 5, 5]
    peak_finder = FindPeaks(original_data, peak_min_value=100)
    peak_finder.speculate_missing_values()
    assert peak_finder.data == [5, 5, 15, 10, 5, 5]

    original_data = [0, 5, 5, 0, 5, 0]
    peak_finder = FindPeaks(original_data)
    peak_finder.speculate_missing_values()
    assert peak_finder.data == [5, 5, 5, 5, 5, 5]

    original_data = [5, 5, 25, 0, 5, 5]
    peak_finder = FindPeaks(original_data, peak_min_value=100)
    peak_finder.speculate_missing_values()
    assert peak_finder.data == [5, 5, 25, 15, 5, 5]

    original_data = [5, 5, 24, 0, 5, 5]
    peak_finder = FindPeaks(original_data)
    peak_finder.speculate_missing_values()
    assert peak_finder.data == [5, 5, 24, 6.9, 5, 5]


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
def test01_check_std_variation():
    data = [5, 5, 25, 5, 5, 5]
    peak_finder = FindPeaks(data)
    assert peak_finder._check_std_variation([2])

    data = [5, 5, 25, 5, 25, 5]
    peak_finder = FindPeaks(data)
    assert peak_finder._check_std_variation([2, 4])

    data = [15, 5, 35, 5, 15, 5]
    peak_finder = FindPeaks(data)
    assert peak_finder._check_std_variation([2])

    data = [15, 5, 16, 5, 15, 5]
    peak_finder = FindPeaks(data)
    assert not peak_finder._check_std_variation([2])

    data = [15, 25, 15, 15, 1, 15]
    peak_finder = FindPeaks(data)
    assert not peak_finder._check_std_variation([2])
    # could be True but test is here for the sake of understanding the function


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
def test02_get_big_maxes():
    data = [5, 5, 25, 5, 5, 5]
    peak_finder = FindPeaks(data)
    assert peak_finder.get_big_maxes() == peak_finder.big_maxes == [2]

    data = [5, 5, 25, 5, 25, 5]
    peak_finder = FindPeaks(data)
    assert peak_finder.get_big_maxes() == peak_finder.big_maxes == [2, 4]

    data = [5, 5, 30, 5, 29, 5]
    peak_finder = FindPeaks(data)
    assert peak_finder.get_big_maxes() == peak_finder.big_maxes == [2, 4]

    data = [15, 5, 35, 5, 15, 5]
    peak_finder = FindPeaks(data)
    # 15 is too far from biggest value 35
    assert peak_finder.get_big_maxes() == peak_finder.big_maxes == [2]

    data = [15, 5, 35, 5, 15, 5]
    peak_finder = FindPeaks(data)
    assert peak_finder.get_big_maxes() == peak_finder.big_maxes == [2]

    data = [5, 25, 5, 25, 5, 25, 5, 25, 5]
    peak_finder = FindPeaks(data)
    assert peak_finder.get_big_maxes() == peak_finder.big_maxes == []

    data = [5, 20, 5, 24, 5, 20, 5, 20, 5]
    peak_finder = FindPeaks(data)
    assert peak_finder.get_big_maxes() == peak_finder.big_maxes == []


# LEAK DETECTION
@pytest.mark.skipif(DEBUG, reason="speed up during debug")
@pytest.mark.skipif(not RUST, reason="Rust version non found")
@use_rust
def test00_mock_rust():
    route_leaks_detection.heuristics.detect_route_leaks.USE_RUST = True
    assert FindRouteLeaks([], []).finder._call_rust_leak_detection(
        [0] * 365, [0] * 365, {"cfl_peak_min_value": 0, "pfx_peak_min_value": 0,
                               "percent_std": 1, "percent_similarity": 0,
                               "max_nb_peaks": 365}) == []


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
@pytest.mark.skipif(not RUST, reason="Rust version non found")
@use_rust
def test01_factory_ru():
    finder = FindRouteLeaks([], [])
    assert isinstance(finder.finder, _RuFindRouteLeaks)


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
@use_python
def test02_factory_py():
    finder = FindRouteLeaks([], [])
    assert isinstance(finder.finder, _PyFindRouteLeaks)


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
@use_python
def test03_get_ases_with_peak():
    plotable_dict = dict()
    plotable_dict[12322] = [5, 5, 25, 5, 5, 5]
    plotable_dict[3215] = [5, 5, 25, 5, 25, 5]
    plotable_dict[202214] = [5, 5, 5, 5, 5, 5]
    leak_finder = FindRouteLeaks(dict(), dict())
    res = leak_finder.finder._get_ases_with_peak(plotable_dict)
    assert res == {12322: [2], 3215: [2, 4]}


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
@run_twice
def test04_get_route_leaks_from_dict():
    pfx_dict = dict()
    pfx_dict[12322] = [5, 5, 25, 5, 5, 5]
    pfx_dict[3215] = [5, 5, 25, 5, 25, 5]
    pfx_dict[202214] = [5, 5, 5, 5, 5, 5]
    pfx_dict[123] = [5, 5, 5, 5, 50, 5]
    pfx_dict[10] = [5, 5, 5, 5, 50, 5]
    pfx_dict[100] = [5, 5, 50, 5, 50, 5]

    cfl_dict = dict()
    cfl_dict[12322] = [5, 5, 5, 5, 5, 5]
    cfl_dict[3215] = [5, 5, 25, 5, 5, 5]
    cfl_dict[202214] = [5, 5, 20, 5, 5, 5]
    cfl_dict[10] = [5, 5, 50, 5, 5, 5]
    cfl_dict[100] = [5, 5, 50, 5, 50, 5]

    leak_finder = FindRouteLeaks(pfx_dict, cfl_dict)

    res = leak_finder.get_route_leaks()
    assert res == {3215: {"leaks": [2],
                          "pfx_data": [5, 5, 25, 5, 25, 5],
                          "cfl_data": [5, 5, 25, 5, 5, 5]},
                   100: {"leaks": [2, 4],
                         "pfx_data": [5, 5, 50, 5, 50, 5],
                         "cfl_data": [5, 5, 50, 5, 50, 5]}}

    pfx_dict["start_date"] = "2015-01-01"
    cfl_dict["start_date"] = "2015-01-01"

    leak_finder = FindRouteLeaks(pfx_dict, cfl_dict)

    res = leak_finder.get_route_leaks()
    assert res == {3215: {"leaks": ["2015-01-03"],
                          "pfx_data": [5, 5, 25, 5, 25, 5],
                          "cfl_data": [5, 5, 25, 5, 5, 5]},
                   100: {"leaks": ["2015-01-03", "2015-01-05"],
                         "pfx_data": [5, 5, 50, 5, 50, 5],
                         "cfl_data": [5, 5, 50, 5, 50, 5]}}

    leak_finder = FindRouteLeaks(pfx_dict, cfl_dict, start_date="2015-01-01", end_date="2015-01-31")

    res = leak_finder.get_route_leaks()
    assert res == {3215: {"leaks": ["2015-01-03"],
                          "pfx_data": [5, 5, 25, 5, 25, 5],
                          "cfl_data": [5, 5, 25, 5, 5, 5]},
                   100: {"leaks": ["2015-01-03", "2015-01-05"],
                         "pfx_data": [5, 5, 50, 5, 50, 5],
                         "cfl_data": [5, 5, 50, 5, 50, 5]}}


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
@run_twice
def test05_get_route_leaks_chg_params():
    pfx_dict = dict()
    pfx_dict[12322] = [5, 5, 25, 5, 5, 5]
    pfx_dict[3215] = [5, 5, 25, 5, 25, 5]
    pfx_dict[202214] = [5, 5, 5, 5, 5, 5]
    pfx_dict[123] = [5, 5, 5, 5, 50, 5]
    pfx_dict[10] = [5, 5, 5, 5, 50, 5]
    pfx_dict[100] = [5, 5, 50, 5, 50, 5]

    cfl_dict = dict()
    cfl_dict[12322] = [5, 5, 5, 5, 5, 5]
    cfl_dict[3215] = [5, 5, 25, 5, 5, 5]
    cfl_dict[202214] = [5, 5, 20, 5, 5, 5]
    cfl_dict[10] = [5, 5, 50, 5, 5, 5]
    cfl_dict[100] = [5, 5, 50, 5, 50, 5]

    leak_finder = FindRouteLeaks(pfx_dict, cfl_dict)

    res = leak_finder.get_route_leaks()
    assert res == {3215: {"leaks": [2],
                          "pfx_data": [5, 5, 25, 5, 25, 5],
                          "cfl_data": [5, 5, 25, 5, 5, 5]},
                   100: {"leaks": [2, 4],
                         "pfx_data": [5, 5, 50, 5, 50, 5],
                         "cfl_data": [5, 5, 50, 5, 50, 5]}}

    res = leak_finder.get_route_leaks(pfx_peak_min_value=100)
    assert res == {}

    res = leak_finder.get_route_leaks(pfx_peak_min_value=30)
    assert res == {100: {"leaks": [2, 4],
                         "pfx_data": [5, 5, 50, 5, 50, 5],
                         "cfl_data": [5, 5, 50, 5, 50, 5]}}


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
@run_twice
def test06_get_route_leaks_no_res():
    pfx_dict = dict()
    pfx_dict[12322] = [5]
    pfx_dict[3215] = [5]
    pfx_dict[202214] = [5]
    pfx_dict[123] = [5]
    pfx_dict[10] = [5]
    pfx_dict[100] = [5]

    cfl_dict = dict()
    cfl_dict[12322] = [5]
    cfl_dict[3215] = [5]
    cfl_dict[202214] = [5]
    cfl_dict[10] = [5]
    cfl_dict[100] = [5]

    leak_finder = FindRouteLeaks(pfx_dict, cfl_dict)

    res = leak_finder.get_route_leaks()
    assert res == {}


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
@run_twice
def test07_get_route_leaks_from_dir():
    pfx_dir = os.path.join(PATH, "prefixes")
    cfl_dir = os.path.join(PATH, "conflicts")

    leak_finder = FindRouteLeaks(pfx_dir, cfl_dir, data_already_processed=False)
    assert leak_finder.get_route_leaks() == {202214: {'cfl_data': [1, 7, 1, 1],
                                                      'leaks': ['2014-12-03'],
                                                      'pfx_data': [1, 100, 1, 1]}}

    leak_finder = FindRouteLeaks(pfx_dir, cfl_dir, data_already_processed=False,
                                 end_date="2014-12-04")
    assert leak_finder.get_route_leaks() == {202214: {'cfl_data': [1, 7, 1],
                                                      'leaks': ['2014-12-03'],
                                                      'pfx_data': [1, 100, 1]}}


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
def test08_get_route_leaks_from_dir_missing_files(caplog):
    pfx_dir = os.path.join(PATH, "prefixes")
    cfl_dir = os.path.join(PATH, "conflicts")
    leak_finder = FindRouteLeaks(pfx_dir, cfl_dir, data_already_processed=False,
                                 start_date="2014-12-01")
    res = leak_finder.get_route_leaks()

    assert "WARNING" in caplog.text()
    assert res == {202214: {'cfl_data': [0, 1, 7, 1, 1],
                            'leaks': ['2014-12-03'],
                            'pfx_data': [0, 1, 100, 1, 1]}}


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
@run_twice
def test09_get_route_leaks_from_files():
    pfx_file = os.path.join(PATH, "prefixes_processed.json")
    cfl_file = os.path.join(PATH, "conflicts_processed.json")
    leak_finder = FindRouteLeaks(pfx_file, cfl_file)
    res = leak_finder.get_route_leaks()
    assert res == {50810: {'leaks': ['2015-01-08'],
                           'cfl_data': [0, 0, 2, 2, 2, 0, 0, 20, 0, 0],
                           'pfx_data': [0, 0, 0, 0, 0, 0, 0, 46, 0, 0]}}


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
@run_twice
def test10_get_route_leaks_from_files_diff_start():
    pfx_file = os.path.join(PATH, "prefixes_processed.json")
    cfl_file = os.path.join(PATH, "conflicts_processed_wrong_start.json")

    with pytest.raises(ValueError):
        FindRouteLeaks(pfx_file, cfl_file)


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
def test11_main(capsys):
    pfx_file = os.path.join(PATH, "prefixes_processed.json")
    cfl_file = os.path.join(PATH, "conflicts_processed.json")

    main([pfx_file, cfl_file])
    out, err = capsys.readouterr()
    out = [json.loads(line) for line in out.split("\n")[:-1]]
    assert out == [{'50810': ['2015-01-08']}]


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
def test12_main_change_param_reduce_leaks(capsys):
    pfx_file = os.path.join(PATH, "prefixes_processed.json")
    cfl_file = os.path.join(PATH, "conflicts_processed.json")

    main([pfx_file, cfl_file, "--pfx_peak_min_value", "100"])
    out, err = capsys.readouterr()
    assert out == ""


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
def test13_main_change_params_raise_leaks(capsys):
    pfx_file = os.path.join(PATH, "prefixes_processed.json")
    cfl_file = os.path.join(PATH, "conflicts_processed.json")

    main([pfx_file, cfl_file, "--pfx_peak_min_value", "2"])
    out, err = capsys.readouterr()
    out = [json.loads(line) for line in out.split("\n")[:-1]]
    assert len(out) == 2


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
@run_twice
def test14_main_diff_start():
    pfx_file = os.path.join(PATH, "prefixes_processed.json")
    cfl_file = os.path.join(PATH, "conflicts_processed_wrong_start.json")

    with pytest.raises(ValueError):
        main([pfx_file, cfl_file])


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
@run_twice
def test15_main_cheat_using_dir():
    pfx_dir = os.path.join(PATH, "prefixes")
    cfl_dir = os.path.join(PATH, "conflicts")

    with pytest.raises(AssertionError):
        main([pfx_dir, cfl_dir])


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
@run_twice
def test16_get_rejection_cause():
    pfx_data = {202214: [15, 5, 16, 5, 15, 5],
                12322: [5, 5, 16, 5, 15, 5],
                3215: [5, 5, 16, 5, 20, 5]}
    cfl_data = {202214: [0] * 6,
                3215: [0, 0, 3, 0, 0, 0]}
    finder = FindRouteLeaks(pfx_data, cfl_data)
    exp = {'prefixes_rejection_cause':
               ('percent_std', (5.1774081890030228, 4.8989794855663558, 1.0568340210970446, 5)),
           'conflicts_rejection_cause': ('not a local max', (0, 0, 0))}
    assert finder.get_rejection_cause(202214, 2) == exp
    exp = {'prefixes_rejection_cause': ('peak detected',),
           'conflicts_rejection_cause': 'AS 12322 not found in data'}
    assert finder.get_rejection_cause(12322, 2) == exp
    exp = {'prefixes_rejection_cause': ('percent_sim', (16, 20)),
           'conflicts_rejection_cause': ('peak_min_value', (0, 3, 0))}
    assert finder.get_rejection_cause(3215, 2) == exp

@pytest.mark.skipif(DEBUG, reason="speed up during debug")
@run_twice
def test17_get_check_info_by_param():
    pfx_data = {202214: [15, 5, 16, 5, 15, 5]}
    cfl_data = {202214: [0] * 6}
    finder = FindRouteLeaks(pfx_data, cfl_data)
    exp = {
        'prefixes_info': {
            'first_big_maxes': [2],
            2: {'peak_min_value': [11, 11], 'percent_sim': [16, 14.4], 'max_nb_peaks': [2]},
            4: {'peak_min_value': [10, 10], 'percent_sim': [15, 14.4]}, 'absolute_max':
            16, 'leaks_detected': [],
            'variations': [10, 11, 11, 10, 10],
            'percent_std': [4.8989794855663558, 5.1774081890030228],
            'big_maxes': [2],
            'smooth_values': [15, 5, 5, 15, 5]},
        'conflicts_info': {'first_big_maxes': [], 'percent_std': [0.0, 0.0], 'absolute_max': 0,
                           'leaks_detected': [], 'variations': [0, 0, 0, 0, 0], 'big_maxes': [],
                           'smooth_values': [0, 0, 0, 0, 0, 0]}}
    assert finder.get_check_info_by_param(202214) == exp


@pytest.mark.skipif(DEBUG, reason="speed up during debug")
@use_python
def test18_manage_duplicates():
    pfx_data = {202214: [5, 5, 16, 5, 15, 5],
                3215: [5, 5, 16, 5, 15, 5]}
    cfl_data = {202214: [0, 0, 10, 0, 0, 0],
                3215: [0, 0, 10, 0, 0, 0]}
    finder = FindRouteLeaks(pfx_data, cfl_data)
    assert len(finder.pfx_data) == 2
    assert len(finder.finder._pfx_unique_data) == 1
    assert len(finder.get_route_leaks()) == 2
