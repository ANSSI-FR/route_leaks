import errno

import pytest

from route_leaks_detection.prepare_data.prepare import *

PATH = os.path.join(os.path.dirname(__file__), "resources")

if not os.path.exists(os.path.join(PATH, "result")):
    try:
        os.makedirs(os.path.join(PATH, "result"))
    except OSError as exc:  # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise


def tool_compare_files(res_file, exp_file, ft_open_res=gzip.open, ft_open_exp=open):
    """
    Compare generator to data in file. Both should contain dictionaries (or any json serializable).

    :param res_file: string - filename
    :param exp_file: string - filename
    :return: Boolean
    """
    ret = True
    res_data = []
    exp_data = []

    with ft_open_res(res_file, 'r') as f:
        for line in f:
            res_data.append(json.loads(line))
    with ft_open_exp(exp_file, 'r') as f:
        for line in f:
            exp_data.append(json.loads(line))

    for elt in exp_data:
        if elt not in res_data:
            print "in exp not in res"
            print elt
            ret = False
            break
    else:
        for elt in res_data:
            if elt not in exp_data:
                print "in res not in exp"
                print elt
                ret = False
                break
    return ret


def test01_count_prefixes():
    expected = {54721: 1, 51587: 1, 201751: 5, 26130: 3, 59897: 2, 62232: 2, 18105: 5, 50810: 46,
                46044: 4, 47563: 1}
    res = count_daily_prefixes(DataLoader(os.path.join(PATH, "input", "prefixes.json")).load())
    assert res == expected


def test02_count_conflicts():
    expected = {10201: 1, 29386: 1, 47887: 7}
    res = count_daily_conflicts(DataLoader(os.path.join(PATH, "input", "conflicts.json")).load())
    assert res == expected


def test03_date_from_filename():
    assert date_from_filename("2015-01-01.json.gz") == datetime(2015, 1, 1)
    assert date_from_filename("2015-01-01.gz") == datetime(2015, 1, 1)
    assert date_from_filename("2015-01-01") == datetime(2015, 1, 1)
    with pytest.raises(ValueError):
        date_from_filename("not_a_date.json.gz")
    with pytest.raises(ValueError):
        date_from_filename("2016-20-01.json.gz")


def test05_update_day():
    merged_input = {"AS1": [5, 2],
                    "AS2": [1, 5],
                    "AS3": [10, 0],
                    "AS5": [0, 7],
                    "AS7": [0, 1]}
    expected = {"AS1": [5, 2, 0],
                "AS2": [1, 5, 0],
                "AS3": [10, 0, 0],
                "AS5": [0, 7, 2],
                "AS7": [0, 1, 0],
                "AS8": [0, 0, 10]}
    day_data = [{"origin_asn": "AS5", "num_prefixes": 2},
                {"origin_asn": "AS8", "num_prefixes": 10}]

    res = update_day(merged_input, day_data, count_daily_prefixes)
    assert res == expected


def test06_update_from_files():
    update_day_from_files("pfx", os.path.join(PATH, "input", "fake_pfx_data.json"),
                          os.path.join(PATH, "input", "raw_daily_pfx_for_merge",
                                       "2016-01-03.json"),
                          os.path.join(PATH, "result", "updated_pfx.json"), open)

    assert tool_compare_files(os.path.join(PATH, "result", "updated_pfx.json"),
                              os.path.join(PATH, "expected", "pfx_data.json"),
                              open, open)


def test06_update_from_files_no_merged():
    update_day_from_files("pfx", None,
                          os.path.join(PATH, "input", "raw_daily_pfx_for_merge",
                                       "2016-01-03.json"),
                          os.path.join(PATH, "result", "updated_pfx.json"), open)

    assert tool_compare_files(os.path.join(PATH, "result", "updated_pfx.json"),
                              os.path.join(PATH, "expected", "pfx_data_with_no_merged.json"),
                              open, open)


def test07_create_from_scratch():
    input_pfx_dir = os.path.join(PATH, "input", "raw_daily_pfx")
    input_cfl_dir = os.path.join(PATH, "input", "raw_daily_cfl")
    res_pfx_file = os.path.join(PATH, "result", "prefixes.json")
    res_cfl_file = os.path.join(PATH, "result", "conflicts.json")

    expected = {'pfx': {'start': datetime(2016, 1, 1, 0, 0),
                        'end': datetime(2016, 1, 3, 0, 0),
                        'data': {202214: [25, 30, 30]}},
                'cfl': {'start': datetime(2016, 1, 1, 0, 0),
                        'end': datetime(2016, 1, 3, 0, 0),
                        'data': {202214: [4, 4, 3]}}}

    res = create_from_scratch(input_pfx_dir, input_cfl_dir, res_pfx_file, res_cfl_file)
    assert res == expected


def test08_create_from_scratch_filter_day():
    input_pfx_dir = os.path.join(PATH, "input", "raw_daily_pfx")
    input_cfl_dir = os.path.join(PATH, "input", "raw_daily_cfl")
    res_pfx_file = os.path.join(PATH, "result", "prefixes.json")
    res_cfl_file = os.path.join(PATH, "result", "conflicts.json")

    expected = {'pfx': {'start': datetime(2016, 1, 2, 0, 0),
                        'end': datetime(2016, 1, 3, 0, 0),
                        'data': {202214: [30, 30]}},
                'cfl': {'start': datetime(2016, 1, 2, 0, 0),
                        'end': datetime(2016, 1, 3, 0, 0),
                        'data': {202214: [4, 3]}}}

    res = create_from_scratch(input_pfx_dir, input_cfl_dir, res_pfx_file, res_cfl_file,
                              start_date="2016-01-02")
    assert res == expected


def test09_main():
    input_pfx_dir = os.path.join(PATH, "input", "raw_daily_pfx")
    input_cfl_dir = os.path.join(PATH, "input", "raw_daily_cfl")

    main([input_pfx_dir, input_cfl_dir])

    with open(os.path.join(PATH, "input", "processed_prefixes.json"), "r") as f:
        assert map(json.loads, f.readlines()) == [{"start_date": "2016-01-01"},
                                                  {"202214": [25, 30, 30]}]
    with open(os.path.join(PATH, "input", "processed_conflicts.json"), "r") as f:
        assert map(json.loads, f.readlines()) == [{"start_date": "2016-01-01"},
                                                  {"202214": [4, 4, 3]}]


def test05_get_input_data_pfx_file_processed():
    filename = os.path.join(PATH, "prefixes_processed.json")
    loader = LoadRouteLeaksData(filename, 'pfx', open, data_already_processed=True)
    plotable_dict = loader.load_data()

    expected = {"fake_54721": [0, 0, 0, 0, 0, 0, 0, 0, 1, 0], 51587: [0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
                18105: [0, 0, 5, 0, 0, 0, 0, 0, 0, 0], 47563: [0, 0, 0, 0, 1, 0, 0, 0, 0, 0],
                26130: [0, 0, 0, 0, 0, 3, 0, 0, 0, 0], 201751: [0, 0, 0, 5, 0, 0, 0, 0, 0, 0],
                62232: [0, 0, 0, 0, 1, 0, 0, 0, 1, 0], 59897: [0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
                50810: [0, 0, 0, 0, 0, 0, 0, 46, 0, 0], 46044: [4, 0, 0, 0, 0, 0, 0, 0, 0, 0]}
    assert loader.str_start == "2016-01-01"
    assert plotable_dict == expected


def test06_get_input_data_cfl_file_processed():
    filename = os.path.join(PATH, "conflicts_processed.json")
    loader = LoadRouteLeaksData(filename, 'cfl', open, data_already_processed=True)
    plotable_dict = loader.load_data()
    expected = {10201: [0, 0, 0, 0, 1, 0, 0, 0, 0], 29386: [0, 0, 0, 1, 0, 0, 0, 0, 0],
                47887: [1, 1, 0, 0, 0, 2, 1, 1, 1]}
    assert loader.str_start == "2016-01-01"
    assert plotable_dict == expected


def test09_get_input_data_pfx_dict():
    file_data = {54721: [0, 0, 0, 0, 0, 0, 0, 0, 1, 0], 51587: [0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
                 18105: [0, 0, 5, 0, 0, 0, 0, 0, 0, 0], 47563: [0, 0, 0, 0, 1, 0, 0, 0, 0, 0],
                 26130: [0, 0, 0, 0, 0, 3, 0, 0, 0, 0], 201751: [0, 0, 0, 5, 0, 0, 0, 0, 0, 0],
                 62232: [0, 0, 0, 0, 1, 0, 0, 0, 1, 0], 59897: [0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
                 50810: [0, 0, 0, 0, 0, 0, 0, 46, 0, 0], 46044: [4, 0, 0, 0, 0, 0, 0, 0, 0, 0]}
    loader = LoadRouteLeaksData(file_data, 'pfx', open, data_already_processed=True)
    plotable_dict = loader.load_data()

    expected = {54721: [0, 0, 0, 0, 0, 0, 0, 0, 1, 0], 51587: [0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
                18105: [0, 0, 5, 0, 0, 0, 0, 0, 0, 0], 47563: [0, 0, 0, 0, 1, 0, 0, 0, 0, 0],
                26130: [0, 0, 0, 0, 0, 3, 0, 0, 0, 0], 201751: [0, 0, 0, 5, 0, 0, 0, 0, 0, 0],
                62232: [0, 0, 0, 0, 1, 0, 0, 0, 1, 0], 59897: [0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
                50810: [0, 0, 0, 0, 0, 0, 0, 46, 0, 0], 46044: [4, 0, 0, 0, 0, 0, 0, 0, 0, 0]}
    assert plotable_dict == expected


def test10_get_input_data_cfl_dict():
    file_data = {10201: [0, 0, 0, 0, 1, 0, 0, 0, 0], 29386: [0, 0, 0, 1, 0, 0, 0, 0, 0],
                 47887: [1, 1, 0, 0, 0, 2, 1, 1, 1]}
    loader = LoadRouteLeaksData(file_data, 'cfl', open, data_already_processed=True)
    plotable_dict = loader.load_data()
    expected = {10201: [0, 0, 0, 0, 1, 0, 0, 0, 0], 29386: [0, 0, 0, 1, 0, 0, 0, 0, 0],
                47887: [1, 1, 0, 0, 0, 2, 1, 1, 1]}
    assert plotable_dict == expected


def test11_get_input_data_pfx_not_processed():
    pfx_dir = os.path.join(PATH, "input", "raw_daily_pfx")
    loader = LoadRouteLeaksData(pfx_dir, 'pfx', open)
    plotable_dict = loader.load_data()

    assert loader.str_start == "2016-01-01"
    assert plotable_dict == {202214: [25, 30, 30]}


def test12_get_input_data_cfl_not_processed():
    cfl_dir = os.path.join(PATH, "input", "raw_daily_cfl")
    loader = LoadRouteLeaksData(cfl_dir, 'cfl', open)
    plotable_dict = loader.load_data()

    assert loader.str_start == "2016-01-01"
    assert plotable_dict == {202214: [4, 4, 3]}
