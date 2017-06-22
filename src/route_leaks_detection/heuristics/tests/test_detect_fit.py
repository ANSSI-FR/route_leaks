from mock import patch
import src.route_leaks_detection.heuristics.detect_route_leaks
from src.route_leaks_detection.heuristics.tests.test_detect import run_twice

src.route_leaks_detection.heuristics.detect_route_leaks.MIN_NB_DAYS = 3
from src.route_leaks_detection.heuristics.detect_route_leaks import *

import pytest

PFX_FILE_15 = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))),
    "data", "prefixes_2015.json")
CFL_FILE_15 = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))),
    "data", "conflicts_2015.json")

FUNCTIONAL = False
DEBUG = False


def test02_parameter_fail():
    with pytest.raises(ValueError):
        ParamValue("fake_parameter")


@run_twice
def test03_parameter():
    if DEBUG:
        pytest.skip()
    finder = ParamValue("percent_std")
    assert finder.selective_index == 0
    res = finder.get_param_optimized_values(PFX_FILE_15, CFL_FILE_15)
    assert finder.nb_leaks == [608, 617, 631, 658, 705, 803, 931, 1303, 2265, 5944]
    assert res == (0.92513860009686011, 0.7, 0.9)


def test04_get_selective_param_value():
    with patch('src.route_leaks_detection.heuristics.detect_route_leaks.ParamValue') as Mocked:
        instance = Mocked.return_value
        instance.get_param_optimized_values.return_value = (0.9, 0, 0)
        finder = FittedFindRouteLeaks(PFX_FILE_15, CFL_FILE_15)
        assert finder.params == {"cfl_peak_min_value": 0,
                                 "pfx_peak_min_value": 0,
                                 "percent_std": 0,
                                 "percent_similarity": 0,
                                 "max_nb_peaks": 0}
