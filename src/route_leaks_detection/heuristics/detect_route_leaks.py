#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2017 ANSSI
# This file is part of the tabi project licensed under the MIT license.

"""
This module uses heuristics to detect fullview leaks.

It uses input data from prepare_data.prepare module for prefixes and conflicts data.
These data are dictionaries with AS numbers as keys and list with one value per day as values.
    ex: {202214: [4, 4, 4, 4, 5, 5]} (if only one week of data)
Prefixes data are {as_number: [number of prefixes announced by the AS per day]}
Conflicts data are {as_number: [number of prefixes announced by the AS per day]}

For each AS, the detection of fullview leaks unfolds in two steps:
- detect significant peaks (independently) for both prefixes and conflicts
- look if amongst the peaks detected, some happened at the same same (same index in data)

entry points:
FindRouteLeaks
FittedFindRouteLeaks (implementation of FindRouteLeaks that determines best values for parameters)

how to use:
finder = FindRouteLeaks(pfx_file, cfl_file, **kwargs)
res = finder.get_route_leaks(**kwargs)
pfx_file and cfl_file are files from prepare_data module
**kwargs are heuristics parameters as defined in FindRouteLeaks.__init__ docstrings

FittedFindRouteLeaks has the same interface.
"""
import abc
from contextlib import closing
from datetime import datetime, timedelta
import json
from multiprocessing import Pool, cpu_count
import os
import sys

import numpy as np
from sklearn.linear_model import LinearRegression
from route_leaks_detection.prepare_data.prepare import LoadRouteLeaksData
from route_leaks_detection.init_logger import logging

LOGGER = logging.getLogger(__name__)

try:
    from deroleru import process_data

    USE_RUST = True
except ImportError:
    USE_RUST = False
    LOGGER.warning("Rust version not found, will be really slow")

MIN_NB_DAYS = 31  # don't try to detect leaks if less than MIN_NB_DAYS days of data
MAX_NB_ZERO_TO_RM = 5  # if more than MAX_NB_ZERO_TO_RM zeros they are not treated as lack of data


class FindPeaks(object):
    """
    Find a peak in data based on the parameters.

    Input data:
    list of numbers representing daily data

    Methodology:
    - find all local maxima value in data
        (in the mathematical sense: value bigger than the previous AND the next values)
    - for each local maximum, check if:
        > it is big enough (param peak_min_value)
        > there are not too many other values close to it (params max_nb_peaks and percent_sim)
        > it is close to the global maximum value (param percent_sim)
        > it impacts significantly the standard deviation (param percent_std)

    Public Method:
    get_big_maxes: find peaks in data and store them in big_maxes

    Public Instance Attribute:
    local_maxes: (property) list of indexes of all maximums in data
    big_maxes: contain indexes of peaks found once get_big_maxes run

    """

    def __init__(self, data, peak_min_value=10, max_nb_peaks=2, percent_similarity=0.9,
                 percent_std=0.9):
        """
        :param data: list of int or float where peaks will be looked for
        :param peak_min_value: int or float
                            peak is bigger than previous and next values from at least min_delta
        :param max_nb_peaks: int
                           if more than max_nb_peaks peaks found, all are discarded
                           (data considered having no peak)
        :param percent_similarity: float (should be <= 1)
                           use to treat two values close enough as the same value
                           peak cannot be smaller that percent_similarity x max value
                           nor have more than max_nb_peaks other values
                           bigger than percent_similarity x its value
        :param percent_std: float (should be <= 1)
                           use to determine if the variation of standard output value is significant
                           the smaller, the more selective ; =1 means no selection
        """
        self.data = data
        self.max_value = max(self.data)
        self.big_maxes = []  # list indexes in data of 'big maxes' (=peaks)

        # peak finding parameters
        self.peak_min_value = peak_min_value
        self.max_nb_peaks = max_nb_peaks
        self.percent_sim = percent_similarity
        self.percent_std = percent_std

        # self.speculate_missing_values()

    def _find_mock_value(self, hint, avg):
        """
        Find what value to use to replace missing value (choose hint if it is not a peak, else avg).
        """
        return hint if abs(hint - avg) < self.peak_min_value / 2 else avg

    def speculate_missing_values(self):
        """
        Replace zero values in data (if less than MAX_NB_ZERO_TO_RM zeros in data).

        The purpose is to smooth data so that when statistics are calculated,
        they are more representative of the data.
        Zero value (if isolated) is a lack of data.

        They are replaced by average between previous and next values
        (or by next/previous value if first/last element)

        If one of the next/previous values is a 'peak', this value is replaced by average
        (calc without zero values) in the calculation of the value that will replace the zero.
        A value is considered a peak if it is more than 'self.peak_min_value' bigger than average.

        :return: Nothing
        """

        # if there are too many zeros it doesn't make sense to try changing them
        if not 0 < self.data.count(0) < MAX_NB_ZERO_TO_RM:
            return

        # calculate average of data without zeros
        data_with_no_zero = [elt for elt in self.data if elt != 0]
        avg = float(sum(data_with_no_zero)) / len(data_with_no_zero)

        # replace zero values
        for i, value in enumerate(self.data):
            if value == 0:

                # manage first index
                if i == 0:
                    next_v = self._find_mock_value(self.data[i + 1], avg)
                    self.data[i] = next_v

                # manage last index
                elif i == len(self.data) - 1:
                    prev_v = self._find_mock_value(self.data[i - 1], avg)
                    self.data[i] = prev_v

                # manage general case
                else:
                    next_v = self._find_mock_value(self.data[i + 1], avg)
                    prev_v = self._find_mock_value(self.data[i - 1], avg)
                    self.data[i] = (prev_v + next_v) / 2

    def get_big_maxes(self):
        """
        Get all points that are "big peaks" according to the parameters and store in self.big_maxes.

        They should:
        - be a local maximum (bigger than the previous and the next values)
        - both variations should be big enough (param peak_min_value)
        - be close to the series max value (param percent_sim)
        - not have too many peaks (param max_nb_peaks)
        - impact significantly standard variation (param percent_std)

        :return: list of peaks indexes
        """
        data_len = len(self.data)
        big_maxes = []
        prev_variation = self.data[1] - self.data[0]

        for i in xrange(1, data_len - 1):
            prev_val = self.data[i - 1]
            cur_val = self.data[i]
            next_val = self.data[i + 1]
            cur_variation = next_val - cur_val
            if (cur_val > prev_val) and (cur_val > next_val):
                if self._is_big_enough(prev_variation, cur_variation) \
                        and self._is_close_to_abs_max(i):
                    big_maxes.append(i)
            prev_variation = cur_variation

        big_maxes = [i for i in big_maxes if self._has_few_enough_peaks(i, big_maxes)]

        if not big_maxes or self._check_std_variation(big_maxes):
            self.big_maxes = big_maxes
        return self.big_maxes

    def _is_big_enough(self, up, down):
        """difference with previous and next values are both bigger than peak_min_value"""
        return (up > self.peak_min_value) and (-down > self.peak_min_value)

    def _has_few_enough_peaks(self, index, local_maxes_indexes):
        """there are less than 'max_nb_peaks' other points with value 'close' to this value"""
        similar_values = [elt for elt in local_maxes_indexes
                          if self.data[elt] >= self.data[index]]
        return len(similar_values) <= self.max_nb_peaks

    def _is_close_to_abs_max(self, index):
        """the value is 'close' to the absolute maximum value"""
        return self.data[index] >= self.percent_sim * self.max_value

    def _check_std_variation(self, indexes_to_check):
        """
        Check if the variation of standard deviation when presumed peak is remove is significant.

        :return: boolean, True if big_maxes are significant (confirmed as peaks), False otherwise
        """
        np_data = np.array(self.data)
        std = np.std(np_data)
        smooth_data = [elt for i, elt in enumerate(self.data) if i not in indexes_to_check]
        np_smooth_data = np.array(smooth_data)
        smooth_std = np.std(np_smooth_data)
        if smooth_std < std * self.percent_std:
            return True
        else:
            return False

    def get_rejection_cause(self, idx):
        """
        Find out why point at index idx in self.data is not considered as a peak.
        """
        prev_val = self.data[idx - 1]
        cur_val = self.data[idx]
        next_val = self.data[idx + 1]

        if not ((cur_val > prev_val) and (cur_val > next_val)):
            return "not a local max", (prev_val, cur_val, next_val)

        if not self._is_big_enough(cur_val - prev_val, next_val - cur_val):
            return "peak_min_value", (prev_val, cur_val, next_val)

        if not self._is_close_to_abs_max(idx):
            return "percent_sim", (cur_val, self.max_value)

        big_maxes = []
        prev_variation = self.data[1] - self.data[0]

        for i in xrange(1, len(self.data) - 1):
            prev_val = self.data[i - 1]
            cur_val = self.data[i]
            next_val = self.data[i + 1]
            cur_variation = next_val - cur_val
            if (cur_val > prev_val) and (cur_val > next_val):
                if self._is_big_enough(prev_variation, cur_variation) \
                        and self._is_close_to_abs_max(i):
                    big_maxes.append(i)
            prev_variation = cur_variation

        if not self._has_few_enough_peaks(idx, big_maxes):
            return "max_nb_peaks", (cur_val, big_maxes, [elt for elt in big_maxes
                                                         if self.data[elt] >= self.data[idx]])

        big_maxes = [i for i in big_maxes if self._has_few_enough_peaks(i, big_maxes)]

        if not self._check_std_variation(big_maxes):
            std = np.std(np.array(self.data))
            smooth_data = [elt for i, elt in enumerate(self.data) if i not in big_maxes]
            smooth_std = np.std(np.array(smooth_data))
            return "percent_std", (std, smooth_std, std / smooth_std, len(smooth_data))

        return "peak detected",

    def get_check_info_by_param(self):
        """
        Get all information on data used to determine whether it is a peak or not.
        """
        data_len = len(self.data)
        big_maxes = []
        variations = [0] * (data_len - 1)
        i = 1
        variations[0] = abs(self.data[1] - self.data[0])
        res = {}
        while i < data_len - 1:
            cur_val = self.data[i]
            prev_val = self.data[i - 1]
            next_val = self.data[i + 1]
            variations[i] = abs(next_val - cur_val)
            if (cur_val > prev_val) and (cur_val > next_val):
                res[i] = {}
                variation_up = variations[i - 1]
                variation_down = variations[i]
                res[i]["peak_min_value"] = [variation_up, variation_down]
                res[i]["percent_sim"] = [self.data[i], self.percent_sim * self.max_value]
                if self._is_big_enough(variation_up, -variation_down):
                    if self._is_close_to_abs_max(i):
                        big_maxes.append(i)
            i += 1
        res["variations"] = variations

        res["big_maxes"] = []
        for i in big_maxes:
            similar_values = [elt for elt in big_maxes
                              if self.data[elt] >= self.data[i]]
            res[i]["max_nb_peaks"] = similar_values
            if len(similar_values) <= self.max_nb_peaks:
                res["big_maxes"].append(i)

        res["first_big_maxes"] = big_maxes

        smooth_data = [elt for i, elt in enumerate(self.data) if i not in res["big_maxes"]]
        res["percent_std"] = [np.std(np.array(smooth_data)), np.std(np.array(self.data))]
        res["smooth_values"] = smooth_data

        res["absolute_max"] = self.max_value

        if not big_maxes or self._check_std_variation(big_maxes):
            res["leaks_detected"] = big_maxes
        else:
            res["leaks_detected"] = []

        return res


class FindRouteLeaks(object):
    """
    Find route leaks with heuristics.

    Heuristics are based on the definition of a route leak as:
    "simultaneous raise
    of the number of prefixes announced
    and the number of ASes in conflict with those announcements
    during a short period of time"

    public method (entry point):
    get_route_leaks (abstract method defined in _RuFindRouteLeaks and _PyFindRouteLeaks)
      detect fullview leaks based on prefixes AND conflicts files from prepare_data

    public methods (helpers):
    get_rejection_cause
      explain why a point in data has not been considered as a route leak
    get_check_info_by_param
      get every data used to decide whether a route leak occurred in data or not

    public properties:
    pfx_data - dictionary - prefixes data loaded
    cfl_data - dictionary - conflicts data loaded
    pfx_peaks - dictionary - peaks found for prefixes (dict {asn: [peaks found]})
    cfl_peaks - dictionary - peaks found for conflicts (dict {asn: [peaks found]})
    """

    def __init__(self, pfx_file, cfl_file, pfx_peak_min_value=10, cfl_peak_min_value=5,
                 max_nb_peaks=2, percent_similarity=0.9, percent_std=0.9,
                 data_already_processed=True, start_date=None, end_date=None):
        """
        Instantiate self.finder - see _BaseFindRouteLeaks doc for more details.
        """
        self.finder = _BaseFindRouteLeaks(pfx_file, cfl_file, pfx_peak_min_value=pfx_peak_min_value,
                                          cfl_peak_min_value=cfl_peak_min_value,
                                          max_nb_peaks=max_nb_peaks,
                                          percent_similarity=percent_similarity,
                                          percent_std=percent_std,
                                          data_already_processed=data_already_processed,
                                          start_date=start_date, end_date=end_date)

    def get_route_leaks(self, **kwargs):
        """
        Find route leaks (synchronized significant peaks in both self.pfx_data and self.cfl_data).

        :param **kwargs: peak finding parameters
                         keys should be in pfx_peak_min_value, cfl_peak_min_value, max_nb_peaks,
                                           percent_similarity, percent_std
                         values should be int of floats

        :return: dict {asn: {"leaks": [days_with_route_leaks],
                             "pfx_data": [nb_of_prefixes_announced_by_asn_by_day],
                             "cfl_data": [nb_of_ases_in_conflicts_with_asn_by_day]}}
        """
        return self.finder.get_route_leaks(**kwargs)

    def get_rejection_cause(self, asn, idx):
        """
        Find out why point of index idx of asn data is not considered as a route leak.
        """
        return self.finder.get_rejection_cause(asn, idx)

    def get_check_info_by_param(self, asn):
        """
        Get all information used to determine whether asn data contains route leak or not.
        """
        return self.finder.get_check_info_by_param(asn)

    @property
    def pfx_data(self):
        """Prefixes time series."""
        return self.finder.pfx_data

    @property
    def cfl_data(self):
        """Conflicts time series."""
        return self.finder.cfl_data

    @property
    def params(self):
        """Parameters used by self.finder to detect route leaks by applying heuristics."""
        return self.finder._params


class FittedFindRouteLeaks(FindRouteLeaks):
    """
    Improved version of FindRouteLeaks using ParamValue to automatically get best parameters values.
    """

    def __init__(self, pfx_file, cfl_file, data_already_processed=True,
                 start_date=None, end_date=None, **params):
        """
        Instantiate as FindRouteLeaks and calculate params value if not given.
        """
        super(FittedFindRouteLeaks, self).__init__(pfx_file, cfl_file,
                                                   data_already_processed=data_already_processed,
                                                   start_date=start_date, end_date=end_date)
        _params = {}
        self.lr_data = {}
        for param in ["pfx_peak_min_value", "cfl_peak_min_value", "max_nb_peaks",
                      "percent_similarity", "percent_std"]:
            ipt_val = params.get(param, None)
            if ipt_val is not None:
                _params[param] = ipt_val
            else:
                _params[param] = self.get_best_param_value(param)
        self.finder._params = _params

    def get_best_param_value(self, param):
        """
        Find optimized (selective) value for param using the linear regression method.

        Saves linear regression infos in self.lr_data
        """
        param_selector = ParamValue(param)
        lr_res = param_selector.get_param_optimized_values(self.pfx_data, self.cfl_data)
        if lr_res[0] < 0.75:
            LOGGER.warning("Low Linear Regression score (%s) for param %s" % (lr_res[0], param))
        self.lr_data[param] = {"res": lr_res, "series": param_selector.nb_leaks}
        if param == "percent_std":
            return lr_res[-1]
        return lr_res[param_selector.selective_index + 1]


class _BaseFindRouteLeaks(object):
    """
    Base class - find route leaks with heuristics.

    This is a Factory that instantiate _RuFindRouteLeaks if rust implementation is available,
    _PyFindRouteLeaks otherwise.
    """

    __metaclass__ = abc.ABCMeta

    def __new__(cls, *args, **kwargs):
        """
        Instantiate _RuFindRouteLeaks (if rust implementation available) or _PyFindRouteLeaks.
        """
        if USE_RUST:
            obj = object.__new__(_RuFindRouteLeaks)
        else:
            obj = object.__new__(_PyFindRouteLeaks)
        return obj

    def __init__(self, pfx_file, cfl_file, pfx_peak_min_value=10, cfl_peak_min_value=5,
                 max_nb_peaks=2, percent_similarity=0.9, percent_std=0.9,
                 data_already_processed=True, start_date=None, end_date=None):
        """
        Load data from pfx_file and cfl_file using LoadRouteLeaksData.

        :param pfx_file: full path of file with prefixes data
        :param cfl_file: full path of file with conflicts data
        :param x_peak_min_value: int or float,
                            peak must be bigger than previous & next values from at least min_delta
        :param max_nb_peaks: int, if more than max_nb_peaks peaks found, all are discarded
        :param percent_similarity: float (should be <= 1) use to filter only biggest values
                            peak cannot be smaller that percent_similarity x max value
                            nor have more than max_nb_peaks other values
                                bigger than percent_similarity x its value
        :param percent_std: float (should be <= 1)
                            use to determine if variation of standard output value is significant
                            the smaller, the more selective ; =1 means no selection
        :param data_already_processed: boolean
                    True if data in pfx/cfl_file are already processed (with prepare_data.prepare)
        :param start/end_date: str date - used to filter data to load if is not prepared yet
                            (ie: if pfx/cfl_file is directory containing raw files)
        """
        # peak finding parameters
        self._params = {"pfx_peak_min_value": float(pfx_peak_min_value),
                        "cfl_peak_min_value": float(cfl_peak_min_value),
                        "max_nb_peaks": float(max_nb_peaks),
                        "percent_similarity": float(percent_similarity),
                        "percent_std": float(percent_std)}

        # Load data
        loader_pfx = LoadRouteLeaksData(pfx_file, "pfx",
                                        data_already_processed=data_already_processed)
        loader_cfl = LoadRouteLeaksData(cfl_file, "cfl",
                                        data_already_processed=data_already_processed)

        self.pfx_data = loader_pfx.load_data(start=start_date, end=end_date)
        self.cfl_data = loader_cfl.load_data(start=start_date, end=end_date)

        if loader_pfx.start != loader_cfl.start:
            raise ValueError("pfx_filename and cfl_filename arguments "
                             "don't have the same start date "
                             "(pfx_file: %s and cfl_file: %s)"
                             % (loader_pfx.start, loader_cfl.start))
        if start_date and ((loader_pfx.str_start or start_date) != start_date):
            LOGGER.warning("start_date %s passed as argument "
                           "doesn't match start date %s from data (in pfx_file)",
                           start_date, loader_pfx.str_start)

        # init attributes
        self.format_date = "%Y-%m-%d"
        self._start = self.start = loader_pfx.start or start_date

    # will be overridden by self._map_leaks_indexes_to_date if self.start is not None
    _map_leaks_indexes = lambda x: x

    def _map_leaks_indexes_to_dates(self, leaks_indexes):
        """
        :return list of string dates when leaks have been detected
        """
        return [(datetime.strptime(self.start, self.format_date)
                 + timedelta(i)).strftime(self.format_date)
                for i in leaks_indexes]

    @property
    def start(self):
        """
        :return: str date self._start - first day of data treated
        """
        return self._start

    @start.setter
    def start(self, value):
        """
        set self._start and define self._map_leaks_indexes accordingly
        """
        if value:
            self._map_leaks_indexes = self._map_leaks_indexes_to_dates
        else:
            self._map_leaks_indexes = lambda x: x
        if isinstance(value, datetime):
            value = value.strftime(self.format_date)
        self._start = value

    @abc.abstractmethod
    def get_route_leaks(self, **kwargs):
        """
        Find route leaks (synchronized significant peaks in both self.pfx_data and self.cfl_data).

        :param **kwargs: peak finding parameters
                         keys should be in pfx_peak_min_value, cfl_peak_min_value, max_nb_peaks,
                                           percent_similarity, percent_std
                         values should be int of floats

        :return: dict {asn: {"leaks": [days_with_route_leaks],
                             "pfx_data": [nb_of_prefixes_announced_by_asn_by_day],
                             "cfl_data": [nb_of_ases_in_conflicts_with_asn_by_day]}}
        """
        pass

    def get_rejection_cause(self, asn, idx):
        """
        Find out why point of index idx of asn data is not considered as a route leak.
        """
        params = self._params.copy()
        pfx_min_val = params.pop("pfx_peak_min_value")
        cfl_min_val = params.pop("cfl_peak_min_value")

        not_found = "AS %s not found in data" % asn

        params["peak_min_value"] = pfx_min_val
        pfx_cause = FindPeaks(self.pfx_data[asn], **params).get_rejection_cause(idx) \
            if asn in self.pfx_data else not_found

        params["peak_min_value"] = cfl_min_val
        cfl_cause = FindPeaks(self.cfl_data[asn], **params).get_rejection_cause(idx) \
            if asn in self.cfl_data else not_found

        return {"prefixes_rejection_cause": pfx_cause,
                "conflicts_rejection_cause": cfl_cause}

    def get_check_info_by_param(self, asn):
        """
        Get all information used to determine whether asn data contains route leak or not.
        """
        params = self._params.copy()
        pfx_min_val = params.pop("pfx_peak_min_value")
        cfl_min_val = params.pop("cfl_peak_min_value")

        not_found = "AS %s not found in data" % asn

        params["peak_min_value"] = pfx_min_val
        pfx_info = FindPeaks(self.pfx_data[asn], **params).get_check_info_by_param() \
            if asn in self.pfx_data else not_found

        params["peak_min_value"] = cfl_min_val
        cfl_info = FindPeaks(self.cfl_data[asn], **params).get_check_info_by_param() \
            if asn in self.cfl_data else not_found

        return {"prefixes_info": pfx_info,
                "conflicts_info": cfl_info}


class _PyFindRouteLeaks(_BaseFindRouteLeaks):
    """
    Implementation of FindRouteLeaks using pure python (FindPeaks class for detection).
    """

    def __init__(self, pfx_file, cfl_file, start_date=None, end_date=None, **kwargs):
        super(_PyFindRouteLeaks, self).__init__(pfx_file, cfl_file, start_date=start_date,
                                                end_date=end_date, **kwargs)

        # manage duplicates (ases having the same data series)

        self._pfx_dupl_ases = {}
        self._cfl_dupl_ases = {}
        self._pfx_unique_data = self.pfx_data.copy()
        self._cfl_unique_data = self.cfl_data.copy()

        self._fill_duplicates_struct(self._pfx_dupl_ases, self._pfx_unique_data)
        self._fill_duplicates_struct(self._cfl_dupl_ases, self._cfl_unique_data)

    def _fill_duplicates_struct(self, dupl_ases, data):
        """
        Tool to _rm_duplicates.
        """
        reversed_data = {}
        for asn in data.keys():
            series = tuple(data[asn])
            if series in reversed_data:
                base_asn = reversed_data[series]
                del data[asn]
                dupl_ases[base_asn] = dupl_ases.get(base_asn, [])
                dupl_ases[base_asn].append(asn)
            else:
                reversed_data[series] = asn

    def get_route_leaks(self, **kwargs):
        """
        Abstract method implementation using FindPeaks.
        """
        # don't run detection if too few data
        if len(self.pfx_data.values()[0]) < MIN_NB_DAYS:
            return {}

        # change self.params values if needed
        for param_name, param_value in kwargs.iteritems():
            if param_name in self._params:
                self._params[param_name] = float(param_value)
            else:
                LOGGER.warning("unknown parameter %s", param_name)

        pfx_peaks, cfl_peaks = self.find_pfx_n_cfl_peaks()
        self._put_duplicates_back_to_peaks(pfx_peaks, cfl_peaks)

        route_leaks = {}
        for asn in cfl_peaks:
            if asn in pfx_peaks:
                leaks = list(set(cfl_peaks[asn]) & set(pfx_peaks[asn]))
                if leaks:
                    route_leaks[asn] = {"leaks": self._map_leaks_indexes(leaks),
                                        "pfx_data": self.pfx_data[asn],
                                        "cfl_data": self.cfl_data[asn]}

        return route_leaks

    def find_pfx_n_cfl_peaks(self):
        """
        Fill self.pfx_peaks and self.cfl_peaks attributes.

        :return: nothing
        """
        params = self._params.copy()

        del params["pfx_peak_min_value"]
        del params["cfl_peak_min_value"]

        params["peak_min_value"] = self._params["pfx_peak_min_value"]
        pfx_peaks = self._get_ases_with_peak(self._pfx_unique_data, **params)

        params["peak_min_value"] = self._params["cfl_peak_min_value"]
        cfl_peaks = self._get_ases_with_peak(self._cfl_unique_data, **params)

        return pfx_peaks, cfl_peaks

    def _get_ases_with_peak(self, plotable_dict, **params):
        """
        Treat plotable_dict to store ASes with peaks in "peaks" argument.

        :param plotable_dict: dict from LoadRouteLeaksData.get_input_data function
                            {asn: list}
                            list indexes represent days (1st day is index 0, ...)
                            list elements represent the number prefixes / conflicts for day
        :param peaks: empty dict that will store peaks found
        :param params: kwarg that can be used to modify the value of FindPeak parameters
        :return: dict {asn: [peaks found]}
        """
        peaks = {}
        for asn in plotable_dict:
            peak_finder = FindPeaks(plotable_dict[asn], **params)
            if peak_finder.max_value >= peak_finder.peak_min_value \
                    and peak_finder.get_big_maxes():
                peaks[asn] = peak_finder.big_maxes

        return peaks

    def _put_duplicates_back_to_peaks(self, pfx_peaks, cfl_peaks):
        """
        Put back to results the duplicate series removed during __init__.
        """
        for asn in pfx_peaks.keys():
            for dupl_asn in self._pfx_dupl_ases.get(asn, []):
                pfx_peaks[dupl_asn] = pfx_peaks[asn]
        for asn in cfl_peaks.keys():
            for dupl_asn in self._cfl_dupl_ases.get(asn, []):
                cfl_peaks[dupl_asn] = cfl_peaks[asn]


class _RuFindRouteLeaks(_BaseFindRouteLeaks):
    """
    Implementation of FindRouteLeaks using Rust for detection (deroleru project).
    """

    def __init__(self, pfx_file, cfl_file, start_date=None, end_date=None, **kwargs):
        super(_RuFindRouteLeaks, self).__init__(pfx_file, cfl_file, start_date=start_date,
                                                end_date=end_date, **kwargs)
        self._aggregated_data = self._aggregate_data_for_rust()

    def _aggregate_data_for_rust(self):
        """
        Create data structure used by rust program and assign to self._aggregated_data

        Aggregates ASes that have the same prefixes and the same conflicts
        so calculation is done only once.
        {(ases): [prefixes_list, conflicts_list]}
        """
        rev_aggr_data = {}
        for asn in set(self.pfx_data) & set(self.cfl_data):
            prefixes = tuple(self.pfx_data[asn])
            conflicts = tuple(self.cfl_data[asn])
            key = (prefixes, conflicts)
            rev_aggr_data[key] = rev_aggr_data.get(key, [])
            rev_aggr_data[key].append(asn)
        aggr_data = {}
        for values, ases in rev_aggr_data.iteritems():
            aggr_data[tuple(ases)] = [list(elt) for elt in values]
        return aggr_data

    def get_route_leaks(self, **kwargs):
        """
        Abstract method implementation using Rust implementation (deroleru).
        """
        # don't run detection if too few data
        if len(self.pfx_data.values()[0]) < MIN_NB_DAYS:
            return {}

        # change self.params values if needed
        for param_name, param_value in kwargs.iteritems():
            if param_name in self._params:
                self._params[param_name] = float(param_value)
            else:
                LOGGER.warning("unknown parameter %s", param_name)

        # run detection & create result
        route_leaks = {}
        for ases, values in self._aggregated_data.iteritems():
            detection_res = self._call_rust_leak_detection(values[0], values[1], self._params)
            if detection_res:
                details = {"leaks": self._map_leaks_indexes(detection_res),
                           "pfx_data": list(values[0]), "cfl_data": list(values[1])}
                for asn in ases:
                    route_leaks[asn] = details

        return route_leaks

    def _call_rust_leak_detection(self, pfx_data, cfl_data, params):
        """
        Use rust binding to detect route leaks in given data, using params.

        Warning: type is important here (casted in the function for params but not for data).

        :param pfx_data: list of integers (prefixes time series)
        :param cfl_data: list of integers (conflicts time series)
        :param params: dict
        :return: list of indexes where leaks have been detected
        """
        params_list = []
        for param_name in ["pfx_peak_min_value", "cfl_peak_min_value",
                           "percent_similarity", "max_nb_peaks", "percent_std"]:
            if "percent" in param_name:
                param = float(params[param_name])
            else:
                param = int(params[param_name])
            params_list.append(param)
        return process_data(pfx_data, cfl_data, *params_list)


def _detect(param, idx):
    """
    Tool for multiprocessing Pool in ParamValue.calc_nb_leaks.
    """
    return idx, len(ParamValue.leaks_finder.get_route_leaks(**param))


def _detect_wrapper(args):
    """
    Tool for multiprocessing Pool in ParamValue.calc_nb_leaks.
    """
    return _detect(*args)


class ParamValue(object):
    """
    Factory: instantiates one of _Param* classes, representing param of leak detection algorithm.

    Its purpose is to find the best value for the given parameter.

    Classes that can be instantiated are ParamValue descendant classes:
    _ParamPfxMinValue
    _ParamCflMinValue
    _ParamSimToMax
    _ParamNbBigger
    _ParamStd

    Route leaks detection in run for different values of the given parameter.
    This gives a series that will be approximated with three consecutive linear regressions.
    This defines three different parts of the curve and two breaking points.
    Those points are two candidate for the parameter's value.

    class attributes:
    neutral_params: parameters values that have no impact on the detection (don't filter any case)
    lr_points: for each parameter, the values that will be used to calculate the detection
                (abscissa of the curve that will be approximated with linear regression)
    selective_index: to be set in subclasses - defines which selected point is the most selective
                    (0: first is the most selective, 1: second is the most selective)
    leak_finder: instance of FindRouteLeaks (or FastFindRouteLeaks)

    Public methods:
    get_lr_result: entry point - run the linear regressions algorithm and finds the breaking points
    calc_nb_leaks: tool - run route leak detection for all points in lr_points of parameter
    """
    __metaclass__ = abc.ABCMeta

    neutral_params = {"cfl_peak_min_value": 0,
                      "pfx_peak_min_value": 0,
                      "percent_std": 2,
                      "percent_similarity": 0,
                      "max_nb_peaks": 400}

    lr_points = {"cfl_peak_min_value": range(50),
                 "pfx_peak_min_value": range(50),
                 "percent_std": [float(elt) / 10 for elt in range(1, 11)],
                 "percent_similarity": [float(elt) / 10 for elt in range(1, 11)],
                 "max_nb_peaks": range(1, 51)}

    leaks_finder = None
    _pfx_ipt = None
    _cfl_ipt = None

    def __new__(cls, param_name):
        """
        Instantiate one of subclasses (param_name matches _param_name subclass attribute)
        """
        for sub in cls.__subclasses__():
            if sub._param_name == param_name:
                o = object.__new__(sub)
                return o
        raise ValueError("Parameter %s does not exist, should be one of %s"
                         % (param_name, ["pfx_peak_min_value", "cfl_peak_min_value",
                                         "max_nb_peaks", "percent_similarity", "percent_std"]))

    def __init__(self, param_name):
        """
        :param param_name: name of the parameter to treat, one of:
            pfx_peak_min_value, cfl_peak_min_value, percent_similarity, max_nb_peaks, percent_std
        """
        self.nb_leaks = []

    def get_param_elt(self, elt_name):
        return getattr(self, elt_name)[self._param_name]

    @property
    def lin_reg_pts(self):
        return self.get_param_elt("lr_points")

    def calc_nb_leaks(self):
        """
        Calculate detection results for each point of lr_points for instantiated parameter.

        Multiprocessing Pool is used to distribute calculations.

        Result is stored in self.nb_leaks

        :return: nothing
        """
        self.nb_leaks = [0] * len(self.lin_reg_pts)
        ParamValue.leaks_finder.finder._params = self.neutral_params.copy()

        with closing(Pool(processes=cpu_count() / 2 or 1)) as pool:
            for res in pool.imap_unordered(_detect_wrapper,
                                           [({self._param_name: v}, i)
                                            for i, v in enumerate(self.lin_reg_pts)]):
                self.nb_leaks[res[0]] = res[1]

    def _get_lr_score(self, l_bound, u_bound):
        """
        Calculate linear regression score for self.nb_leaks between l_bound and u_bound.
        """

        shape = u_bound - l_bound + 1
        lin_reg_pts = self.lin_reg_pts[l_bound - 1:u_bound]
        nb_leaks_pts = self.nb_leaks[l_bound - 1:u_bound]

        lin_reg = LinearRegression()

        lin_reg.fit(np.array(lin_reg_pts).reshape((shape, 1)),
                    np.array(nb_leaks_pts).reshape((shape, 1)))

        return lin_reg.score(np.array(lin_reg_pts).reshape((shape, 1)),
                             np.array(nb_leaks_pts).reshape((shape, 1)))

    def _get_3lr_res(self):
        """
        Approximate self.nb_leaks  with all possible combinations of three straight lines.
        """
        i0 = 1
        i3 = len(self.lin_reg_pts)

        res = []

        for i1 in range(i0 + 2, i3):
            for i2 in range(i1 + 2, i3):
                score1 = self._get_lr_score(i0, i1)
                score2 = self._get_lr_score(i1, i2)
                score3 = self._get_lr_score(i2, i3)

                res.append(((score1 + score2 + score3) / 3, i1, i2))
        return res

    def _get_2lr_res(self):
        """
        Approximate self.nb_leaks  with all possible combinations of two straight lines.
        """
        i0 = 1  # in _get_lr_score, l_bound - 1 is used
        i2 = len(self.lin_reg_pts)

        res = []

        for i1 in range(i0 + 2, i2):
            score1 = self._get_lr_score(i0, i1)
            score2 = self._get_lr_score(i1, i2)

            res.append(((score1 + score2) / 2, i1))
        return res

    def get_param_optimized_values(self, pfx_data=None, cfl_data=None):
        """
        Find the two values advised for this parameter.

        See class doc for more details.

        If Parameter.leak_finder is not defined, pfx_data and cfl_data arguments are mandatory.
        Otherwise, they can be given to create a new leak_finder.

        :param pfx_data: see FindRouteLeaks doc
        :param cfl_data: see FindRouteLeaks doc
        :return: (linear_regressions_score, first_breaking_point_value, second_breaking_point_value)
        """
        if ParamValue.leaks_finder is None \
                or ParamValue._pfx_ipt != pfx_data or ParamValue._cfl_ipt != cfl_data:
            ParamValue.leaks_finder = FindRouteLeaks(pfx_data, cfl_data)
            ParamValue._pfx_ipt = pfx_data
            ParamValue._cfl_ipt = cfl_data

        self.calc_nb_leaks()

        res = self._get_3lr_res()
        best_case = max(res, key=lambda x: x[0])
        return (best_case[0],
                self._map_lr_value_to_real_value(best_case[1]),
                self._map_lr_value_to_real_value(best_case[2]))

    def _map_lr_value_to_real_value(self, lr_value):
        """
        Get param actual value from index of linear regression.
        """
        return self.lin_reg_pts[lr_value - 1]

    def _map_real_value_to_lr_value(self, real_value):
        """
        Get param breaking index for linear regression for breaking point value.
        """
        return self.lin_reg_pts.index(real_value) + 1

    @abc.abstractproperty
    def selective_index(self):
        return


class _ParamPfxMinValue(ParamValue):
    _param_name = "pfx_peak_min_value"
    selective_index = 1


class _ParamCflMinValue(ParamValue):
    _param_name = "cfl_peak_min_value"
    selective_index = 1


class _ParamSimToMax(ParamValue):
    _param_name = "percent_similarity"
    selective_index = 1


class _ParamNbBigger(ParamValue):
    _param_name = "max_nb_peaks"
    selective_index = 0


class _ParamStd(ParamValue):
    _param_name = "percent_std"
    selective_index = 0


def main(args):
    """
    Use FindRouteLeaks and print leaks detected on stdout or save to file.
    """
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("pfx_file",
                        help="full path of file with prefixes data - "
                             "from prepare_data.prepare module")
    parser.add_argument("cfl_file",
                        help="full path of file with conflicts data - "
                             "from prepare_data.prepare module")
    parser.add_argument("--out", default=None,
                        help="full path of file where results will be saved - default stdout")
    parser.add_argument("--pfx_peak_min_value", default=None, help="heuristics parameter")
    parser.add_argument("--cfl_peak_min_value", default=None, help="heuristics parameter")
    parser.add_argument("--max_nb_peaks", default=None, help="heuristics parameter")
    parser.add_argument("--percent_similarity", default=None, help="heuristics parameter")
    parser.add_argument("--percent_std", default=None, help="heuristics parameter")
    parser.add_argument("--fit_params", action="store_true", default=False,
                        help="if specified, best params will be calculated (when not given) before "
                             "running the detection.")

    args = parser.parse_args(args)

    if not os.path.isfile(args.pfx_file):
        raise AssertionError("Prefixes file %s not found", args.pfx_file)
    if not os.path.isfile(args.cfl_file):
        raise AssertionError("Conflicts file %s not found", args.cfl_file)

    params = {}
    if args.pfx_peak_min_value is not None:
        params["pfx_peak_min_value"] = float(args.pfx_peak_min_value)
    if args.cfl_peak_min_value is not None:
        params["cfl_peak_min_value"] = float(args.cfl_peak_min_value)
    if args.max_nb_peaks is not None:
        params["max_nb_peaks"] = float(args.max_nb_peaks)
    if args.percent_similarity is not None:
        params["percent_similarity"] = float(args.percent_similarity)
    if args.percent_std is not None:
        params["percent_std"] = float(args.percent_std)

    params["data_already_processed"] = True
    if args.fit_params:
        finder = FittedFindRouteLeaks(args.pfx_file, args.cfl_file, **params)
    else:
        finder = FindRouteLeaks(args.pfx_file, args.cfl_file, **params)

    leaks = finder.get_route_leaks()

    if args.out:
        with open(args.out, "w") as f:
            f.write(json.dumps(leaks))
    else:
        for elt in leaks:
            print json.dumps({elt: leaks[elt]["leaks"]})


if __name__ == "__main__":
    main(sys.argv[1:])
