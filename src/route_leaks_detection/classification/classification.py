#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2017 ANSSI
# This file is part of the tabi project licensed under the MIT license.

"""
This module intends to detect alleged fullview leaks using Machine Learning (classification)

entry point : ApplyModel
uses already learnt svm model (classification/data/svm_model.p)

how to use:
classifier = ApplyModel()
classifier.get_model_svm_classifier()
res = classifier.get_classification_result(pfx_file, cfl_file)
pfx_file and cfl_file are files from prepare_data module
"""

from collections import defaultdict
import csv
import gzip
import json
import numbers
import os
import sys

import cPickle
import numpy as np
from sklearn import svm, grid_search
from route_leaks_detection.heuristics.detect_route_leaks import LoadRouteLeaksData, FindPeaks


class CreateClassifBaseData(object):
    """
    Load data and calculate variation and normalized variation data points.

    variation data point is a list of differences between each value and its next value.

    Uses LoadRouteLeaksData and FindPeaks from algo.detect_route_leaks module

    Public Methods:
    get_input_data load data from filename into self.raw_loaded_data
    create_var_input calculate variation of self.raw_loaded_data into self.var_data
    create_normalized_var_input calculate variation of raw_loaded_data into self.normalize_var_data

    Public Attributes:
    raw_loaded_data {asn: [list_of_int_representing_daily_values]}
    var_data {asn: [list_of_int_representing_variation_of_daily_values]}
    normalize_var_data {asn: [list_of_int_representing_normalized_variation_of_daily_values]}
    """

    def __init__(self, filename, pfx_or_cfl="", ft_open=gzip.open, data_already_processed=False):
        """
        :param filename: name of prefixes or conflicts file (one time series per AS)
        :param pfx_or_cfl: string "pfx" or "cfl" depending of filename type
        :param ft_open: function to use to open filename
        :param data_already_processed: boolean
                    True if data in filename are already processed (output of create_plot_input)
                    In this case, get_input_data will only return dict containing data in file
        """
        self._data_loader = LoadRouteLeaksData(filename, pfx_or_cfl, ft_open,
                                               data_already_processed)
        self.raw_loaded_data = None
        self.var_data = defaultdict(list)
        self.normalized_var_data = defaultdict(list)

    def get_input_data(self):
        """
        Store data from self.filename into self.raw_loaded_data with zero values smoothed out.

        Zero values are replaced by the average between the previous and next values
        unless there are more than 5 zero values.

        :return: nothing
        """
        self.raw_loaded_data = self._data_loader.load_data()
        for asn in self.raw_loaded_data:
            finder = FindPeaks(self.raw_loaded_data[asn], peak_min_value=10)
            finder.speculate_missing_values()

    def create_var_input(self):
        """
        Store variation data points into self.var_data

        :return: nothing
        """
        if not self.raw_loaded_data:
            self.get_input_data()

        for asn in self.raw_loaded_data.keys():
            for i in range(len(self.raw_loaded_data[asn]) - 1):
                self.var_data[asn].append(self.raw_loaded_data[asn][i + 1]
                                          - self.raw_loaded_data[asn][i])

    def create_normalized_var_input(self):
        """
        Store normalized variation data points into self.var_data

        :return: nothing
        """

        if not self.var_data:
            self.create_var_input()

        for asn in self.var_data.keys():
            max_value = max([abs(elt) for elt in self.var_data[asn]])
            if max_value != 0:
                for var in self.var_data[asn]:
                    self.normalized_var_data[asn].append(float(var) / max_value)
            else:
                self.normalized_var_data[asn] = [0] * len(self.var_data[asn])


class AttributeMakers(object):
    """
    Register all functions to be used to create attributes.

    Public Method:
    create_attributes run each attribute maker function on data

    Note : some function create several attributes

    Class attribute:
    attribute_makers dict {"bilat": [], "unilat": []}
    contains all functions registered classified between 'bilat' and 'unilat'
    bilat: function will be run twice, with different arguments
                (purpose is to switch prefixes and conflicts infos)
    unilat: function will be run only once
                (purpose is to use it with correlation infos)

    Attributes:
    max_var for both prefixes and conflicts
    next_var for both prefixes and conflicts
    max_other_var for both prefixes and conflicts
    next_other_var for both prefixes and conflicts
    nb_maxes both for prefixes and conflicts
    std_ratio for prefixes, conflicts and correlation
    other_std_ratio for prefixes, conflicts and correlation
    last_decile for prefixes, conflicts and correlation
    last_decile_spread for prefixes, conflicts and correlation
    last_quartile for prefixes, conflicts and correlation
    last_quartile_spread for prefixes, conflicts and correlation
    percent_above_average for prefixes, conflicts and correlation
    corr_max_var
    corr_max_next
    """
    attribute_makers = {"bilat": [], "unilat": []}
    _makers_names = []

    def _load_arguments(self, arguments, *args):
        """
        Tool to find argument by name in **kwarg (or any dict).

        :param arguments: dict
        :param args: keys to find in dict
        :return: list of values matching keys
        """
        try:
            arg_values = [arguments[arg_name] for arg_name in args]
        except KeyError:
            raise ValueError("Argument %s listed in param args doesn't exists in param arguments %s"
                             % (arg_name, arguments))
        return arg_values

    def load_makers(self):
        """
        Use decorator to fill class attribute attribute_makers with all functions decorated.
        """

        def register_attr(*args):
            """
            Decorator to register attribute makers - get decorator argument.

            :param arg: 'bilat' or 'unilat'
                        key in cls.attribute_makers where function will be stored
            """

            def register_decorator(func):
                """
                Do the recording into cls.attribute_makers if not recorded yet.
                """
                if func.__name__ not in AttributeMakers._makers_names:
                    for arg in args:
                        AttributeMakers.attribute_makers[arg].append(func)
                    AttributeMakers._makers_names.append(func.__name__)

                return func

            return register_decorator

        @register_attr("unilat")
        def corr_max_next(**kwargs):
            """
            :param corr: list - correlation between prefixes and conflicts normalized variation data
            :param max_indexes: list - indexes of corr maximum values
            :return int - biggest value after a maximum value for correlation data
                            (or zero if no max value)
            """
            corr, max_indexes = self._load_arguments(kwargs, "corr", "max_indexes")
            next_maxes = [corr[i + 1] for i in max_indexes if i + 1 < len(corr)]
            return max(next_maxes) if next_maxes else 0,

        @register_attr("unilat")
        def corr_nb_maxes(**kwargs):
            """
            :param pfx_var_norm_data: list - normalized variation data for prefixes
            :param max_indexes: list - indexes of correlation maximum values
            :return float - ratio: occurrences of correlation max values compared to length of data
            """
            pfx_var_norm_data, max_indexes = self._load_arguments(kwargs, "pfx_var_norm_data",
                                                                  "max_indexes")
            return float(len(max_indexes)) / len(pfx_var_norm_data),

        @register_attr("unilat")
        def corr_max_value(**kwargs):
            """
            :param value_corr: list - correlation between prefixes and conflicts variation data
            :return float - log of maximum value of value_corr (zero if max value not positive)
            """
            value_corr, = self._load_arguments(kwargs, "value_corr")
            return np.log(max(value_corr)) if max(value_corr) > 0 else 0,

        @register_attr("bilat")
        def max_var(**kwargs):
            """
            :param norm_var: prefixes or conflicts normalized variation data
            :param max_index: int - index of maximum value of prefixes or conflicts raw data
                            if several, biggest value of the other (conflicts or prefixes) is chosen
            :return int - biggest value of prefixes / conflicts normalized variation data
                            corresponding to biggest value of conflicts / prefixes variation
            """
            norm_var, max_index = self._load_arguments(kwargs, "norm_var", "max_index")
            return norm_var[max_index - 1],

        @register_attr("bilat")
        def next_var(**kwargs):
            """
            :param norm_var: prefixes or conflicts normalized variation data
            :param max_index: int - index of maximum value of prefixes or conflicts raw data
                            if several, biggest value of the other (conflicts or prefixes) is chosen
            :return int - value of normalized variation prefixes of conflicts data after the biggest
                            (see max_var for more precise definition of the biggest)
            """
            norm_var, max_index = self._load_arguments(kwargs, "norm_var", "max_index")
            return norm_var[max_index],

        @register_attr("bilat")
        def max_other_var(**kwargs):
            """
            Same as max_var exchanging prefixes and conflicts data.
            (max_index param still refers to the same as for max_var)
            """
            max_index, other_norm_var = self._load_arguments(kwargs, "max_index", "other_norm_var")
            return other_norm_var[max_index - 1],

        @register_attr("bilat")
        def next_other_var(**kwargs):
            """
            Same as next_var exchanging prefixes and conflicts data.
            (max_index param still refers to the same as for next_var)
            """
            max_index, other_norm_var = self._load_arguments(kwargs, "max_index", "other_norm_var")
            return other_norm_var[max_index],

        @register_attr("bilat")
        def nb_maxes(**kwargs):
            """
            :param raw_data: list - prefixes /conflicts raw data (not variation)
            :param max_indexes: list - indexes of prefixes /conflicts maximum values
            :return float - ratio: occurrences of max values compared to length of data
            """
            raw_data, max_indexes = self._load_arguments(kwargs, "raw_data", "max_indexes")
            return float(self.get_nb_approx_maxes(raw_data, max_indexes[0])) / len(raw_data),

        @register_attr("bilat", "unilat")
        def std_ratio(**kwargs):
            """
            :param norm_var_for_std: prefixes / conflicts normalized variation data
                        note: when used for correlation, prefixes normalized variation data is used
            :param max_index: int - index of maximum value of
                        > prefixes / conflicts raw data for prefixes / conflicts
                        if several, biggest value of the other (conflicts or prefixes) is chosen
                        > correlation data for correlation
                        if several, biggest next value if chosen
            """
            norm_var, max_index = self._load_arguments(kwargs, "norm_var_for_std", "max_index")
            return self.get_max_impact_on_std(norm_var, max_index),

        @register_attr("bilat")
        def other_std_ratio(**kwargs):
            """
            Same as std_ratio exchanging prefixes and conflicts data.
            (max_index param still refers to the same as for std_ratio)
            """
            other_norm_var, max_index, = self._load_arguments(kwargs, "other_norm_var", "max_index")
            return self.get_max_impact_on_std(other_norm_var, max_index),

        @register_attr("bilat", "unilat")
        def last_decile_attributes(**kwargs):
            """
            :param norm_var_for_std: prefixes / conflicts normalized variation data
            :return tuple:  last decile
                            ratio of last decile length compared to data length
            """
            norm_var, = self._load_arguments(kwargs, "norm_var")
            last_decile = np.percentile(norm_var, np.arange(0, 100, 10))[-1]
            last_decile_indexes = [i for i, elt in enumerate(norm_var)
                                   if elt - last_decile > -10 ** -10]  # manage float inaccuracy
            last_decile_spread = self.calc_spread(last_decile_indexes, norm_var)
            return last_decile, last_decile_spread,

        @register_attr("bilat", "unilat")
        def last_quartile_attributes(**kwargs):
            """
            :param norm_var_for_std: prefixes / conflicts normalized variation data
            :return tuple:  last quartile
                            ratio of last quartile length compared to data length
            """
            norm_var, = self._load_arguments(kwargs, "norm_var")
            last_quartile = np.percentile(norm_var, np.arange(0, 100, 25))[-1]
            last_quartile_indexes = [i for i, elt in enumerate(norm_var)
                                     if elt - last_quartile > -10 ** -10]  # manage float inaccuracy
            last_quartile_spread = self.calc_spread(last_quartile_indexes, norm_var)
            return last_quartile, last_quartile_spread,

        @register_attr("bilat", "unilat")
        def percent_above_average(**kwargs):
            """
            :param norm_var_for_std: prefixes / conflicts normalized variation data
            :return float - ratio of number of values above the average compared to data length
            """
            norm_var, = self._load_arguments(kwargs, "norm_var")
            avg = float(sum(norm_var)) / len(norm_var)
            above_average = float(len([elt for elt in norm_var if elt >= avg])) / len(norm_var)
            return above_average,

        @register_attr("bilat")
        def var_of_max(**kwargs):
            """
            :param curve_var: prefixes / conflicts variation data
            :param max_index: int - index of maximum value of prefixes / conflicts data
                            if several, biggest value of the other (conflicts or prefixes) is chosen
            :return float - log of value of variation matching biggest value in raw data
            """
            max_index, curve_var = self._load_arguments(kwargs, "max_index", "curve_var")
            return np.log(curve_var[max_index - 1]) if curve_var[max_index - 1] > 0 else 0,

    def create_attributes(self, bilat_args, bilat_rev_args, unilat_args):
        """
        Create attributes running each function in cls.attribute_makers.

        Arguments must fit the arguments needed by all the functions of the same type.
        (bilat_args contain all arguments needed by functions in cls.attr_makers["bilat"])

        :param bilat_args: dict kwargs arguments passed to 'bilat' functions the first time
        :param bilat_rev_args: dict kwargs arguments passed to 'bilat' functions the second time
        :param unilat_args: dict kwargs arguments passed to 'unilat' functions
        :return: yield attributes
        """
        self.load_makers()
        for maker in self.attribute_makers["bilat"]:
            for elt in maker(series_name="prefixes", **bilat_args):
                yield elt
        for maker in self.attribute_makers["bilat"]:
            for elt in maker(series_name="conflicts", **bilat_rev_args):
                yield elt
        for maker in self.attribute_makers["unilat"]:
            for elt in maker(series_name="corr", **unilat_args):
                yield elt

    @staticmethod
    def get_nb_approx_maxes(data, max_index):
        """
        :return int - number of values in data close to value of the element at max_index
        """

        nb_maxes = len([elt for elt in data
                        if elt >= 0.9 * data[max_index]])

        return nb_maxes

    @staticmethod
    def get_max_impact_on_std(data, max_index):
        """
        Calculate impact on standard deviation of removing maximum value from data.

        :param data: list of numbers
        :param max_index: integer - index in data of the max value whose impact will be calculated
        :return ratio between standard deviation without max value and regular standard deviation.
                (returns 1 if standard deviation is zero)
        """
        np_data = np.array(data)
        std = np.std(np_data)

        smoothen_data = [elt for i, elt in enumerate(data) if i != max_index]
        np_smoothen_data = np.array(smoothen_data)
        smoothen_std = np.std(np_smoothen_data)

        if not isinstance(std, numbers.Number):
            std = 0
        if not isinstance(smoothen_std, numbers.Number):
            smoothen_std = 0

        return float(smoothen_std) / std if std != 0 else 1

    @staticmethod
    def calc_spread(indexes, data):
        """
        :return ratio between width of selected indexes and width of full data
        """
        return float(max(indexes) - min(indexes) + 1) / len(data)


class _AsnPrefOrConfData(object):
    """
    Easy access to either prefixes or conflicts data for specific AS.

    Instance attributes:
    raw: raw data - list of integers - number of prefixes announced / AS in conflicts by day
    var: variation of raw data - list of integers - difference between each value and the next one
    norm_var: normalized variation data - list of integers
    max_indexes: list of integers - indexes in raw data where the maximum value is found
    selected_maxes: list of integers - indexes in max_indexes corresponding to the biggest
                                       correlated data (conflicts or prefixes) value for max_indexes
    max_index: integer - first element of selected_maxes
    """

    def __init__(self, asn, data, correlated_data):
        """
        :param asn: string AS number
        :param data: CreateClassifBaseData instance created for prefixes or conflicts
        :param correlated_data: CreateClassifBaseData instance created for conflicts or prefixes
        """
        self.var = data.var_data[asn]
        self.norm_var = data.normalized_var_data[asn]
        self.raw = data.raw_loaded_data[asn]
        self.max_indexes = self._get_exact_max_indexes(self.raw)

        raw_corr_data = correlated_data.raw_loaded_data[asn]
        self.selected_maxes = [i for i in self.max_indexes if
                               raw_corr_data[i] == max([raw_corr_data[k]
                                                        for k in self.max_indexes])]
        self.max_index = self.selected_maxes[0]

    @staticmethod
    def _get_exact_max_indexes(asn_raw_data):
        """
        :return list of indexes of maximum value in asn_raw_data (except first and last elements)
                    (return all indexes if no max)
        """

        max_var = max([elt for i, elt in enumerate(asn_raw_data)
                       if i != 0 and i != len(asn_raw_data) - 1])

        max_indexes = [i for i, v in enumerate(asn_raw_data)
                       if (v == max_var) and (i != 0) and (i != len(asn_raw_data) - 1)]

        return max_indexes


class _AsnCorrData(object):
    """
    Easy access to correlation between prefixes and conflicts from _AsnPrefOrConfData instances.

    Instance attributes:
    norm_corr: correlation of prefixes and conflicts normalized variation data (product of elements)
               list of integers
    value_corr: correlation of prefixes and conflicts raw data (product of elements)
                list of integers
    max_indexes: list of integers - indexes in norm_corr where the maximum value is found
    max_index: integer - index of maximum value in norm_corr with the biggest next value
    """

    def __init__(self, pfx_data, cfl_data):
        """
        :param pfx_data: _AsnPrefOrConfData instance created for prefixes
        :param cfl_data: _AsnPrefOrConfData instance created for conflicts
        """
        self.norm_corr = self.calc_correlation(pfx_data.norm_var, cfl_data.norm_var)
        self.value_corr = self.calc_correlation(pfx_data.var, cfl_data.var)
        self.max_indexes = [i for i, elt in enumerate(self.norm_corr) if elt == max(self.norm_corr)]

        self.max_index = self.select_max_index()

    @staticmethod
    def calc_correlation(elt1, elt2):
        """
        Calculate correlation between the elements

        :param elt1 elt2: list
        :return list
        """
        return [elt * elt2[i] for i, elt in enumerate(elt1)]

    def select_max_index(self):
        """
        Find the most representative index of maximum value in self.norm_corr

        If maximum value appears several times, the one with the maximum next value is chosen.

        :return integer - index of self.norm_corr selected as max index
        """
        # list of values following a max value
        maxes_next_values = [self.norm_corr[i + 1]
                             for i in self.max_indexes if i + 1 < len(self.norm_corr)]
        # biggest value following a max value
        max_next = max(maxes_next_values) if maxes_next_values else 0
        # indexes amongst max_indexes with the biggest next value
        selected_max = [i for i in self.max_indexes
                        if (i + 1 < len(self.norm_corr)) and (self.norm_corr[i + 1] == max_next)]

        return selected_max[0] if selected_max else self.max_indexes[0]


class AsnData(object):
    """
    Easy access to prefixes, conflicts and correlation data for specific AS for attribute creation.

    Uses _AsnPrefOrConfData for prefixes data and conflicts data
    """

    def __init__(self, asn, pfx_data, cfl_data):
        """
        :param asn: string AS number
        :param pfx_data: CreateClassifBaseData instance created for prefixes (pfx_or_cfl_"pfx")
        :param cfl_data: CreateClassifBaseData instance created for conflicts (pfx_or_cfl_"cfl")
        """
        self.pfx = _AsnPrefOrConfData(asn, pfx_data, cfl_data)
        self.cfl = _AsnPrefOrConfData(asn, cfl_data, pfx_data)
        self.corr = _AsnCorrData(self.pfx, self.cfl)


class CreateClassificationAttributes(object):
    """
    Create attributes needed for Machine learning.

    Uses CreateClassifBaseData to load data before processing it.

    Public Methods:
    create_svm_input
    """

    def __init__(self, pfx_file, cfl_file, ft_open=gzip.open, data_already_processed=False):
        """
        :param pfx_file: prefixes file from prepare data
        :param cfl_file: conflicts file from prepare data
        :param ft_open: function to use to open filename
        :param data_already_processed: boolean
                    True if data in filename are already processed (output of create_plot_input)
                    In this case, get_input_data will only return dict containing data in file
        """
        self.pfx_data = CreateClassifBaseData(pfx_file, "pfx", ft_open, data_already_processed)
        self.cfl_data = CreateClassifBaseData(cfl_file, "cfl", ft_open, data_already_processed)
        self.common_max_indexes = {}

    def load_raw_data(self):
        """
        Use CreateClassifBaseData instances to load data from prefixes and conflicts files.

        :return: nothing
        """
        self.pfx_data.get_input_data()
        self.cfl_data.get_input_data()

    def load_norm_var_data(self):
        """
        Use CreateClassifBaseData instance to create normalize variation.

        :return: nothing
        """
        self.pfx_data.create_normalized_var_input()
        self.cfl_data.create_normalized_var_input()

    def _normalize_svm_input(self, svm_input, max_attr_values, normalize_mask):
        """
        Normalize in place each input type in svm_input that is in normalize_mask.

        Not used

        :param svm_input: dict {asn: [classification attributes for asn]}
        :param max_attr_values: list of maximum for each attribute in svm_input
        :param normalize_mask: function to use to normalize
        :return: nothing
        """
        for asn in svm_input:
            for i, attr in enumerate(svm_input[asn]):
                if i in normalize_mask:
                    svm_input[asn][i] = normalize_mask[i](attr, max_attr_values[i])

    def is_to_skip(self, asn):
        """
        Check if asn data is obviously not a fullview leak.

        - no conflicts has been found for asn (asn not in cfl_data.raw_loaded_data)
        - standard deviation is zero for pfx_data or cfl_data

        :param asn: string asn
        :return boolean
        """
        if asn not in self.cfl_data.raw_loaded_data:
            return True
        if np.std(self.pfx_data.normalized_var_data[asn]) == 0:
            return True
        if np.std(self.cfl_data.normalized_var_data[asn]) == 0:
            return True

        return False

    def create_svm_input(self, ident_format=lambda x: x):
        """
        Create all attributes needed as input for SVM classifier.

        :param ident_format: function to use to format asn name
        :return dict {asn: [attributes]}
        """
        if not self.pfx_data.normalized_var_data:
            self.load_norm_var_data()

        svm_input = defaultdict(list)

        attr_maker = AttributeMakers()
        attr_maker.load_makers()

        max_attr_values = [0] * 2 * (2 * len(attr_maker.attribute_makers["bilat"])
                                     + len(attr_maker.attribute_makers["unilat"]))
        # is set bigger that exact number of attributes because this number cannot be anticipated
        # WARNING: may not be sufficient if attribute makers return too many attributes

        for asn in self.pfx_data.normalized_var_data.keys():
            if self.is_to_skip(asn):
                continue

            asn_data = AsnData(asn, self.pfx_data, self.cfl_data)

            bilat_args = {"norm_var": asn_data.pfx.norm_var,
                          "norm_var_for_std": asn_data.pfx.norm_var,
                          "other_norm_var": asn_data.cfl.norm_var,
                          "max_indexes": asn_data.pfx.max_indexes,
                          "raw_data": asn_data.pfx.raw,
                          "curve_var": asn_data.pfx.var,
                          "max_index": asn_data.pfx.max_index}

            bilat_rev_args = {"norm_var": asn_data.cfl.norm_var,
                              "norm_var_for_std": asn_data.cfl.norm_var,
                              "other_norm_var": asn_data.pfx.norm_var,
                              "max_indexes": asn_data.cfl.max_indexes,
                              "raw_data": asn_data.cfl.raw,
                              "curve_var": asn_data.cfl.var,
                              "max_index": asn_data.cfl.max_index}

            unilat_args = {"pfx_var_norm_data": asn_data.pfx.norm_var,
                           "corr": asn_data.corr.norm_corr,
                           "value_corr": asn_data.corr.value_corr,
                           "max_indexes": asn_data.corr.max_indexes,
                           "norm_var": asn_data.corr.norm_corr,
                           "norm_var_for_std": asn_data.pfx.norm_var,
                           "max_index": asn_data.corr.max_index}

            for i, attr_value in enumerate(attr_maker.create_attributes(bilat_args, bilat_rev_args,
                                                                        unilat_args)):
                svm_input[ident_format(asn)].append(attr_value)
                if abs(attr_value) > max_attr_values[i]:
                    max_attr_values[i] = abs(attr_value)

        self._normalize_svm_input(svm_input, max_attr_values, {})

        return svm_input


class ApplyModel(object):
    """
    Apply SVM classifier to learnt model.

    Path to pickled model is stored in model_svm_file class attribute.

    Public methods:

    get_model_svm_classifier
    Load svm classifier - must be used before trying to classify

    get_classification_result
    Do classification and return result
        {"PEAK": {asn: {"leaks": [indexes in data where leak is detected],
                        "prefixes": [prefixes raw data for asn],
                        "conflicts": [conflicts raw data  for asn]}},
         "NORMAL": {asn: {"prefixes": [prefixes raw data for asn],
                          "conflicts": [conflicts raw data  for asn]}}}
    or {"PEAK": {}} if no peak found
    """
    # directory where input files are stored
    model_data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "model_data")
    # pickled svm model
    model_svm_file = os.path.join(model_data_path, "svm_model.p")
    # labels for known ases (AS202214,NORMAL) - only if model_svm_file missing
    model_ases_labels_file = os.path.join(model_data_path, "ases_init_labels.csv")
    # data from CreateClassificationAttributes {asn: [attributes]} - only if model_svm_file missing
    model_svm_input_file = os.path.join(model_data_path, "model_svm_input.json")
    # prefixes file from prepare_data  - only if model_svm_file and model_svm_input_file missing
    model_pfx_file = os.path.join(model_data_path, "model_pfx_data.csv")
    # conflicts file from prepare_data  - only if model_svm_file and model_svm_input_file missing
    model_cfl_file = os.path.join(model_data_path, "model_cfl_data.csv")

    def __init__(self):
        self._clf = None
        self._results = defaultdict(dict)

    @property
    def clf(self):
        """
        :return classifier (svm model)
        """
        if not self._clf:
            raise NotImplementedError("method get_model_svm_classifier should be called first")
        return self._clf

    @property
    def results(self):
        """
        :return: classification results
        """
        if not self._results:
            raise NotImplementedError("methods get_model_svm_classifier "
                                      "and make_classification_results "
                                      "should be called first")
        return self._results

    def has_svm_input_file(self):
        """
        :return: bool - True if self.model_svm_input_file exists
        """
        if os.path.isfile(self.model_svm_input_file):
            return True
        return False

    def has_svm_file(self):
        """
        :return: bool - True if self.model_svm_file exists
        """
        if os.path.isfile(self.model_svm_file):
            return True
        return False

    def get_model_svm_classifier(self):
        """
        Load svm classifier (self.clf) depending on what input data is available.

        _very_fast_get_model_svm_classifier -> uses self.svm_file
        _fast_get_model_svm_classifier -> uses self.model_svm_input_file
        _get_model_svm_classifier -> uses CreateClassificationAttributes
                                        with self.model_pfx_file and self.model_cfl_file
        """
        if self.has_svm_file():
            self._very_fast_get_model_svm_classifier()
        elif self.has_svm_input_file():
            self._fast_get_model_svm_classifier()
        else:
            self._get_model_svm_classifier()

    def _get_ases_init_labels(self):
        """
        :return: dict {asn: label} from self.model_ases_labels_file
        """
        ases_labels = {}
        with open(self.model_ases_labels_file, "r") as ases_fd:
            ases_reader = csv.reader(ases_fd)
            for line in ases_reader:
                ases_labels[line[0]] = line[1]
        return ases_labels

    def _fit_model_to_data(self, input_data, ases_labels):
        """
        Do the initialization of classifier self.clf (run svm.fit on input_data with ases_labels).

        :return: nothing
        """
        model_normal_input = [v for k, v in input_data.iteritems() if ases_labels[k] == "NORMAL"]
        model_abn_input = [v for k, v in input_data.iteritems() if ases_labels[k] == "ABNORMAL"]

        param_grid = [
            {'C': [0.01, 0.1, 1, 10, 100, 1000, 10000], 'kernel': ['linear']},
            {'C': [0.01, 0.1, 1, 10, 100, 1000, 10000], 'gamma': [0.001, 0.0001],
             'kernel': ['rbf']},
        ]

        gc = grid_search.GridSearchCV(svm.SVC(), param_grid)

        gc.fit(model_normal_input + model_abn_input,
               ["NORMAL"] * len(model_normal_input) + ["PEAK"] * len(model_abn_input))

        self._clf = svm.SVC(**gc.best_params_)

        self.clf.fit(model_normal_input + model_abn_input,
                     ["NORMAL"] * len(model_normal_input) + ["PEAK"] * len(model_abn_input))

    def _get_model_svm_classifier(self):
        """
        Initialize classifier (self.clf) using model_pfx_file and model_cfl_file

        :return: nothing
        """
        ases_labels = self._get_ases_init_labels()

        model_attr_maker = CreateClassificationAttributes(self.model_pfx_file,
                                                          self.model_cfl_file,
                                                          ft_open=open, data_already_processed=True)
        model_svm_inputs = model_attr_maker.create_svm_input()

        self._fit_model_to_data(model_svm_inputs, ases_labels)

    def _fast_get_model_svm_classifier(self):
        """
        Initialize classifier (self.clf) using model_svm_inputs

        :return: nothing
        """
        ases_labels = self._get_ases_init_labels()

        with open(self.model_svm_input_file, "r") as f:
            model_svm_inputs = json.loads(f.readline())

        self._fit_model_to_data(model_svm_inputs, ases_labels)

    def _very_fast_get_model_svm_classifier(self):
        """
        Initialize classifier (self.clf) using model_svm_file (pickle)

        :return: nothing
        """
        with open(self.model_svm_file, "r") as f:
            self._clf = cPickle.load(f)

    def get_classification_result(self, pfx_file, cfl_file, ft_open=open,
                                  data_already_processed=True, as_format=int):
        """
        Run classification once self.clf is initialized (self.get_model_svm_classifier).

        :param pfx_file: prefixes file from prepare data
        :param cfl_file: conflicts file from prepare data
        :param ft_open: function to use to open filename
        :param data_already_processed: boolean
                    True if data in filename are already processed (output of create_plot_input)
                    In this case, get_input_data will only return dict containing data in file
        :param as_format: function to apply to ases to format them in result
        :return: dict - classification result:
                    {"PEAK": {asn: {"leaks": [indexes in data where leak is detected],
                                    "prefixes": [prefixes raw data for asn],
                                    "conflicts": [conflicts raw data  for asn]}},
                     "NORMAL": {asn: {"prefixes": [prefixes raw data for asn],
                                      "conflicts": [conflicts raw data  for asn]}}}
        """

        self._results = defaultdict(dict)

        attr_maker = CreateClassificationAttributes(pfx_file, cfl_file, ft_open,
                                                    data_already_processed)
        svm_inputs = attr_maker.create_svm_input().items()
        if not svm_inputs:
            return {"PEAK": {}}
        res = self.clf.predict([inpt for asn, inpt in svm_inputs])

        for i, label in enumerate(res):
            asn = svm_inputs[i][0]
            self._results[label][as_format(asn)] = {
                "prefixes": attr_maker.pfx_data.raw_loaded_data[asn],
                "conflicts": attr_maker.cfl_data.raw_loaded_data[asn]}

        return self._results

    def save_model_svm_inputs(self, output_file):
        """
        Write attributes calculated with CreateClassificationAttributes into output_file.
        """
        model_attr_maker = CreateClassificationAttributes(self.model_pfx_file,
                                                          self.model_cfl_file,
                                                          ft_open=open, data_already_processed=True)
        model_svm_inputs = model_attr_maker.create_svm_input()
        with open(output_file, "w") as f:
            f.write(json.dumps(model_svm_inputs) + "\n")

    def save_model(self, output_file):
        """
        Pickle fitted classifier (self.clf) into output_file
        """
        self.get_model_svm_classifier()

        with open(output_file, "w") as f:
            cPickle.dump(self.clf, f)


def main(args):
    """
    Use ApplyModel and print classification results on stdout
    """
    import argparse

    parser = argparse.ArgumentParser("Detect alleged fullview leaks using Machine Learning")

    parser.add_argument("pfx_file",
                        help="full path of file with prefixes data - "
                             "from prepare_data.prepare module "
                             "(can be directory with raw output from jarvis "
                             "if process_data is set to True)")
    parser.add_argument("cfl_file",
                        help="full path of file with conflicts data - "
                             "from prepare_data.prepare module "
                             "(can be directory with raw output from jarvis "
                             "if process_data is set to True)")
    parser.add_argument("--out", default=None,
                        help="full path of file where results will be saved - default stdout")

    args = parser.parse_args(args)

    classifier = ApplyModel()
    classifier.get_model_svm_classifier()
    res = classifier.get_classification_result(args.pfx_file, args.cfl_file,
                                               ft_open=open, data_already_processed=True)

    if args.out:
        with open(args.out, "w") as f:
            f.write(json.dumps(res["PEAK"]))
    else:
        for asn in res["PEAK"]:
            print asn


if __name__ == '__main__':
    main(sys.argv[1:])
