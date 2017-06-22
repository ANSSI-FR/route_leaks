#! /usr/bin/env python
"""
Prepare prefixes and conflicts data from jarvis for route leaks detection.

entry points:

create_from_scratch
Create merged files from input daily files from jarvis

update_day_from_files
Update merged files with one additional day (daily file from jarvis)
"""
import argparse
from datetime import datetime
import functools
import gzip
import json
import sys

import abc
import os
from agnostic_loader import DataLoader
from route_leaks_detection.init_logger import logging

LOGGER = logging.getLogger(__name__)


# TOOLS

def count_daily_prefixes(pfx_raw_data):
    """
    Count how many prefixes have been announced by each AS in input.

    Note: input should contain data for only one day.

    :param pfx_raw_data: generator that yields dictionaries containing BGP announces for one day
                          (from jarvis)
                            {"day": "2016/01/01", "origin_asn": 1000, "num_prefixes": 2,
                            "prefixes": ["190.210.210.0/24", "190.210.211.0/24"]}
    :return: dictionary {asn: nb_of_prefixes_announced_by_asn}
    """
    data = {}
    for line in pfx_raw_data:
        asn = line["origin_asn"]
        nb_pfx = line["num_prefixes"]
        data[asn] = data.get(asn, 0) + nb_pfx
    return data


def count_daily_conflicts(cfl_raw_data):
    """
    Count in input how many ASes each AS have been in conflict with.

    Note: input should contain data for only one day.

    :param cfl_raw_data: generator that yields dictionaries containing BGP conflicts for one day
                          (from jarvis)
                            {"origin": {"prefix": "38.0.0.0/8", "asn": 174},
                            "begin": 1451606400.0, "end": 1451664000.0,
                            "hijacker": {"prefix": "38.130.85.0/24", "asn": 62826},
                            "peer": {"ip": "198.32.176.3", "asn": 1280}, "collector": "rrc14",
                            "type": "DIRECT"}
    :return: dictionary {asn: nb_of_ases_in_conflict_with_asn_bgp_announces}
    """
    data = {}
    for line in cfl_raw_data:
        asn = line["hijacker"]["asn"]
        data[asn] = data.get(asn, set())
        data[asn].add(line["origin"]["asn"])
    return {asn: len(values) for asn, values in data.iteritems()}


def date_from_filename(filename, format_date="%Y-%m-%d"):
    """
    Extract date from filename.

    :param filename: path that should be build like path/to/file/YYYY-MM-DD.gz
    :return: string date in filename YYYY-MM-DD
    """
    day = (os.path.split(filename)[-1]).split(".")[0]
    try:
        day = datetime.strptime(day, format_date)
    except ValueError:
        raise ValueError("filenames must be string date like 2016-01-01.gz, got %s" % filename)
    return day


def _make_default_output_name(input_dir, input_type):
    """
    Create filename by concatenating 'input_dir'/processed_'input_type'.json
    """
    input_dir = input_dir.rstrip(os.path.sep)
    return os.path.join(os.path.dirname(input_dir), "processed_%s.json" % input_type)


def write_json_in_file(data, output_file, ft_open=open, mode="w"):
    """
    Write one json line for each key, value pair in dictionary data into output_file.
    """
    with ft_open(output_file, mode) as f:
        for asn, values in data.iteritems():
            f.write(json.dumps({asn: values}) + "\n")


# DAILY TREATMENT

def update_day(merged_data, day_data, count_ft):
    """
    Update merged_data with values from day_data: append one element to merged_data value.

    :param merged_data: dictionary containing lists of daily values for each asn
                        values can be either nb of prefixes announced or nb of ASes in conflict
                        {"as1": [value_day1, value_day2, value_day3]}
    :param day_data: loaded data of prefixes or conflicts from jarvis
                    prefixes format:
                        {"day": "2016/01/01", "origin_asn": 1000, "num_prefixes": 2,
                        "prefixes": ["190.210.210.0/24", "190.210.211.0/24"]}
                    conflicts format:
                        {"origin": {"prefix": "38.0.0.0/8", "asn": 174},
                        "begin": 1451606400.0, "end": 1451664000.0,
                        "hijacker": {"prefix": "38.130.85.0/24", "asn": 62826},
                        "peer": {"ip": "198.32.176.3", "asn": 1280}, "collector": "rrc14",
                        "type": "DIRECT"}
    :param count_ft: pointer to function to use to aggregate day data
                     should be count_daily_prefixes or count_daily_conflicts
    :return: merged_data updated
    """

    prev_len = len(merged_data.values()[0]) if merged_data.values() else 0

    data = count_ft(day_data)
    for asn, nb in data.iteritems():
        asn = str(asn)
        merged_data[asn] = merged_data.get(asn, [0] * prev_len)
        merged_data[asn].append(nb)

    # add 0 for ASes not in day_data
    for values in merged_data.itervalues():
        if len(values) == prev_len:
            values.append(0)

    return merged_data


def update_day_from_files(pfx_or_cfl, merged_file, day_file,
                          updated_file=None, output_open=gzip.open, format_date="%Y-%m-%d"):
    """
    Add data from day file to merged_file.

    Merged file format is csv : asn,[one_nb_per_day].
    Merge consist in appending value from day_file (csv - format asn,nb) to asn list

    :param pfx_or_cfl: string - either 'pfx' to update prefixes files
                                      or 'cfl' to update conflicts files
    :param merged_file: name of file containing prepared data
    :param day_file: name of file containing data for one day to add to merged_file
                    (prefixes or conflicts from jarvis)
                    filename must be date like YYYY-MM-DD.gz
    :param updated_file: optional - name of file where output data will be written
                                    if not given, output it replaces merged_file
    :param output_open: function to use to open output file
                        (for merged_file and day_file, function to use is detected)
    :return: nothing
    """
    if pfx_or_cfl == "pfx":
        _update_day = functools.partial(update_day, count_ft=count_daily_prefixes)
    elif pfx_or_cfl == "cfl":
        _update_day = functools.partial(update_day, count_ft=count_daily_conflicts)
    else:
        raise ValueError("argument pfx_or_cfl should be either 'pfx' or 'cfl string")

    if updated_file is None:
        updated_file = merged_file

    if merged_file is not None and os.path.isfile(merged_file):
        data = {asn: values for (asn, values)
                in (line.items()[0] for line in DataLoader(merged_file).load())}
        start_date = data.pop("start_date", None)
    else:
        data = {}
        start_date = os.path.basename(day_file).split(".")[0]
        try:
            datetime.strptime(start_date, format_date)
        except ValueError as err:
            LOGGER.warning("Argument day_file filename must be a date (ex: YYYY-MM-DD.gz)")
            raise err

    updated = _update_day(data, DataLoader(day_file).load())

    if start_date:
        write_json_in_file({"start_date": start_date}, updated_file, output_open)
    write_json_in_file(updated, updated_file, output_open, mode="a")


# WORK ON FULL DATA

def create_from_scratch(pfx_dir, cfl_dir, pfx_file=None, cfl_file=None,
                        start_date=None, end_date=None, format_date="%Y-%m-%d"):
    """
    Create merged file from raw input files (without using the intermediate daily csv).

    :param pfx_dir: directory containing all daily prefixes files: BGP announces for one day
                     (from jarvis)
                            {"day": "2016/01/01", "origin_asn": 1000, "num_prefixes": 2,
                            "prefixes": ["190.210.210.0/24", "190.210.211.0/24"]}
                     filenames must be dates YYYY-MM-DD.gz
    :param cfl_dir: dir containing all conflicts files: BGP conflicts for one day (from jarvis)
                            {"origin": {"prefix": "38.0.0.0/8", "asn": 174},
                            "begin": 1451606400.0, "end": 1451664000.0,
                            "hijacker": {"prefix": "38.130.85.0/24", "asn": 62826},
                            "peer": {"ip": "198.32.176.3", "asn": 1280}, "collector": "rrc14",
                            "type": "DIRECT"}
                    filenames must be dates YYYY-MM-DD.gz
    :param pfx_file: optional - name of output file for prefixes
                        default "processed_prefixes.json" in pfx_dir parent directory
    :param cfl_file: optional - name of output file for conflicts
                        default "processed_conflicts.json" in cfl_dir parent directory
    :param start_date end_date: string dates to filter files in pfx_dir and cfl_dir
    :param format_date: format of start_date and end_date
                        must match format of date filenames in pfx_dir and cfl_dir
    :return: data loaded {"pfx": pfx_data, "cfl": cfl_data}
    """
    # check arguments

    assert os.path.isdir(pfx_dir)
    some_pfx_file = os.listdir(pfx_dir)[0].split(".")[0]
    try:
        datetime.strptime(some_pfx_file, format_date)
    except ValueError as err:
        LOGGER.error("Filename (before extension) %s found in pfx_dir %s "
                     "doesn't match date format %s",
                     some_pfx_file, pfx_dir, format_date)
        raise err

    assert os.path.isdir(cfl_dir)
    some_cfl_file = os.listdir(cfl_dir)[0].split(".")[0]
    try:
        datetime.strptime(some_cfl_file, format_date)
    except ValueError as err:
        LOGGER.error("Filename (before extension) %s found in cfl_dir %s "
                     "doesn't match format data %s",
                     some_cfl_file, cfl_dir, format_date)
        raise err

    if not pfx_file:
        pfx_file = _make_default_output_name(pfx_dir, "prefixes")
    else:
        assert os.path.isdir(os.path.dirname(pfx_file))

    if not cfl_file:
        cfl_file = _make_default_output_name(cfl_dir, "conflicts")
    else:
        assert os.path.isdir(os.path.dirname(cfl_file))

    # treat data
    pfx_loader = LoadRouteLeaksData(pfx_dir, "pfx", data_already_processed=False)
    pfx = pfx_loader.load_data(start=start_date, end=end_date)
    cfl_loader = LoadRouteLeaksData(cfl_dir, "cfl", data_already_processed=False)
    cfl = cfl_loader.load_data(start=start_date, end=end_date)

    # write data into output
    write_json_in_file({"start_date": pfx_loader.str_start}, pfx_file)
    write_json_in_file(pfx, pfx_file, mode="a")
    write_json_in_file({"start_date": cfl_loader.str_start}, cfl_file)
    write_json_in_file(cfl, cfl_file, mode="a")

    return {"pfx": {"data": pfx, "start": pfx_loader.start, "end": pfx_loader.end},
            "cfl": {"data": cfl, "start": cfl_loader.start, "end": cfl_loader.end}}


class LoadRouteLeaksData(object):
    """
    Load (prepare if needed) prefixes and conflicts data for route_leaks detection.

    This class is a factory that will use _LoadRouteLeaksPrepared or _LoadRouteLeaksRaw
    to load the input data

    Input data can be
        bgp/prefixes or bgp/conflicts from jarvis
        those data processed by prepare_data module
    In the case of raw data from jarvis, they will be loaded and prepared.

    Once loaded, data is like
        {asn: [list_of_int_representing_daily_values]}

    Public Method:
    load: return the data loaded and processed if needed

    Public Attributes:
    start datetime datetime - start of the data loaded - can be None
    str_start string date - start of the data loaded - can be None
    format_date format of start string date - should match format in input data
    """

    __metaclass__ = abc.ABCMeta

    def __new__(cls, filename, cfl_or_pfx, ft_open=gzip.open, data_already_processed=False):
        if data_already_processed is False:
            obj = object.__new__(_LoadRouteLeaksRaw)
        else:
            obj = object.__new__(_LoadRouteLeaksPrepared)
        obj.__init__(filename, cfl_or_pfx, ft_open=ft_open,
                     data_already_processed=data_already_processed)
        return obj

    def __init__(self, filename, cfl_or_pfx, ft_open=gzip.open, data_already_processed=False):
        """
        :param filename: full path of file to load data from
                         bgp/prefixes or bgp/conflicts directories from jarvis (contain daily files)
                         or these files already processed (from module prepare_data_.prepare)
                         or these files already loaded (in dict)
        :param cfl_or_pfx: 'pfx' or 'cfl
        :param ft_open: function to use to open filename
        :param data_already_processed: boolean
                    True if data in filename are already processed (output of create_plot_input)
                    In this case, load_input_data will only return dict containing data in file
        """
        if cfl_or_pfx == "cfl":
            self._count_ft = count_daily_conflicts
        elif cfl_or_pfx == "pfx":
            self._count_ft = count_daily_prefixes
        else:
            raise ValueError("cfl_or_pfx param should be 'cfl' or 'pfx', not %s" % cfl_or_pfx)
        self._filename = filename
        self._data_already_processed = data_already_processed
        self._ft_open = ft_open
        self.format_date = "%Y-%m-%d"
        self._start = None
        self._end = None

    @property
    def start(self):
        """
        :return: str date self._start - first day of data treated
        """
        if isinstance(self._start, basestring):
            self._start = datetime.strptime(self._start, self.format_date)
        return self._start

    @property
    def end(self):
        """
        :return: str date _end - first day of data treated
        """
        if isinstance(self._end, basestring):
            self._start = datetime.strptime(self._end, self.format_date)
        return self._end

    @property
    def str_start(self):
        """
        :return: str date self._start - first day of data treated
        """
        return self.start.strftime(self.format_date) if self.start else None

    @abc.abstractmethod
    def load_data(self, *args, **kwargs):
        """
        Method to overload so it can load data from self._filename.
        """
        return


class _LoadRouteLeaksPrepared(LoadRouteLeaksData):
    """
    Subclass of LoadRouteLeaksData that will be loaded if data_already_processed arg is True.
    """

    def load_data(self, *args, **kwargs):
        """
        Load data from file when already processed (by prepare_data module)

        :return {asn: [list_of_int_representing_daily_values]}
        """
        raw_data_loader = DataLoader(self._filename)
        input_data = {}
        for data in raw_data_loader.load():
            for asn, values in data.iteritems():
                try:
                    asn = int(asn)
                except ValueError:
                    pass
                input_data[asn] = values
        self._start = input_data.pop("start_date", None)
        return input_data


class _LoadRouteLeaksRaw(LoadRouteLeaksData):
    """
    Subclass of LoadRouteLeaksData that will be loaded if data_already_processed arg is False.
    """

    def load_data(self, *args, **kwargs):
        """
        Load data from directory when not processed yet (output from jarvis)

        :param args: nothing expected
        :param kwargs: "start" and "end" string dates can be given
                        to filter files in self._filename directory

        :return {asn: [list_of_int_representing_daily_values]}
        """
        data = {}
        files_in_dir = sorted(os.listdir(self._filename))

        str_start_date = kwargs.get("start", None) or files_in_dir[0]
        str_end_date = kwargs.get("end", None) or files_in_dir[-1]

        start_date = date_from_filename(str_start_date)
        end_date = date_from_filename(str_end_date)

        nb_days = (end_date - start_date).days + 1

        if nb_days > len(files_in_dir):
            LOGGER.warning("Duration between arguments start_date %s and end_date %s "
                           "doesn't match the number of files in input_dir %s",
                           start_date.date(), end_date.date(), self._filename)

        for fic in files_in_dir:
            if str_start_date[:10] <= fic[:10] <= str_end_date[:10]:
                day_nb = (date_from_filename(fic, format_date=self.format_date)
                          - start_date).days
                data_loader = DataLoader(os.path.join(self._filename, fic))

                for asn, values in self._count_ft(data_loader.load()).iteritems():
                    data[asn] = data.get(asn, [0] * nb_days)
                    data[asn][day_nb] = values

        self._start = start_date
        self._end = end_date
        return data


def _create_parser():
    """
    Create CLI arguments parser
    """
    parser = argparse.ArgumentParser("Create prefixes and conflicts leaks detection input files "
                                     "based on jarvis prefixes and conflicts daily files. "
                                     "daily files names must be dates (YYYY-MM-DD.gz)")

    parser.add_argument("pfx_dir", help="directory containing all daily prefixes files: "
                                        "BGP announces for one day")
    parser.add_argument("cfl_dir", help="directory containing all conflicts files: "
                                        "BGP conflicts for one day")

    parser.add_argument("--pfx_output", default=None,
                        help="name of prefixes output file - default is processed_prefixes.json")
    parser.add_argument("--cfl_output", default=None,
                        help="name of conflicts output file - default is processed_conflicts.json")

    parser.add_argument("--start_date", default=None,
                        help="String date with same format as daily files. "
                             "Will not merge files before start_date. "
                             "Default will take every file.")
    parser.add_argument("--end_date", default=None,
                        help="String date with same format as daily files. "
                             "Will not merge files after end_date."
                             "Default will take every file.")
    parser.add_argument("--format_date",
                        help="Python format for days in daily filenames - default YYYY-MM-DD")

    return parser


def main(args):
    """
    Create prefixes and conflicts leaks detection input files based on jarvis daily files.
    """
    parser = _create_parser()
    args = parser.parse_args(args)

    kwargs = {}
    if args.pfx_output:
        kwargs["pfx_file"] = args.pfx_output
    if args.cfl_output:
        kwargs["cfl_file"] = args.cfl_output
    if args.start_date:
        kwargs["start_date"] = args.start_date
    if args.end_date:
        kwargs["end_date"] = args.end_date
    if args.format_date:
        kwargs["format_date"] = args.format_date

    create_from_scratch(args.pfx_dir, args.cfl_dir, **kwargs)


if __name__ == "__main__":
    main(sys.argv[1:])
