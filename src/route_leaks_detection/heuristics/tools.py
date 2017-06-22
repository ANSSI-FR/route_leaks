# -*- coding: utf-8 -*-
# This file is part of the tabi project licensed under the MIT license.
# Copyright (C) 2017 ANSSI

from datetime import datetime, timedelta


def iter_on_str_dates(str_start_date, str_end_date, format_date="%Y-%m-%d"):
    if not str_end_date:
        yield str_start_date
    else:
        start_date = datetime.strptime(str_start_date, format_date)
        end_date = datetime.strptime(str_end_date, format_date)
        for n in range((end_date - start_date).days + 1):
            yield (start_date + timedelta(n)).strftime(format_date)
