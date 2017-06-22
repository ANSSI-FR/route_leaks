# -*- coding: utf-8 -*-
# Copyright (C) 2017 ANSSI
# This file is part of the tabi project licensed under the MIT license.

def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    return obj
