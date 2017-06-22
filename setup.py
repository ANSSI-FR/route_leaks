# -*- coding: utf-8 -*-
# Copyright (C) 2017 ANSSI
# This file is part of the tabi project licensed under the MIT license.

from setuptools import setup

setup(name="route_leaks_detection",
      description="Detect BGP route leaks",
      author="Julie Rossi",
      author_email="julie.rossi@ssi.gouv.fr",
      version="0.1",
      package_dir={"": "src"},
      packages=["route_leaks_detection", "route_leaks_detection.heuristics",
                "route_leaks_detection.classification", "route_leaks_detection.prepare_data"],
      data_files=[("route_leaks_detection/classification/model_data/",
                   ["src/route_leaks_detection/classification/model_data/ases_init_labels.csv",
                    "src/route_leaks_detection/classification/model_data/model_cfl_data.csv",
                    "src/route_leaks_detection/classification/model_data/model_pfx_data.csv",
                    "src/route_leaks_detection/classification/model_data/model_svm_input.json",
                    "src/route_leaks_detection/classification/model_data/svm_model.p"])],
      scripts=["src/bin/detect_route_leaks", "src/bin/classification"],
      include_package_data=True
      )
