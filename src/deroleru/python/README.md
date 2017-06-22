# deroleru - Python module

## Overview

This directory provides code that exposes deroleru detection code to Python.

Once installed, it could be used as follows:
```python
from deroleru import process_data
params = [10, 5, 0.9, 2, 0.9]
result = process_data(prefixes_AS202214_data, conflicts_AS202214_data, params)
```

The `result` variable is a list of days, represented as integers, on which
AS20214 performed route leaks.  A complete example is available in `test.py`.


## Installation

```shell
virtualenv ve_deroleru
source ve_deroleru/bin/activate
pip install -r requirements.txt
python setup.py install
```
