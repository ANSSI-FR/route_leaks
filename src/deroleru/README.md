# deroleru - Detecting Route Leak in Rust

## Overview

This projects implements the algorithm described in the article *Detecting
Route Leak at Scale* in Rust. It was developed to make detections faster in
order to experiment with parameter values. A companion Python module is
available in the `python/` directory.


## Building deroleru

A working Rust compiler is needed. Check the [Rust
Install](https://www.rust-lang.org/en-US/install.html) page to install it.

```shell
cargo build --release
```

## Usage

By default, `deroleru` requires a data file. The file `data/pfx_cfl_2015.json`
can be used as an example. The `--params` parameter can be used to override the
default parameters, or test several parameters on a single dataset. The
`--flat` parameter provides a compact output. The `--progress` displays a
progress bar: this is useful when computing results with many parameters.

A typical run looks like:
```shell
$ cargo run --release -- data/pfx_cfl_2015.json --flat
    Finished release [optimized] target(s) in 0.0 secs
     Running `target/release/deroleru data/pfx_cfl_2016.json --flat`
[..]
10 5 0.9 2 0.9 200759 112
[..]
```

This output means than 200759 performed a route leak on April 21, 2016. The
algorithm used the following parameters values:
- prefixes_peak_min_value: 10
- conflicts_peak_min_value: 5
- similarity: 0.9
- max_nb_peaks: 2
- percent_std: 0.9

## Creating a dataset

`deroleru` use a specific dataset file format that can be built using the
`reformat.py` Python script as follows:

```shell
cd data/
python reformat.py 2015 > pfx_cfl_2015.json
```
