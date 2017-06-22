// Guillaume Valadon <guillaume.valadon@ssi.gouv.fr>
//
// deroleru - Detect Route Leaks in Rust - main.rs
//


use std::io;


extern crate argparse;
use argparse::{ArgumentParser, StoreTrue, Store};

extern crate pbr;
use pbr::ProgressBar;

extern crate pipeliner;
use pipeliner::Pipeline;


extern crate deroleru;


fn main() {

    // Get command line arguments
    let mut arg_flat: bool = false;
    let mut arg_params: String = String::new();
    let mut arg_progress: bool = false;
    let mut arg_filename: String = String::new();

    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Detect Route Leaks in Rust");

        ap.refer(&mut arg_flat)
            .add_option(&["--flat"], StoreTrue, "Dump flat results");

        ap.refer(&mut arg_progress)
            .add_option(&["--progress"], StoreTrue, "Display a progress bar");

        ap.refer(&mut arg_params)
            .add_option(&["--params"], Store, "File that contains parameters");

        ap.refer(&mut arg_filename)
            .add_argument("filename", Store, "Filename to process")
            .required();

        ap.parse_args_or_exit();
    }


    // Get data from file
    let data = match deroleru::read_data(arg_filename.as_str()) {
        Ok(d) => d,
        Err(err) => {
            println!("Error while reading data: {}", err);
            return;
        }
    };


    // Get parameters
    let parameters = match arg_params.len() {
        // Parse parameters from file
        len if len > 0 => {
            match deroleru::read_parameters(arg_params.as_str(), arg_flat) {
                Ok(p) => p,
                Err(err) => {
                    println!("Error while reading parameters: {}", err);
                    return;
                }
            }
        }
        // Use default parameters
        _ => {
            vec![deroleru::Parameters {
                     prefixes_peak_min_value: 10,
                     conflicts_peak_min_value: 5,
                     similarity: 0.9,
                     max_nb_peaks: 2,
                     percent_std: 0.9,
                     flat: arg_flat,
                 }]
        }
    };


    // Parse data using parameters
    let mut pb = ProgressBar::on(io::stderr(), parameters.len() as u64);

    for _ in parameters
            .with_threads(6)
            .map(move |params| deroleru::process_data(&data, &params)) {
        if arg_progress {
            pb.inc();
        }
    }

    if arg_progress {
        pb.finish_print("done");
    }
}
