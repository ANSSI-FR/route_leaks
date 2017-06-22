// Guillaume Valadon <guillaume.valadon@ssi.gouv.fr>
//
// deroleru - Detect Route Leaks in Rust - lib.rs


use std::io;
use std::io::prelude::*;
use std::io::BufReader;
use std::fs::File;
use std::str;
use std::fmt;
//use std::error;


extern crate num;
use num::pow;

extern crate rustc_serialize;
use rustc_serialize::json;


#[derive(Debug)]
#[derive(RustcDecodable, RustcEncodable)]
pub struct Data {
    pub ases: Vec<u32>,
    pub prefixes: Vec<u32>,
    pub conflicts: Vec<u32>,
}


#[derive(Debug)]
#[derive(RustcEncodable)]
pub struct Parameters {
    pub prefixes_peak_min_value: u32,
    pub conflicts_peak_min_value: u32,
    pub similarity: f32,
    pub max_nb_peaks: u32,
    pub percent_std: f32,
    pub flat: bool,
}


fn detect_local_max(ret: &mut Vec<u32>, i: usize, previous: u32, current: u32, next: u32) -> () {
    // Check for a local maximum, and store its index in the ret variable

    if previous < current && current > next {
        ret.push(i as u32);
    }
}


fn is_big_enough(min_value: u32, up: u32, down: u32) -> bool {
    // Check if up and down are bigger than min_value

    up > min_value && down > min_value
}


fn is_close_to_abs_max(value: u32, similarity: f32, absolute_max: u32) -> bool {
    // Check if value is close to the absolute maximum

    // Allow floats inaccuracies
    0.000_1 >= (similarity * absolute_max as f32) - (value as f32)
}


fn has_few_enough_peaks(big_maxes: &Vec<u32>,
                        values: &Vec<u32>,
                        index: u32,
                        max_nb_peaks: u32)
                        -> bool {
    // Check if there are enough peaks

    let mut similar_values: u32 = 0;

    for elt in big_maxes {
        if values[*elt as usize] >= values[index as usize] {
            similar_values += 1;
        }

        // Try to return early
        if similar_values > max_nb_peaks {
            return false;
        }
    }

    true
}


// GV: make it only public for unit tests ?!?
pub fn pre_computations(values: &Vec<u32>) -> (Vec<u32>, Vec<u32>, u32) {
    // Perform computations needed by the main algorithm

    // Local variables
    let mut previous_value = values[0];
    let len_values = values.len();

    // Variables that will be returned
    let mut local_maxes = Vec::new();
    let mut variations = Vec::new();
    let mut absolute_max: u32 = 0;

    for i in 1..len_values {

        // Detect local maximum
        let current_value = values[i];
        let next_value = if i + 1 < len_values {
            values[i + 1]
        } else {
            u32::max_value()
        };
        detect_local_max(&mut local_maxes,
                         i,
                         previous_value,
                         current_value,
                         next_value);

        // Compute variations
        let tmp_variation = (current_value as i64 - previous_value as i64).abs() as u32;
        variations.push(tmp_variation);

        // Compute the absolute maximum
        if current_value > absolute_max {
            absolute_max = current_value;
        }

        // Update previous value
        previous_value = current_value;
    }

    (local_maxes, variations, absolute_max)
}


// GV: make it only public for unit tests ?!?
pub fn get_big_maxes(local_maxes: Vec<u32>,
                     variations: Vec<u32>,
                     values: &Vec<u32>,
                     absolute_max: u32,
                     params: &Parameters,
                     peak_min_value: u32)
                     -> Vec<u32> {
    // Identify maxes using several checks

    let mut big_maxes: Vec<u32> = Vec::new();

    for max_index in local_maxes {
        let variation_up = variations[(max_index - 1) as usize];
        let variation_down = variations[max_index as usize];

        if is_big_enough(peak_min_value, variation_up, variation_down) &&
           is_close_to_abs_max(values[max_index as usize], params.similarity, absolute_max) {
            big_maxes.push(max_index);
        }
    }

    let mut cleaned_big_maxes: Vec<u32> = Vec::new();
    for max_index in &big_maxes {
        if has_few_enough_peaks(&big_maxes, values, *max_index, params.max_nb_peaks) {
            cleaned_big_maxes.push(*max_index)
        }
    }

    cleaned_big_maxes
}


fn average(values: &Vec<u32>) -> f32 {
    let mut avg: f32 = 0.0;
    for value in values {
        avg += *value as f32;
    }
    avg / (values.len() as f32)
}


fn variance(values: &Vec<u32>, average: f32) -> f32 {
    let mut var: f32 = 0.0;
    for value in values {
        let tmp = pow(*value as f32 - average as f32, 2);
        var += tmp as f32;
    }
    var / (values.len() as f32)
}


fn check_std_variation(values: &Vec<u32>, big_maxes: &Vec<u32>, params: &Parameters) -> bool {

    // Keep value not included in big_maxes
    let mut smooth_values: Vec<u32> = Vec::new();
    for i in 0..values.len() {
        // Looks for i in big_maxes
        let index = big_maxes.iter().position(|&v| v == i as u32);
        match index {
            Some(_) => (), // i found in big_maxes
            None => smooth_values.push(values[i]),
        }
    }
    let smooth_std = variance(&smooth_values, average(&smooth_values)).sqrt();

    let std = variance(values, average(values)).sqrt();

    #[cfg(feature = "debug")]
    println!("smooth_values={:?} smooth_std={} smooth_avg={} std={} std_avg={}",
             smooth_values,
             smooth_std,
             average(&smooth_values),
             std,
             average(values));

    smooth_std < std * params.percent_std
}


fn find_maxes(values: &Vec<u32>, params: &Parameters, peak_min_value: u32) -> Option<Vec<u32>> {

    // Perform pre-computations
    let (local_maxes, variations, absolute_max) = pre_computations(values);

    #[cfg(feature = "debug")]
    println!("local_maxes={:?} variations={:?} absolute_max={}",
             local_maxes,
             variations,
             absolute_max);

    if local_maxes.is_empty() {
        return None;
    }

    // Get maximums
    let big_maxes = get_big_maxes(local_maxes,
                                  variations,
                                  values,
                                  absolute_max,
                                  params,
                                  peak_min_value);

    #[cfg(feature = "debug")]
    println!("big_maxes={:?}", big_maxes);

    // Filter values
    if check_std_variation(values, &big_maxes, params) {
        return Some(big_maxes);
    }

    None
}


fn find_maxes_prefixes(values: &Vec<u32>, params: &Parameters) -> Option<Vec<u32>> {
    // find_maxes for prefixes

    return find_maxes(values, params, params.prefixes_peak_min_value);
}


fn find_maxes_conflicts(values: &Vec<u32>, params: &Parameters) -> Option<Vec<u32>> {
    // find_maxes for conflicts

    return find_maxes(values, params, params.conflicts_peak_min_value);
}


fn print_leaks(handle: &mut io::StdoutLock,
               ases: &Vec<u32>,
               leaks: &Vec<u32>,
               params: &Parameters)
               -> () {
    // Display leaks according to the command line argument

    if params.flat {
        write!(handle,
               "{} {} {} {} {} ",
               params.prefixes_peak_min_value,
               params.conflicts_peak_min_value,
               params.similarity,
               params.max_nb_peaks,
               params.percent_std)
                .unwrap();

        write!(handle,
               "{} ",
               ases.iter()
                   .map(|asn| asn.to_string())
                   .collect::<Vec<String>>()
                   .join(","))
                .unwrap();

        writeln!(handle,
                 "{}",
                 leaks
                     .iter()
                     .map(|asn| asn.to_string())
                     .collect::<Vec<String>>()
                     .join(","))
                .unwrap();
    } else {
        for asn in ases {
            for leak in leaks {
                writeln!(handle, "{} {}", asn, leak).unwrap();
            }
        }
    }
}


fn identify_leaks(prefixes_indexes: &mut Vec<u32>,
                  conflicts_indexes: &mut Vec<u32>)
                  -> Option<Vec<u32>> {
    // Identify leaks by checking for peaks seen in prefixes and conflicts datasets

    // Skip test if a list is empty
    if prefixes_indexes.len() == 0 || conflicts_indexes.len() == 0 {
        return None;
    }

    // Sort vectors
    prefixes_indexes.sort();
    conflicts_indexes.sort();

    // Compare peaks
    let mut leaks = Vec::new();
    for prefixes_i in 0..prefixes_indexes.len() {
        for conflicts_i in 0..conflicts_indexes.len() {
            if prefixes_indexes[prefixes_i] == conflicts_indexes[conflicts_i] {
                leaks.push(prefixes_indexes[prefixes_i]);
            }
        }
    }

    // Return results
    match leaks.len() {
        0 => None,
        _ => Some(leaks),
    }
}


pub fn read_data(filename: &str) -> Result<Vec<Data>, ReadError> {
    // Load data from a JSON file

    let mut data = Vec::new();

    let file = try!(File::open(filename));
    let mut reader = BufReader::new(file);

    let mut line_num = 1;
    loop {
        // Parse a JSON document and convert it to a Rust structure
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Err(e) => return Err(ReadError::ReadLineError(line_num, e)),
            Ok(len) if len == 0 => break,
            Ok(_) => (),
        }

        let doc: Data =
            try!(json::decode(&line).map_err(|e| ReadError::DataFormatError(line_num, e)));
        data.push(doc);

        line_num += 1;
    }

    Ok(data)

}

pub fn process_data(data: &Vec<Data>, params: &Parameters) -> () {

    for doc in data {
        #[cfg(feature = "debug")]
        println!("ases={:?}", doc.ases);

        // Display results
        match process_doc(&doc, &params) {
            None => (),
            Some(leaks) => {
                let stdout = io::stdout();
                let mut handle = stdout.lock();
                print_leaks(&mut handle, &doc.ases, &leaks, &params);
            }
        }

    }
}


pub fn process_doc(doc: &Data, params: &Parameters) -> Option<Vec<u32>> {

    let prefixes_maxes = find_maxes_prefixes(&doc.prefixes, &params);
    let conflicts_maxes = find_maxes_conflicts(&doc.conflicts, &params);

    #[cfg(feature = "debug")]
    println!("====");

    match (prefixes_maxes, conflicts_maxes) {
        (Some(mut p_maxes), Some(mut c_maxes)) => identify_leaks(&mut p_maxes, &mut c_maxes),
        _ => None,
    }
}


fn get_integer(iter: &mut str::SplitWhitespace) -> Result<u32, ReadError> {
    iter.next()
        .ok_or(ReadError::ParameterFormatError)
        .and_then(|s| s.parse::<u32>().map_err(ReadError::IntegerConvertError))
}

#[test]
fn test_get_integer_ok() {
    let mut iter = "2807".split_whitespace();
    assert_eq!(get_integer(&mut iter).unwrap(), 2807)
}

#[test]
fn test_get_integer_ko_0() {
    let mut iter = "ko".split_whitespace();
    let tmp = get_integer(&mut iter);
    assert_eq!(tmp.unwrap_err(), "invalid digit found in string")
}

#[test]
fn test_get_integer_ko_1() {
    let mut iter = "2807".split_whitespace();
    assert_eq!(get_integer(&mut iter).unwrap(), 2807);
    let tmp = get_integer(&mut iter);
    assert_eq!(get_integer(&mut iter).unwrap_err(),
               "incorrect parameter format !")
}


fn get_float(iter: &mut str::SplitWhitespace) -> Result<f32, ReadError> {
    iter.next()
        .ok_or(ReadError::ParameterFormatError)
        .and_then(|s| s.parse::<f32>().map_err(ReadError::FloatConvertError))
}


fn parse_parameter(line: &String, arg_flat: bool) -> Result<Parameters, ReadError> {
    // Parse a parameter from a string

    let mut iter = line.split_whitespace();

    Ok(Parameters {
           prefixes_peak_min_value: try!(get_integer(&mut iter)),
           conflicts_peak_min_value: try!(get_integer(&mut iter)),
           max_nb_peaks: try!(get_integer(&mut iter)),
           similarity: try!(get_float(&mut iter)),
           percent_std: try!(get_float(&mut iter)),
           flat: arg_flat,
       })
}


#[derive(Debug)]
pub enum ReadError {
    OpenError(io::Error),
    ReadLineError(u32, io::Error),
    ParameterFormatError,
    ParameterLineError(u32, Box<ReadError>),
    IntegerConvertError(std::num::ParseIntError),
    FloatConvertError(std::num::ParseFloatError),
    DataFormatError(u32, json::DecoderError),
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ReadError::OpenError(ref err) => write!(f, "can't open the file !\n    -> {}", err),
            ReadError::ReadLineError(lnum, ref err) => {
                write!(f, "can't read line #{} !\n    -> {}", lnum, err)
            }
            ReadError::ParameterFormatError => write!(f, "invalid format !"),
            ReadError::ParameterLineError(lnum, ref err) => {
                write!(f, "invalid parameter at line #{} !\n    -> {}", lnum, err)
            }
            ReadError::IntegerConvertError(ref err) => write!(f, "{}", err),
            ReadError::FloatConvertError(ref err) => write!(f, "{}", err),
            ReadError::DataFormatError(lnum, ref err) => {
                write!(f, "invalid data at line #{} !\n    -> {}", lnum, err)
            }
        }
    }
}

/*
// Currenty not used
impl error::Error for ReadError {
    fn description(&self) -> &str {
        "ReadError.description()"
    }

    fn cause(&self) -> Option<&error::Error> {
        None
    }
}
*/

impl From<io::Error> for ReadError {
    fn from(err: io::Error) -> ReadError {
        ReadError::OpenError(err)
    }
}


pub fn read_parameters(filename: &str, arg_flat: bool) -> Result<Vec<Parameters>, ReadError> {
    // Load parameters from a file

    // Open the file for reading
    let file = try!(File::open(filename)); // Thanks to from<io::Error>() !
    let mut reader = BufReader::new(file);

    // Iterate over the lines and store parameters into data
    let mut data = Vec::new();
    let mut line_num = 1;
    loop {

        // Read one line
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Err(e) => return Err(ReadError::ReadLineError(line_num, e)),
            Ok(len) if len == 0 => break,
            Ok(_) => (),
        }

        // Parse a line as a parameter struct
        data.push(try!(parse_parameter(&line, arg_flat)
                       .map_err(|e| ReadError::ParameterLineError(line_num, Box::new(e)))));

        line_num += 1;
    }

    Ok(data)
}
