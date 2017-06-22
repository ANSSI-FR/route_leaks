// Guillaume Valadon <guillaume.valadon@ssi.gouv.fr>
//
// deroleru - Detect Route Leaks in Rust from Python - lib.rs
// Call the Rust function from Python


#[macro_use]
extern crate cpython;


extern crate deroleru;


// The Python module will be called deroleru
py_module_initializer!(deroleru, initderoleru, PyInit_deroleru, |py, m| {
    m.add(py, "__doc__", "Detect Route Leaks")?;
    m.add(py,
             "process_data",
             py_fn!(py,
                    process_data_py(prefixes: Vec<u32>,
                                    conflicts: Vec<u32>,
                                    prefixes_peak_min_value: u32,
                                    conflicts_peak_min_value: u32,
                                    similarity: f32,
                                    max_nb_peaks: u32,
                                    percent_std: f32)))?;
    Ok(())
});


fn process_data_py(_: cpython::Python,
                   prefixes: Vec<u32>,
                   conflicts: Vec<u32>,
                   prefixes_peak_min_value: u32,
                   conflicts_peak_min_value: u32,
                   similarity: f32,
                   max_nb_peaks: u32,
                   percent_std: f32)
                   -> cpython::PyResult<Vec<u32>> {

    // Wrapper used to call process_doc()


    // Fill a Parameters struct
    let params = deroleru::Parameters {
        prefixes_peak_min_value: prefixes_peak_min_value,
        conflicts_peak_min_value: conflicts_peak_min_value,
        similarity: similarity,
        max_nb_peaks: max_nb_peaks,
        percent_std: percent_std,
        flat: true,
    };

    // Build a Data structure
    let doc = deroleru::Data {
        ases: vec![0], // dummy AS number
        prefixes: prefixes,
        conflicts: conflicts,
    };

    // Detect leaks and return the list of indexes
    Ok(match deroleru::process_doc(&doc, &params) {
           None => Vec::new(),
           Some(leaks) => leaks,
       })
}
