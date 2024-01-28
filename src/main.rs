use std::collections::HashMap;
use std::env;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufRead, BufReader, Error};
use std::path::Path;
use std::time::Instant;

use memmap::Mmap;
use rayon::prelude::*;

struct StationData {
    min_temp: f64,
    max_temp: f64,
    sum_temp: f64,
    n: u32,
}

const SLICE_SIZE: usize = 2 << 15;

fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();

    simple_file_read(&args[1])?;

    parallel_memory_mapped(&args[1])?;

    Ok(())
}

fn simple_file_read<'a, P: AsRef<Path>>(path: P) -> Result<(), Error> {
    let start = Instant::now();

    let file = File::open(path)?;
    let m = read_stations_data(BufReader::new(file));

    let duration = start.elapsed();
    print_result(&m);
    println!("Duration simple file read: {:?}", duration);
    Ok(())
}

fn read_stations_data<P: BufRead>(reader: P) -> HashMap<String, StationData> {
    let mut m: HashMap<String, StationData> = HashMap::new();
    for line in reader.lines() {
        if let Ok(l) = line {
            let parts: Vec<&str> = l.split(';').collect();
            if parts.len() == 2 {
                let station: String = parts[0].to_owned();
                let temp: f64 = parts[1].parse().expect(&format!("Invalid temperature, ignoring: {}", parts[1]));
                m.entry(station).and_modify(|e| {
                    e.max_temp = temp.max(e.max_temp);
                    e.min_temp = temp.min(e.min_temp);
                    e.sum_temp += temp;
                    e.n += 1;
                }
                ).or_insert(StationData {
                    min_temp: temp,
                    max_temp: temp,
                    sum_temp: temp,
                    n: 1,
                });
            }
        }
    }
    m
}

fn parallel_memory_mapped<P: AsRef<Path>>(path: P) -> Result<(), Error> {
    let start = Instant::now();

    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let slices = slice(&mmap[..]);
    let m = slices
        .par_iter()
        .map(|slice| read_stations_data_slice(*slice))
        .reduce(|| HashMap::new(),
                |mut m1, m2| {
                    for (station, station_data) in m2.into_iter() {
                        m1.entry(station).and_modify(|e| {
                            e.max_temp = station_data.max_temp.max(e.max_temp);
                            e.min_temp = station_data.min_temp.min(e.min_temp);
                            e.sum_temp += station_data.sum_temp;
                            e.n += station_data.n;
                        }
                        ).or_insert(station_data);
                    }
                    m1
                },
        );

    let duration = start.elapsed();
    print_result(&m);
    println!("Duration parallel mmap read: {:?}", duration);
    Ok(())
}

fn slice(data: &[u8]) -> Vec<&[u8]> {
    let mut slices: Vec<&[u8]> = Vec::new();
    let mut slice_start: usize = 0;
    let len = data.len();
    while slice_start < len {
        let mut slice_end: usize = slice_start + SLICE_SIZE;
        while slice_end < len && &data[slice_end] != &b'\n' {
            slice_end += 1;
        }
        if slice_end < len {
            slices.push(&data[slice_start..slice_end]);
        } else {
            slices.push(&data[slice_start..len]);
        }
        slice_start = slice_end + 1;
    }
    slices
}

fn read_stations_data_slice(data: &[u8]) -> HashMap<&str, StationData> {
    let mut m: HashMap<&str, StationData> = HashMap::new();
    let mut i: usize = 0;
    let len: usize = data.len();

    let mut station_start: usize = 0;
    let mut station_end: usize = 0;
    let mut temp_start: usize = 0;
    while i < len {
        if &data[i] == &b'\n' {
            process_record(data, &mut m, station_start, station_end, temp_start, i);
            station_start = i + 1;
        } else if &data[i] == &b';' {
            station_end = i;
            temp_start = i + 1;
        }
        i += 1;
    }
    // process the last record if the file does not end with a newline
    if &data[len - 1] != &b'\n' {
        process_record(data, &mut m, station_start, station_end, temp_start, len);
    }
    m
}

fn process_record<'a>(data: &'a [u8], m: &mut HashMap<&'a str, StationData>, station_start: usize, station_end: usize, temp_start: usize, temp_end: usize) {
    let station: &str = std::str::from_utf8(&data[station_start..station_end]).expect("Invalid UTF-8 sequence");
    // TODO use a custom temperature parser
    let temp: &str = std::str::from_utf8(&data[temp_start..temp_end]).expect("Invalid UTF-8 sequence");
    let temp: f64 = temp.parse().expect(temp);
    m.entry(station).and_modify(|e| {
        if temp > e.max_temp {
            e.max_temp = temp;
        }
        if temp < e.min_temp {
            e.min_temp = temp;
        }
        e.sum_temp += temp;
        e.n += 1;
    }
    ).or_insert(StationData {
        min_temp: temp,
        max_temp: temp,
        sum_temp: temp,
        n: 1,
    });
}

fn print_result<P: AsRef<str> + Display>(m: &HashMap<P, StationData>) {
    let list: Vec<String> = m.iter()
        .map(|(station, station_data)| format!("{}={:.1}/{:.1}/{:.1}", station, station_data.min_temp, station_data.sum_temp / station_data.n as f64, station_data.max_temp))
        .collect();
    println!("{{{}}}", list.join(", "));
}
