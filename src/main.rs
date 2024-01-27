use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, Error};
use std::path::Path;
use std::time::Instant;

use memmap::Mmap;

struct StationData {
    min: f64,
    max: f64,
    sum: f64,
    n: u32,
}

fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();


    let result1 = simple_file_read(&args[1])?;
    let result2 = memory_mapped(&args[1])?;
    println!("Map size: {} {}", result1.len(), result2.len());
    Ok(())
}

fn simple_file_read<P: AsRef<Path>>(path: P) -> Result<HashMap<String, StationData>, Error> {
    let start = Instant::now();

    let file = File::open(path)?;
    let mut m: HashMap<String, StationData> = HashMap::new();
    for line in BufReader::new(file).lines() {
        if let Ok(l) = line {
            process_line(&mut m, &l);
        }
    }

    let duration = start.elapsed();
    println!("Duration simple file read: {:?}", duration);
    Ok(m)
}

fn memory_mapped<P: AsRef<Path>>(path: P) -> Result<HashMap<String, StationData>, Error> {
    let start = Instant::now();

    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let reader = &mmap[..];
    let mut m: HashMap<String, StationData> = HashMap::new();
    for line in reader.lines() {
        if let Ok(l) = line {
            process_line(&mut m, &l);
        }
    }

    let duration = start.elapsed();
    println!("Duration mmap read: {:?}", duration);
    Ok(m)
}

fn process_line(m: &mut HashMap<String, StationData>, line: &String) {
    let parts: Vec<&str> = line.split(';').collect();
    if parts.len() == 2 {
        let station: String = parts[0].to_owned();
        let temp: f64 = parts[1].parse().expect(&format!("Invalid temperature, ignoring: {}", parts[1]));
        m.entry(station).and_modify(|e| {
            e.max = temp.max(e.max);
            e.min = temp.min(e.min);
            e.sum += temp;
            e.n += 1;
        }
        ).or_insert(StationData {
            min: temp,
            max: temp,
            sum: temp,
            n: 1,
        });
    }
}
