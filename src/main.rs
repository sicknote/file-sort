extern crate stopwatch;

use std::io::{BufReader, BufRead, Write, LineWriter};
use std::cmp::Ordering;
use std::fs;
use stopwatch::Stopwatch;

fn main() {
    let source_path = "C:\\data\\CLionProjects\\exports";
    let mut tup: Vec<(&str, usize, usize)> = Vec::new();

    tup.push(("s", 0, 2));
    tup.push(("s", 2, 2));
    tup.push(("s", 7, 6));
    tup.push(("s", 13, 5));
    tup.push(("s", 23, 5));
    tup.push(("s", 139, 3));
    tup.push(("s", 37, 3));
    tup.push(("s", 33, 4));
    tup.push(("s", 40, 1));
    tup.push(("s", 43, 8));
    tup.push(("s", 51, 4));
    tup.push(("s", 55, 1));
    tup.push(("s", 56, 6));
    tup.push(("d", 108, 8));
    tup.push(("s", 142, 5));
    tup.push(("d", 116, 8));
    tup.push(("d", 124, 8));
    tup.push(("s", 99, 9));

    let paths = fs::read_dir(source_path).unwrap();

    for path in paths {
        let entry = path.unwrap();
        let file_type = entry.file_type();

        let is_dir = match file_type {
            Ok(ft) => {
                ft.is_dir()
            }
            Err(_) => {
                panic!("file type not determined for '{:?}'.", entry.file_name());
            }
        };

        let file_entry = entry.file_name();
        let mut source_file = String::from(source_path);

        if is_dir {
            println!("skipping '{}' as it is a directory", source_file);

            continue;
        }

        let sw = Stopwatch::start_new();

        source_file.push_str("\\");
        source_file.push_str(file_entry.to_str().expect("File name error"));

        println!("handling file: {}", source_file);

        sort_file_contents(&source_file, &tup);

        println!("Time Taken: {:?}", sw.elapsed());
    }
}

fn sort_file_contents(source_file: &str, tup: &Vec<(&str, usize, usize)>) {
    let mut buffer: Vec<String> = Vec::new();
    let source = std::fs::File::open(source_file).expect("Failed to open file");
    let mut source_reader = BufReader::new(source);

    loop {
        let mut line = String::new();
        let l = source_reader.read_line(&mut line).expect("Failed to read line");

        if l == 0 {
            break;
        }

        buffer.push(line);
    }

    fs::remove_file(source_file).expect("Failed to remove file");

    buffer.sort_by(|a, b| comparison_predicate(a, b, &tup));

    let source = std::fs::File::create(source_file).expect("Failed to create file");
    let mut source_writer = LineWriter::new(source);

    for line in buffer
    {
        source_writer.write(line.as_bytes()).expect("Failed to write line");
    }
}

fn comparison_predicate(a: &str, b: &str, tup: &Vec<(&str, usize, usize)>) -> Ordering {
    let mut compare: Ordering;

    for var in tup {
        if var.0 == "s" {
            compare = compare_string_slice(a, b, var.1, var.2);

            if compare != Ordering::Equal
            {
                return compare;
            }
        }
        if var.0 == "d" {
            compare = compare_string_date_slice(a, b, var.1, var.2);

            if compare != Ordering::Equal
            {
                return compare;
            }
        }
    }

    Ordering::Equal
}

fn compare_string_slice(a: &str, b: &str, start: usize, length: usize) -> Ordering {
    let end = start + length;
    let slice_a: &str = &a[start..end];
    let slice_b: &str = &b[start..end];

    slice_a.cmp(&slice_b)
}

fn compare_string_date_slice(a: &str, b: &str, start: usize, length: usize) -> Ordering {
    let end = start + length;
    let slice_a: &str = &a[start..end];
    let slice_b: &str = &b[start..end];

    compare_string_dates(&slice_a, &slice_b)
}

fn compare_string_dates(a: &str, b: &str) -> Ordering {
    const BLANK_DATE: &str = "        ";

    if a == BLANK_DATE && b == BLANK_DATE {
        return Ordering::Equal;
    }

    if a == BLANK_DATE {
        return Ordering::Greater;
    }

    if b == BLANK_DATE {
        return Ordering::Less;
    }

    a.cmp(b)
}