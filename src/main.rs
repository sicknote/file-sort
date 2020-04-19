extern crate clap;
extern crate stopwatch;

use clap::{App, Arg};
use std::io::{BufReader, BufRead, Write, BufWriter, SeekFrom, Seek};
use std::cmp::Ordering;
use std::{fs, env};
use stopwatch::Stopwatch;
use uuid::Uuid;
use std::fs::File;

const LIMIT: i32 = 1000;
const BUFFER_SIZE: usize = 500;
const BLANK_DATE: &str = "        ";

fn main() {
    let matches = App::new("TACT File splitter")
        .version("1.0")
        .author("edb")
        .about("Sort files by row/block")
        .arg(Arg::with_name("INPUT")
            .help("Sets the input file to use")
            .required(true)
            .index(1))
        .get_matches();

    let source_path = matches.value_of("INPUT").unwrap();
    let sort_rules = sorter();

    let sw_full = Stopwatch::start_new();

    println!("Sorting file: {}", source_path);

    let split_path = split_file(source_path);

    println!("Time: {:?}", sw_full.elapsed());

    sort_files(&split_path, &sort_rules);

    println!("Time: {:?}", sw_full.elapsed());

    let final_path = join_files(&split_path, &sort_rules);

    std::fs::remove_dir_all(split_path).expect("Failed to remove intermediate folder");

    println!("Total Time: {:?}", sw_full.elapsed());
    println!("Sorted file: {}", final_path);
}

/// Splits the file source_path into a temporary folder and returns that path
fn split_file(source_path: &str) -> String {
    let root = root_path();
    let target_path = temporary_directory(root);

    println!("split_file: {}", &target_path);

    std::fs::create_dir(&target_path).expect("Failed to create target directory");

    let source = std::fs::File::open(source_path).expect("Failed to open file");
    let mut source_reader = BufReader::new(source);
    let mut f = 1;

    'outer: loop {
        let mut file = String::from(&target_path);

        file.push_str("\\file.");
        file.push_str(f.to_string().as_str());

        drop_file(&file);

        let target = File::create(file).expect("Failed to open target file");
        let mut target_writer = BufWriter::new(target);
        let mut total = 0;

        'inner: loop {
            let mut line = String::new();
            let l = source_reader.read_line(&mut line).expect("Failed to read line");

            if l == 0 { // EOF
                break 'outer;
            }

            let b = line.as_bytes();

            target_writer.write(b).expect("Failed to write to file");

            total += l;

            if total >= 10_485_760 {
                break 'inner;
            }
        }

        target_writer.flush().expect("Failed to flush writer");

        f = f + 1;
    }

    target_path
}

/// Sorts all the files in source_path in memory
fn sort_files(source_path: &str, sort: &Vec<(&str, usize, usize)>) {
    let paths = files_in_directory(source_path);

    println!("sort_files: {}", &source_path);

    for path in paths {
        sort_file_contents(&path, &sort);
    }
}

/// Joins the sorted files in source into a temporary folder and returns the full path to the final file
fn join_files(source: &str, sort: &Vec<(&str, usize, usize)>) -> String {
    let files = &files_in_directory(source);
    let root = root_path();
    let mut target = temporary_directory(root);
    let mut buffers = Vec::new();
    let mut offsets: Vec<u64> = Vec::new();
    let mut positions: Vec<usize> = Vec::new();
    let mut drained: Vec<bool> = Vec::new();

    std::fs::create_dir(&target).expect("Failed to create target directory");

    println!("join_files: {}", &target);

    populate_first_collections(&files, &mut buffers, &mut offsets, &mut positions, &mut drained);

    target.push_str("\\sorted.export");

    let target_file = std::fs::File::create(&target).expect("Failed to create file");
    let mut source_writer = BufWriter::new(target_file);

    loop {
        let final_buffer = parse_buffers(&mut buffers, &mut offsets, &mut positions, &mut drained, &sort, &files);

        if final_buffer.is_empty() {
            break;
        }

        for x in final_buffer {
            source_writer.write(x.as_bytes()).expect("Failed to write line");
        }

        source_writer.flush().expect("Flush failed");
    }

    target
}

fn sorter<'a>() -> Vec<(&'a str, usize, usize)> {
    let mut tup: Vec<(&'a str, usize, usize)> = Vec::new();

    tup.push(("s", 0, 2));
    tup.push(("s", 2, 2 + 2));
    tup.push(("s", 7, 7 + 6));
    tup.push(("s", 13, 13 + 5));
    tup.push(("s", 23, 23 + 5));
    tup.push(("s", 139, 139 + 3));
    tup.push(("s", 37, 37 + 3));
    tup.push(("s", 33, 33 + 4));
    tup.push(("s", 40, 40 + 1));
    tup.push(("s", 43, 43 + 8));
    tup.push(("s", 51, 51 + 4));
    tup.push(("s", 55, 55 + 1));
    tup.push(("s", 56, 56 + 6));
    tup.push(("d", 108, 108 + 8));
    tup.push(("s", 142, 142 + 5));
    tup.push(("d", 116, 116 + 8));
    tup.push(("d", 124, 124 + 8));
    tup.push(("s", 99, 99 + 9));

    tup
}

/// Initial load of data from the given files to a set of vectors
fn populate_first_collections<'a>(files: &Vec<String>, buffers: &mut Vec<Vec<String>>, offsets: &mut Vec<u64>, positions: &mut Vec<usize>, drained: &mut Vec<bool>) {
    for source_file in files {
        let mut buffer_offset: u64 = 0;
        let mut lines = 0;
        let mut buffer: Vec<String> = Vec::new();
        let source = std::fs::File::open(source_file).expect("Failed to open file");
        let mut source_reader = BufReader::new(source);

        'inner: loop {
            let mut line = String::new();
            let l = source_reader.read_line(&mut line).expect("Failed to read line");

            if l == 0 { // EOF
                break 'inner;
            }

            lines = lines + 1;

            buffer_offset = buffer_offset + line.len() as u64;

            buffer.push(line);

            if lines >= LIMIT
            {
                break 'inner;
            }
        }

        buffers.push(buffer);
        offsets.push(buffer_offset);
        positions.push(0);
        drained.push(false);
    }
}

fn populate_empty_collection(buffers: &mut Vec<Vec<String>>, offsets: &mut Vec<u64>, positions: &mut Vec<usize>, files: &Vec<String>, lowest_index: usize) {
    let mut offset = offsets[lowest_index];
    let mut lines = 0;
    let mut buffer: Vec<String> = Vec::new();
    let path = &files[lowest_index];
    let source = std::fs::File::open(path).expect("Failed to open file");
    let mut source_reader = BufReader::with_capacity(16384, source);

    source_reader.seek(SeekFrom::Start(offset)).expect("Failed to seek in reader");

    loop {
        let mut line = String::new();
        let l = source_reader.read_line(&mut line).expect("Failed to read line");

        if l == 0 {
            break;
        }

        lines = lines + 1;

        offset = offset + line.len() as u64;

        buffer.push(line);

        if lines >= LIMIT
        {
            break;
        }
    }

    offsets[lowest_index] = offset;
    buffers[lowest_index] = buffer;
    positions[lowest_index] = 0;
}

/// sorts all elements in all buffers into a final buffer
fn parse_buffers(buffers: &mut Vec<Vec<String>>, offsets: &mut Vec<u64>, positions: &mut Vec<usize>, drained: &mut Vec<bool>, sort: &Vec<(&str, usize, usize)>, files: &Vec<String>) -> Vec<String> {
    let mut final_buffer: Vec<String> = Vec::new();
    let reader_length = buffers.len();

    loop {
        let mut lowest_index: usize = usize::min_value();
        let mut lowest_value: String = "".to_string();
        let mut found = false;

        for i in { 0..reader_length } {
            let mut position = positions[i];
            let mut buffer = &buffers[i];

            if position == buffer.len() {
                let is_drained = drained[i];

                if is_drained
                {
                    continue;
                }

                populate_empty_collection(buffers, offsets, positions, &files, i);

                position = positions[i];
                buffer = &buffers[i];

                if position == buffer.len() {
                    drained[i] = true;

                    continue;
                }
            }

            let current_value = &buffer[position];

            if !found {
                lowest_index = i;
                found = true;
                lowest_value = String::from(current_value);

                continue;
            }

            let compare = compare_by_predicate(&current_value, &lowest_value, &sort);

            if compare == Ordering::Less {
                lowest_index = i;
                found = true;
                lowest_value = String::from(current_value);
            }
        }

        if !found {
            break;
        }

        final_buffer.push(lowest_value);

        let update = positions[lowest_index];

        positions[lowest_index] = update + 1;

        if final_buffer.len() >= BUFFER_SIZE {
            break;
        }
    }

    final_buffer
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

    buffer.sort_by(|a, b| compare_by_predicate(a, b, &tup));

    let source = std::fs::File::create(source_file).expect("Failed to create file");
    let mut source_writer = BufWriter::new(source);

    for line in buffer
    {
        source_writer.write(line.as_bytes()).expect("Failed to write line");
    }
}

fn compare_by_predicate(a: &String, b: &String, tup: &Vec<(&str, usize, usize)>) -> Ordering {
    let mut compare: Ordering;
    let equal = Ordering::Equal;

    for var in tup {
        if var.0 == "s" {
            compare = compare_string_slice(a, b, var.1, var.2);

            if compare != equal
            {
                return compare;
            }
        } else if var.0 == "d" {
            compare = compare_string_date_slice(a, b, var.1, var.2);

            if compare != equal
            {
                return compare;
            }
        }
    }

    equal
}

fn compare_string_slice(a: &String, b: &String, start: usize, end: usize) -> Ordering {
    let slice_a: &str = &a[start..end];
    let slice_b: &str = &b[start..end];

    slice_a.cmp(&slice_b)
}

fn compare_string_date_slice(a: &String, b: &String, start: usize, end: usize) -> Ordering {
    let slice_a: &str = &a[start..end];
    let slice_b: &str = &b[start..end];

    compare_string_dates(&slice_a, &slice_b)
}

fn compare_string_dates(a: &str, b: &str) -> Ordering {
    if a == BLANK_DATE && b == BLANK_DATE {
        return Ordering::Equal;
    } else if a == BLANK_DATE {
        return Ordering::Greater;
    } else if b == BLANK_DATE {
        return Ordering::Less;
    }

    a.cmp(b)
}

// FS

fn files_in_directory(source_path: &str) -> Vec<String> {
    let paths = fs::read_dir(source_path).expect(&format!("Source folder '{}' not located", source_path));
    let mut files: Vec<String> = Vec::new();

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

        if is_dir {
            continue;
        }

        let file_entry = entry.file_name();
        let mut source_file = String::from(source_path);

        source_file.push_str("\\");
        source_file.push_str(file_entry.to_str().expect("File name error"));

        files.push(source_file);
    }

    files
}

fn temporary_directory(root: String) -> String {
    let mut directory = root.clone();
    let my_uuid = Uuid::new_v4().to_simple()
        .to_string();

    directory.push('\\');
    directory.push_str(my_uuid.as_str());

    directory
}

fn root_path() -> String {
    let root_result = env::current_dir();
    let root = root_result.unwrap();
    let root_str_result = root.to_str();
    let root_str = root_str_result.unwrap();

    String::from(root_str)
}

fn drop_file(s: &str) {
    let r = std::fs::remove_file(s);

    match r {
        Ok(_) => {}
        Err(_) => {}
    }
}