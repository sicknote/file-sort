extern crate clap;
extern crate stopwatch;

use clap::{App, Arg};
use std::io::{BufReader, BufRead, Write, BufWriter, SeekFrom, Seek};
use std::cmp::Ordering;
use std::{fs, env};
use stopwatch::Stopwatch;
use uuid::Uuid;
use std::fs::File;
use std::collections::VecDeque;

const READER_BUFFER_SIZE: usize = 5_242_880;
const WRITER_BUFFER_SIZE: usize = 5_242_880;
const SPLIT_SIZE: usize = 10_485_760;
const LIMIT: i32 = 1000;
const BLANK_DATE: &str = "        ";

enum CollectionState {
    Drained,
    Ok,
}

fn main() {
    let matches = App::new("TACT File splitter")
        .version("1.0")
        .author("edb")
        .about("Sort files by row/block")
        .arg(Arg::with_name("INPUT")
            .help("Sets the input file to use")
            .required(true)
            .index(1))
        .arg(Arg::with_name("SORT")
            .help("Sets the type of sorting applied to the file")
            .short("s")
            .long("sort")
            .takes_value(true)
            .required(false))
        .get_matches();

    let source_path = matches.value_of("INPUT").unwrap();
    let sort_value: &str;
    let sort_type = matches.value_of("SORT");

    match sort_type {
        None => {
            sort_value = "line";
        }
        Some(st) => {
            sort_value = st;
        }
    }

    let sort_rules = sorter(sort_value);

    let sw_full = Stopwatch::start_new();

    println!("Sorting file: {}", source_path);

    let split_path = split_file(source_path);

    println!("Time: {:?}", sw_full.elapsed());

    sort_files(&split_path, &sort_rules);

    println!("Time: {:?}", sw_full.elapsed());

    //let split_path = "C:\\data\\CLionProjects\\file-sort\\b8deff39284e49048bb46c68b1d94385";

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
    let mut source_reader = BufReader::with_capacity(READER_BUFFER_SIZE, source);
    let mut f = 1;

    loop {
        let mut file = String::from(&target_path);
        let mut buffer: Vec<String> = Vec::new();
        let mut total = 0;

        file.push_str(&format!("\\file.{}", f));

        loop {
            let mut line = String::new();
            let l = source_reader.read_line(&mut line).expect("Failed to read line");

            if l == 0 { // EOF
                break;
            }

            buffer.push(line);
            total += l;

            if total >= SPLIT_SIZE {
                break;
            }
        }

        if buffer.len() == 0 {
            break;
        }

        let target = File::create(file).expect("Failed to open target file");
        let mut target_writer = BufWriter::with_capacity(WRITER_BUFFER_SIZE, target);

        for line in &buffer {
            let b = line.as_bytes();

            target_writer.write(b).expect("Failed to write to file");
        }

        target_writer.flush().expect("Failed to flush writer");
        buffer.clear();

        f = f + 1;
    }

    target_path
}

/// Sorts all the files in source_path in memory
fn sort_files(source_path: &str, sort: &Vec<(&str, usize, usize)>) {
    let paths = files_in_directory(source_path);
    let mut i = 1;

    println!("sort_files: {}", &source_path);

    for path in paths {
        sort_file_contents(source_path, &path, &sort, i);

        i = i + 1;
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

    for _i in 0..files.len() {
        let buffer: VecDeque<String> = VecDeque::new();

        buffers.push(buffer);
        offsets.push(0);
        positions.push(0);
        drained.push(false);
    }

    std::fs::create_dir(&target).expect("Failed to create target directory");

    println!("join_files: {}", &target);

    target.push_str("\\sorted.export");

    let target_file = std::fs::File::create(&target).expect("Failed to create file");
    let mut source_writer = BufWriter::with_capacity(WRITER_BUFFER_SIZE, target_file);

    let mut internal_buffer: VecDeque<(usize, String)> = VecDeque::new();
    let reader_length = buffers.len();
    let mut found = false;

    for current_position in 0..reader_length {
        let buffer: &mut VecDeque<String>;

        match check_collection(&mut buffers, &mut offsets, &mut positions, &mut drained, &files, current_position) {
            CollectionState::Drained => {
                continue;
            }
            CollectionState::Ok => {
                buffer = &mut buffers[current_position];
            }
        }

        let first = &mut buffer.pop_front();

        let current_value: String;

        match first {
            None => {
                panic!("asd");
            }
            Some(s) => {
                current_value = s.to_string();
            }
        }

        if !found {
            found = true;
            let lowest_value = String::from(current_value);
            internal_buffer.push_back((current_position, lowest_value));

            continue;
        }

        let cur_length = internal_buffer.len();

        internal_sort(&mut internal_buffer, &current_value, sort, current_position);

        if internal_buffer.len() == cur_length {
            let lowest_value = String::from(&current_value);

            internal_buffer.push_back((current_position, lowest_value));
        }
    }

    let mut pos: usize;

    loop {
        let first = &mut internal_buffer.pop_front();

        match first {
            None => {
                break;
            }
            Some(s) => {
                pos = s.0;
                let val = &s.1;

                source_writer.write(val.as_bytes()).expect("failed to write to file");
            }
        }

        let buffer: &mut VecDeque<String>;

        match check_collection(&mut buffers, &mut offsets, &mut positions, &mut drained, &files, pos) {
            CollectionState::Drained => {
                continue;
            }
            CollectionState::Ok => {
                buffer = &mut buffers[pos];
            }
        }
        let first = &mut buffer.pop_front();

        let current_value: String;

        match first {
            None => {
                panic!("asd");
            }
            Some(s) => {
                current_value = s.to_string();
            }
        }

        let cur_length = internal_buffer.len();

        internal_sort(&mut internal_buffer, &current_value, sort, pos);

        if internal_buffer.len() == cur_length {
            let lowest_value = current_value;

            internal_buffer.push_back((pos, lowest_value));
        }
    }

    source_writer.flush().expect("File flush failed");

    target
}

fn sorter<'a>(sort_value: &str) -> Vec<(&'a str, usize, usize)> {
    let mut tup: Vec<(&'a str, usize, usize)> = Vec::new();

    if sort_value == "line" {
        tup.push(("l", 0, 0));
    } else {
        let mut sv = String::from(sort_value);
        if !sv.ends_with(";") {
            sv.push_str(";");
        }
        let sort_length = sv.len();
        let mut c: String = "".to_string();
        let mut t: String = "".to_string();
        let mut s: String = "".to_string();
        let mut l: String ;

        for start in 0..sort_length {
            let sl = &sv[start..start + 1];

            if sl == "," {
                if &t == "" {
                    t = String::from(&c);
                } else if &s == "" {
                    s = String::from(&c);
                }
                c.clear();
            } else if sl == ";" {
                l = String::from(&c);
                c.clear();
                let start = s.parse::<usize>().unwrap();
                let length = l.parse::<usize>().unwrap();
                if &t == "s" {
                    tup.push(("s", start, start + length));
                } else if &t == "d" {
                    tup.push(("d", start, start + length));
                }
                t.clear();
                s.clear();
                l.clear();
            } else {
                c.push_str(sl);
            }
        }
    }

    tup
}

fn internal_sort(internal_buffer: &mut VecDeque<(usize, String)>, current_value: &String, sort: &Vec<(&str, usize, usize)>, pos: usize) {
    let cur_length = internal_buffer.len();

    for p in 0..cur_length {
        let t = &internal_buffer[p];
        let compare = compare_by_predicate(&current_value, &t.1, &sort);

        if compare == Ordering::Less {
            let lowest_value = String::from(current_value);

            internal_buffer.insert(p, (pos, lowest_value));

            break;
        }
    }
}

fn check_collection(buffers: &mut Vec<VecDeque<String>>, offsets: &mut Vec<u64>, positions: &mut Vec<usize>, drained: &mut Vec<bool>, files: &Vec<String>, position: usize) -> CollectionState {
    let mut buffer = &mut buffers[position];

    if buffer.is_empty() {
        let is_drained = drained[position];

        if is_drained
        {
            return CollectionState::Drained;
        }

        populate_empty_collection(buffers, offsets, positions, &files, position);

        buffer = &mut buffers[position];

        if buffer.is_empty() {
            drained[position] = true;

            return CollectionState::Drained;
        }
    }

    return CollectionState::Ok;
}

fn populate_empty_collection(buffers: &mut Vec<VecDeque<String>>, offsets: &mut Vec<u64>, positions: &mut Vec<usize>, files: &Vec<String>, position: usize) {
    let mut offset = offsets[position];
    let mut lines = 0;
    let mut buffer: VecDeque<String> = VecDeque::new();
    let path = &files[position];
    let source = std::fs::File::open(path).expect("Failed to open file");
    let mut source_reader = BufReader::new(source);

    source_reader.seek(SeekFrom::Start(offset)).expect("Failed to seek in reader");

    loop {
        let mut line = String::new();
        let l = source_reader.read_line(&mut line).expect("Failed to read line");

        if l == 0 {
            break;
        }

        lines = lines + 1;

        offset = offset + line.len() as u64;

        buffer.push_back(line);

        if lines >= LIMIT
        {
            break;
        }
    }

    offsets[position] = offset;
    buffers[position] = buffer;
    positions[position] = 0;
}

fn sort_file_contents(source_path: &str, source_file: &str, tup: &Vec<(&str, usize, usize)>, i: i32) {
    let mut buffer: Vec<String> = Vec::new();
    let source = std::fs::File::open(source_file).expect("Failed to open file");
    let mut target_file = String::from(source_path);
    let mut source_reader = BufReader::new(source);

    target_file.push_str(&format!("\\sort.{}", i));

    loop {
        let mut line = String::new();
        let l = source_reader.read_line(&mut line).expect("Failed to read line");

        if l == 0 {
            break;
        }

        buffer.push(line);
    }

    buffer.sort_by(|a, b| compare_by_predicate(a, b, &tup));

    let source = std::fs::File::create(&target_file).expect(&format!("Failed to create file: '{}'", &target_file));
    let mut source_writer = BufWriter::new(source);

    for line in buffer
    {
        source_writer.write(line.as_bytes()).expect("Failed to write line");
    }

    fs::remove_file(source_file).expect("Failed to remove file");
}

fn compare_by_predicate(a: &String, b: &String, tup: &Vec<(&str, usize, usize)>) -> Ordering {
    let mut compare: Ordering;
    let equal = Ordering::Equal;

    for var in tup {
        if var.0 == "l" {
            return a.cmp(b);
        } else if var.0 == "s" {
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
