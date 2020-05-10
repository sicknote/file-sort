# File Sort

This is a Rustlang utility for sorting large text files without large resource overheads. It scratches an itch to port a C# implementation to Rustlang by way of a test project that is much faster.

## Usage

For a line by line sort, use the following:

```
file-sort.exe "c:\input.file"
```

For a sort on line substrings, use something similar to the following:

```
file-sort.exe "c:\input.file" --sort "s,0,2;s,2,2;s,7,6;s,13,5;s,23,5;s,139,3;s,37,3;s,33,4;s,40,1;s,43,8;s,51,4;s,55,1;s,56,6;d,108,8;s,142,5;d,116,8;d,124,8;s,99,9;"
```

The --sort argument, in this case is as follows:

Each semi-colon separated item is the sort type (s or d), the starting index of the input string, and the length. So s,0,1 is a string comparison starting at zero and having a length of 2. The d type sort is a specific date sort. In this case the date is expected to be in a sortable date format 20200522, and eight spaces is seen as a 'NULL' date. A 'NULL' date is greater than any provided date, giving the ability to sort a date range.

The sorting is currently always ascending, but that may change in future.

## License
[MIT](https://choosealicense.com/licenses/mit/)
