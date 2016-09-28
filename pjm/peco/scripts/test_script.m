#!/usr/local/bin/WolframKernel -script

files = $CommandLine[[4;;]];

stdout = Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#, ", "]]&;

writeFunc @ files;

Quit[];
