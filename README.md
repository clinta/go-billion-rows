My go solution for the 1 billion row challenge: https://github.com/gunnarmorling/1brc

I've seen a few solutions to this that look interesting, but one thing I wanted to try, which I hadn't seen done is using a memory mapped file to avoid having to buffer the input. This works very well, getting time down to 5 seconds on my laptop. Also a fun learning experience to try using mmap for myself.
