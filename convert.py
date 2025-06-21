#!/usr/bin/env python3

'''
Based on traces from the OSCA paper:

Yu Zhang, Ping Huang, Ke Zhou, Hua Wang, Jianying Hu, Yongguang Ji, and Bin Cheng.
"OSCA: An Online-Model Based Cache Allocation Scheme in Cloud Block Storage Systems."
In *Proceedings of the 2020 USENIX Annual Technical Conference (USENIX ATC 20)*, pp. 785–798.
USENIX Association, July 2020. https://www.usenix.org/conference/atc20/presentation/zhang-yu
'''

from cachetools import LRUCache
from math import floor, ceil
import tarfile
import struct
import sys
import os

class FileCache:

    def __init__(self, maxsize=128):
        self.cache = LRUCache(maxsize=maxsize)


    def get(self, path):
        if path in self.cache:
            return self.cache[path]

        if len(self.cache) >= self.cache.maxsize:
            _, old_file = self.cache.popitem()
            old_file.close()

        file = open(path, 'ab')
        self.cache[path] = file
        return file


    def close_all(self):
        for fh in self.cache.values():
            fh.close()
        self.cache.clear()


'''
Was getting complaints about having too many files open, and repeatedly acquiring file
descriptors will take forever, so instead use a cache to keep frequently accessed files
open and close files that haven't been written in some time. How ironic.
'''
file_cache = FileCache()

def align_io(offset, size, align_to=4096):
    begin, end = offset, offset + size
    begin_aligned = floor(begin / align_to) * align_to
    end_aligned = ceil(end / align_to) * align_to
    aligned = list(range(begin_aligned, end_aligned, align_to))
    return [(i, align_to) for i in aligned]


def write_to_trace(input_str: str):
    for line in input_str.strip().split('\n'):
        if not line.strip():
            continue

        parts = line.strip().split(',')
        if len(parts) != 5:
            raise ValueError(f"Invalid line: {line}")

        offset = int(parts[1])  # uint64_t
        size = int(parts[2])    # uint32_t
        io_type = int(parts[3]) # uint8_t
        assert(io_type == 0 or io_type == 1)
        volume_id = int(parts[4])

        os.makedirs('out', exist_ok=True)
        trace_name = f'out/{volume_id}.bin'

        for aligned_offset, aligned_size in align_io(offset, size):
            packed = struct.pack('<QIB', aligned_offset, aligned_size, io_type)
            trace_file = file_cache.get(trace_name)
            trace_file.write(packed)
            trace_file.flush()


if __name__ == "__main__":

    with tarfile.open(sys.argv[1], "r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue

            f = tar.extractfile(member)
            if not f:
                continue

            content = f.read()
            print(member.name)
            write_to_trace(content.decode(errors="replace"))

    file_cache.close_all()
