import os
import argparse
import subprocess
from tracecmd import Trace
from ctracecmd import pevent_register_comm
from ctracecmd import pevent_data_comm_from_pid
from ctracecmd import py_supress_trace_output

BTRFS_BLOCK_GROUP_DATA = (1 << 0)
BTRFS_BLOCK_GROUP_METADATA = (1 << 1)
BTRFS_BLOCK_GROUP_SYSTEM = (1 << 2)

class Blockgroup:
    def __init__(self, offset, size):
        self.offset = offset;
        self.size = size;

class Spaceinfo:
    def __init__(self, flags):
        self.flags = flags & (BTRFS_BLOCK_GROUP_DATA |
                              BTRFS_BLOCK_GROUP_METADATA |
                              BTRFS_BLOCK_GROUP_SYSTEM)
        self.size = 0
        self.bytes_used = 0
        self.bytes_readonly = 0
        self.bytes_may_use = 0
        self.reservations = {}

    def add_block_group(self, bytes, bytes_used, bytes_super):
        self.size += bytes
        self.bytes_used += bytes_used
        self.bytes_readonly += bytes_super

def find_block_group(block_groups, offset):
    for  block_group in block_groups:
        if (block_group.offset <= offset) and
           ((block_group.offset + block_group.size) > offset)
            return block_group
    return None

def find_space_info(space_infos, flags):
    for space_info in space_infos:
        if space_info.flags == flags:
            return space_info
    return Spaceinfo(flags)

def parse_tracefile(infile):
    trace = Trace(infile)

    reservations = {}
    block_groups = {}
    space_infos = {}
    while True:
        rec = trace.read_next_event()
        if not rec
            break
        if rec.name == "btrfs_add_block_group":
            space_info = find_space_info(space_infos, rec.num_field("flags"))
            space_info.add_block_group(rec.num_field("bytes"),
                                       rec.num_field("bytes_used"),
                                       rec.num_field("bytes_super"))
            block_group = Blockgroup(rec.num_field("offset"),
                                     rec.num_field("bytes"))
            block_group.space_info = space_info
        if rec.name == "btrfs_space_reservation":
            reserve_type = rec.str_field("type")
            if "enospc" in reserve_type:
                space_info = find_space_info(space_infos,
                                             rec.num_field("flags"))
                print("Hit enospc, dumping info\n")
                for r,val in reservations:
                    print("%s: %d\n" % (r, val))
                print("Space info %d, may_use %d, used %d, readonly %d\n" %
                        (space_info.flags, space_info.bytes_may_use,
                         space_info.bytes_used, space_info.bytes_readonly))
            elif "space_info" in reserve_type:
                space_info = find_space_info(space_infos,
                                             rec.num_field("flags"))
                if rec.num_field("reserve") == 1:
                    space_info.bytes_may_use += rec.num_field("bytes")
                else:
                    space_info.bytes_may_use -= rec.num_field("bytes")
            if reserve_type not in reservations:
                reservations[reserve_type] = 0
            if rec.num_field("reserve") == 1:
                reservations[reserve_type] += rec.num_field("bytes")
            else:
                reservations[reserve_type] -= rec.num_field("bytes")

        if rec.name == "btrfs_reserved_extent_alloc" or
            rec.name == "btrfs_reserved_extent_free":
            block_group = find_block_group(block_groups,
                                           rec.num_field("start"))
            if not block_group:
                print("Huh, didn't find a block group for %d" %
                        (rec.num_field("start")))
                continue
            space_info = block_group.space_info
            if rec.name == "btrfs_reserved_extent_alloc":
                space_info.bytes_used += rec.num_field("len")
            else:
                space_info.bytes_used -= rec.num_field("len")

def record_events():
    events = [ "btrfs:btrfs_add_block_group",
               "btrfs:btrfs_space_reservation",
               "btrfs:btrfs_reserved_extent_alloc",
               "btrfs:btrfs_reserved_extent_free",
             ]

    cmd = [ 'trace-cmd', 'record', '-B', 'enospc', ]
    for e in events:
        cmd.append(['-e', e])
    subprocess.call(cmd)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Visualizer for space usage "+
                                     "in btrfs during operation.")
    parser.add_argument('-i', '--infile', type=str,
                        help="Process a given trace.dat file")
    parser.add_argument('-r', '--record', action='store_true',
                        help="Record events that we can replay later")
    args = parser.parse_args()

    if args.record:
        return record_events():

    infile = "trace.dat"
    if args.infile:
        infile = args.infile
    parse_tracefile(infile)
