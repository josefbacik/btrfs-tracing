import os
import argparse
import subprocess
from tracecmd import Trace
from ctracecmd import pevent_register_comm
from ctracecmd import pevent_data_comm_from_pid
from ctracecmd import py_supress_trace_output
from ctracecmd import tracecmd_buffer_instances
from ctracecmd import tracecmd_buffer_instance_handle
from graphscreen import GraphWindow
import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk

NSECS_IN_SEC = 1000000000
reservations = {}
block_groups = []
space_infos = []

class SpaceHistory:
    def __init__(self):
        self.used_bytes = 0
        self.reserved_bytes = 0
        self.readonly_bytes = 0
        self.total_bytes = 0

        self.total_hist = {}
        self.used_hist = {}
        self.reserved_hist = {}
        self.readonly_hist = {}

    def add_space(self, ts, total=0, used=0, reserved=0, readonly=0):
        self.total_bytes += total
        self.total_hist[ts] = self.total_bytes
        self.used_bytes += used
        self.used_hist[ts] = self.used_bytes
        self.reserved_bytes += reserved
        self.reserved_hist[ts] = self.reserved_bytes
        self.readonly_bytes += readonly
        self.readonly_hist[ts] = self.readonly_bytes

    def remove_space(self, ts, total=0, used=0, reserved=0, readonly=0):
        self.total_bytes -= total
        self.total_hist[ts] = self.total_bytes
        self.used_bytes -= used
        self.used_hist[ts] = self.used_bytes
        self.reserved_bytes -= reserved
        self.reserved_hist[ts] = self.reserved_bytes
        self.readonly_bytes -= readonly
        self.readonly_hist[ts] = self.readonly_bytes

class Blockgroup:
    def __init__(self, offset, size):
        self.offset = offset;
        self.size = size;

class Spaceinfo:
    BTRFS_BLOCK_GROUP_DATA = (1 << 0)
    BTRFS_BLOCK_GROUP_METADATA = (1 << 1)
    BTRFS_BLOCK_GROUP_SYSTEM = (1 << 2)

    def __init__(self, flags):
        self.flags = flags & (self.BTRFS_BLOCK_GROUP_DATA |
                              self.BTRFS_BLOCK_GROUP_METADATA |
                              self.BTRFS_BLOCK_GROUP_SYSTEM)
        self.size = 0
        self.bytes_used = 0
        self.bytes_readonly = 0
        self.bytes_may_use = 0

    def add_block_group(self, size, bytes_used, bytes_super):
        self.size += size
        self.bytes_used += bytes_used
        self.bytes_readonly += bytes_super

def find_block_group(offset):
    for  block_group in block_groups:
        if (block_group.offset <= offset) and ((block_group.offset + block_group.size) > offset):
            return block_group
    return None

def find_space_info(flags):
    for space_info in space_infos:
        if space_info.flags == flags:
            return space_info
    space_info = Spaceinfo(flags)
    space_infos.append(space_info)
    return space_info

def parse_tracefile(infile, space_history):
    trace = Trace(infile)

    instances = tracecmd_buffer_instances(trace._handle)
    if instances != 0:
        new_handle = tracecmd_buffer_instance_handle(trace._handle, 0)
        trace._handle = new_handle

    while True:
        rec = trace.read_next_event()
        if not rec:
            break
        if rec.name == "btrfs_add_block_group":
            space_history.add_space(rec.ts, total=rec.num_field("size"),
                                    used=rec.num_field("bytes_used"),
                                    readonly=rec.num_field("bytes_super"))

            space_info = find_space_info(rec.num_field("flags"))
            space_info.add_block_group(rec.num_field("size"),
                                       rec.num_field("bytes_used"),
                                       rec.num_field("bytes_super"))
            block_group = Blockgroup(rec.num_field("offset"),
                                     rec.num_field("size"))
            block_groups.append(block_group)
            block_group.space_info = space_info
        if rec.name == "btrfs_space_reservation":
            reserve_type = rec.str_field("type")
            if "enospc" in reserve_type:
                space_info = find_space_info(rec.num_field("val"))
                print("Hit enospc, dumping info\n")
                for r,val in reservations:
                    print("%s: %d\n" % (r, val))
                print("Space info %d, may_use %d, used %d, readonly %d\n" %
                        (space_info.flags, space_info.bytes_may_use,
                         space_info.bytes_used, space_info.bytes_readonly))
            elif "space_info" in reserve_type:
                space_info = find_space_info(rec.num_field("val"))
                if rec.num_field("reserve") == 1:
                    space_info.bytes_may_use += rec.num_field("bytes")
                    space_history.add_space(rec.ts,
                                            reserved=rec.num_field("bytes"))
                else:
                    space_history.remove_space(rec.ts,
                                               reserved=rec.num_field("bytes"))
                    space_info.bytes_may_use -= rec.num_field("bytes")
            if reserve_type not in reservations:
                reservations[reserve_type] = 0
            if rec.num_field("reserve") == 1:
                reservations[reserve_type] += rec.num_field("bytes")
            else:
                reservations[reserve_type] -= rec.num_field("bytes")
        if rec.name == "btrfs_reserved_extent_alloc" or rec.name == "btrfs_reserved_extent_free":
            block_group = find_block_group(rec.num_field("start"))
            if not block_group:
                print("Huh, didn't find a block group for %d" %
                        (rec.num_field("start")))
                continue
            space_info = block_group.space_info
            if rec.name == "btrfs_reserved_extent_alloc":
                space_history.add_space(rec.ts, used=rec.num_field("len"))
                space_info.bytes_used += rec.num_field("len")
            else:
                space_history.remove_space(rec.ts, used=rec.num_field("len"))
                space_info.bytes_used -= rec.num_field("len")
    num_leaks = 0
    for space_info in space_infos:
        if space_info.bytes_may_use != 0:
            print("Bytes may use leak for space info %d, bytes_may_use %d"  %
                  (space_info.flags, space_info.bytes_may_use))
            num_leaks += 1
    for name,value in reservations.iteritems():
        if value != 0:
            print("Reservation for %s outstanding, value %d" % (name, value))
            num_leaks += 1

    if num_leaks == 0:
        print("Yay no leaks!")

def visualize_space(space_history):
    max_size = 0

    total_times = sorted(list(space_history.total_hist.keys()))
    total_vals = []
    for ts in total_times:
        total_vals.append(space_history.total_hist[ts])

    used_times = sorted(list(space_history.used_hist.keys()))
    used_vals = []
    for ts in used_times:
        if space_history.used_hist[ts] > max_size:
            max_size = space_history.used_hist[ts]
        used_vals.append(space_history.used_hist[ts])

    reserved_times = sorted(list(space_history.reserved_hist.keys()))
    reserved_vals = []
    for ts in reserved_times:
        if space_history.reserved_hist[ts] > max_size:
            max_size = space_history.reserved_hist[ts]
        reserved_vals.append(space_history.reserved_hist[ts])

    readonly_times = sorted(list(space_history.readonly_hist.keys()))
    readonly_vals = []
    for ts in readonly_times:
        if space_history.readonly_hist[ts] > max_size:
            max_size = space_history.readonly_hist[ts]
        readonly_vals.append(space_history.readonly_hist[ts])

    window = GraphWindow()
    window.add_datapoints("Total", total_times, total_vals, (1, 1, 0))
    window.add_datapoints("Used", used_times, used_vals, (0, 1, 0))
    window.add_datapoints("Reserved", reserved_times, reserved_vals, (1, 0, 0))
    window.add_datapoints("Readonly", readonly_times, readonly_vals, (0, 0, 1))
    window.show_all()
    Gtk.main()

def record_events():
    events = [ "btrfs:btrfs_add_block_group",
               "btrfs:btrfs_space_reservation",
               "btrfs:btrfs_reserved_extent_alloc",
               "btrfs:btrfs_reserved_extent_free",
             ]

    cmd = [ 'trace-cmd', 'record', '-B', 'enospc', ]
    for e in events:
        cmd.extend(['-e', e])
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
        record_events()
    else:
        py_supress_trace_output()
        infile = "trace.dat"
        if args.infile:
            infile = args.infile
        space_history = SpaceHistory()
        parse_tracefile(infile, space_history)
        visualize_space(space_history)
