import os
import argparse
import subprocess
import gc
import sys
import time
import binascii
from tracecmd import Trace
from ctracecmd import pevent_register_comm
from ctracecmd import pevent_data_comm_from_pid
from ctracecmd import py_supress_trace_output
from ctracecmd import tracecmd_buffer_instances
from ctracecmd import tracecmd_buffer_instance_handle

NSECS_IN_SEC = 1000000000
reservations = {}
block_groups = []
space_infos = []
flush_events = []

class SpaceHistory:
    def __init__(self):
        self.used_bytes = 0
        self.reserved_bytes = 0
        self.readonly_bytes = 0
        self.total_bytes = 0

        self.running_totals = {}
        self.hists = {}
        self.times = {}
        self.vals = {}
        self.enabled = True

    def add_space(self, name, ts, value):
        if not self.enabled:
            return
        if name not in self.hists:
            self.hists[name] = {}
            self.running_totals[name] = 0

            # We have to back populate the timestamps from the first event we've
            # recorded upt through current time so that all the histories match
            # up
            for n in self.running_totals.keys():
                if n == name:
                    continue
                for t in self.hists[n].keys():
                    self.hists[name][t] = 0
                break
        self.running_totals[name] += value

        for n in self.running_totals.keys():
            self.hists[n][ts] = self.running_totals[n]

    def remove_space(self, name, ts, value):
        if not self.enabled:
            return
        if name not in self.hists:
            print("WOOOOOOOPPPPSSSS!")
            self.hists[name] = {}
            self.running_totals[name] = 0
        self.running_totals[name] -= value

        for n in self.running_totals.keys():
            self.hists[n][ts] = self.running_totals[n]

    def _build_list(self, hist, ts_start, ts_end):
        times = sorted(list(hist.keys()))
        vals = []
        start = None
        end = 0
        for i in range(0, len(times)):
            ts = times[i]
            if ts_start > 0:
                if ts < ts_start:
                    continue
                elif start is None:
                    start = i
            if ts_end > 0 and ts > ts_end:
                end = i
                break
            vals.append(hist[ts])
        if start is not None:
            times = times[start:end]
        return times, vals

    def _average_down_list(self, orig_list, max_len):
        orig_len = len(orig_list)
        if orig_len <= max_len:
            return orig_list
        scale = int(orig_len / max_len)
        l = []
        value = 0
        cur = 0
        for i in orig_list:
            value += i
            cur += 1
            if cur == scale:
                l.append(int(value / scale))
                cur = 0
                value = 0
        if cur:
            l.append(int(value / cur))
        return l

    def _scale_down_list(self, orig_list, max_len):
        orig_len = len(orig_list)
        if orig_len <= max_len:
            return orig_list
        scale = int(orig_len / max_len)
        l = []
        cur = 0
        while cur < orig_len:
            l.append(orig_list[cur])
            cur += scale
        if len(l) < max_len:
            l.append(orig_list[-1])
        return l

    def build_lists(self, max_vals=0, ts_start=0, ts_end=0):
        for n in self.hists.keys():
            print("length of hist %s is %d" % (n, len(self.hists[n].keys())))
            self.times[n], self.vals[n] = self._build_list(self.hists[n],
                                                           ts_start, ts_end)
        if max_vals:
            for n in self.times.keys():
                self.times[n] = self._scale_down_list(self.times[n], max_vals)
                self.vals[n] = self._average_down_list(self.vals[n], max_vals)

class Blockgroup:
    def __init__(self, offset, size):
        self.offset = offset
        self.size = size

class Spaceinfo:
    BTRFS_BLOCK_GROUP_DATA = (1 << 0)
    BTRFS_BLOCK_GROUP_SYSTEM = (1 << 1)
    BTRFS_BLOCK_GROUP_METADATA = (1 << 2)

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

def pretty_size(size):
    names = ["bytes", "kib", "mib", "gib", "tib"]
    i = 0
    while size > 1024:
        size /= 1024
        i += 1
    return str(size) + names[i]

def add_bg(rec):
    ret = "read, "
    if rec.num_field("create") == 1:
        ret = "create, "
    flags = rec.num_field("flags")
    if Spaceinfo.BTRFS_BLOCK_GROUP_DATA & flags:
        ret += "BTRFS_BLOCK_GROUP_DATA, "
    elif Spaceinfo.BTRFS_BLOCK_GROUP_METADATA & flags:
        ret += "BTRFS_BLOCK_GROUP_METADATA, "
    else:
        ret += "BTRFS_BLOCK_GROUP_SYSTEM, "
    ret += pretty_size(rec.num_field("size"))
    return ret

def flush_event(rec):
    state = rec.num_field("state")
    event_str = ""
    if state == 1:
        event_str += "FLUSH_DELAYED_ITEMS_NR: "
    elif state == 2:
        event_str += "FLUSH_DELAYED_ITEMS: "
    elif state == 3:
        event_str += "FLUSH_DELALLOC: "
    elif state == 4:
        event_str += "FLUSH_DELALLOC_WAIT: "
    elif state == 5:
        event_str += "ALLOC_CHUNK: "
    elif state == 6:
        event_str += "COMMIT_TRANS: "
    event_str += "num_bytes = "
    event_str += pretty_size(rec.num_field("num_bytes"))
    event_str += ", orig_bytes = "
    event_str += pretty_size(rec.num_field("orig_bytes"))
    event_str += ", ret = " + str(rec.num_field("ret"))
    return event_str

def record_space(flags, mixed_bg):
    if flags & Spaceinfo.BTRFS_BLOCK_GROUP_METADATA:
        return True
    if not mixed_bg:
        return False
    if flags & Spaceinfo.BTRFS_BLOCK_GROUP_DATA:
        return True
    return False

def parse_tracefile(args, space_history):
    trace = Trace(args.infile)

    cpustats = trace.cpustats()

    # The format is "Buffer: name\n\n\nCpu0: blah\n\nCpu1: blah\n\n"
    cpus = cpustats.split('\n\n\n')
    cpus = cpus[1].split('\n\n')

    instances = tracecmd_buffer_instances(trace._handle)
    if instances != 0:
        new_handle = tracecmd_buffer_instance_handle(trace._handle, 0)
        trace._handle = new_handle

    total_events = 0
    for cpu in range(0, trace.cpus):
        stats = dict(item.split(':') for item in cpus[cpu].split("\n"))
        total_events += int(stats['read events'])

    print("Total events %d" % (total_events))
    # Just assume 10k events per second to start off with
    rem = total_events / 10000

    cur_event = 0
    obj_count = 0
    enospc_flushes = 0
    preempt_flushes = 0
    start_time = time.time()
    rem = 0
    run_limit = -1
    mixed_bg = False

    seen_uuids = []
    if args.fsid:
        seen_uuids.append(args.fsid)

    while True:
        if obj_count > 100000:
            now = time.time()
            t = now - start_time
            start_time = now
            t /= obj_count
            rem = total_events - cur_event
            rem *= t
            gc.collect()
            obj_count = 1
        sys.stdout.write("\r%d - %s seconds remaining" % (cur_event, rem))
        sys.stdout.flush()
        rec = trace.read_next_event()
        if rec is None:
            break

        # First figure out if we have a run time limit
        if run_limit == -1:
            if args.time:
                run_limit = rec.ts + (args.time * NSECS_IN_SEC)
            else:
                run_limit = 0
        if run_limit > 0 and rec.ts > run_limit:
            break
        cur_event += 1
        obj_count += 1

        if "fsid" in rec:
            # Deal with multiple fsid's in the trace data
            fsid = binascii.hexlify(rec["fsid"].data)
            if fsid not in seen_uuids:
                if len(seen_uuids):
                    print("\nSaw a new uuid %s" % fsid)
                seen_uuids.append(fsid)
            if fsid != seen_uuids[0]:
                continue

        if rec.name == "btrfs_add_block_group":
            event_str = add_bg(rec)
            flush_events.append([rec.ts, rec.pid, rec.cpu, rec.name, event_str])
            flags = rec.num_field("flags")

            # We only care about metadata for space history
            if flags & Spaceinfo.BTRFS_BLOCK_GROUP_METADATA:
                if flags & Spaceinfo.BTRFS_BLOCK_GROUP_DATA and not mixed_bg:
                    print("\nMixed block group discovered")
                    mixed_bg = True
                space_history.add_space("Total", rec.ts, rec.num_field("size"))
                space_history.add_space("Used", rec.ts,
                                        rec.num_field("bytes_used"))
                space_history.add_space("Readonly", rec.ts,
                                        rec.num_field("bytes_super"))
            space_info = find_space_info(flags)
            space_info.add_block_group(rec.num_field("size"),
                                       rec.num_field("bytes_used"),
                                       rec.num_field("bytes_super"))
            block_group = Blockgroup(rec.num_field("offset"),
                                     rec.num_field("size"))
            block_groups.append(block_group)
            block_group.space_info = space_info
        if rec.name == "btrfs_space_reservation":
            reserve_type = rec.str_field("type")
            reserve = rec.num_field("reserve")
            if "enospc" in reserve_type:
                space_info = find_space_info(rec.num_field("val"))
                if args.nogtk:
                    print("\nHit enospc, dumping info\n")
                    for r in reservations.keys():
                        print("%s: %d" % (r, reservations[r]))
                    print("Space info %d, may_use %d, used %d, readonly %d\n" %
                            (space_info.flags, space_info.bytes_may_use,
                             space_info.bytes_used, space_info.bytes_readonly))
                flush_events.append([rec.ts, rec.pid, rec.cpu, reserve_type,
                                     str(rec.num_field("bytes"))])
                continue
            elif "space_info" in reserve_type:
                space_info = find_space_info(rec.num_field("val"))
                if reserve == 1:
                    space_info.bytes_may_use += rec.num_field("bytes")
                    space_history.add_space("Reserved", rec.ts,
                                            rec.num_field("bytes"))
                else:
                    space_history.remove_space("Reserved", rec.ts,
                                               rec.num_field("bytes"))
                    space_info.bytes_may_use -= rec.num_field("bytes")
            elif "pinned" in reserve_type:
                space_info = find_space_info(rec.num_field("val"))
                if reserve == 0:
                    if record_space(space_info.flags, mixed_bg):
                        space_history.remove_space("Used", rec.ts,
                                                   rec.num_field("bytes"))
                    space_info.bytes_used -= rec.num_field("bytes")
            else:
                if reserve == 1:
                    space_history.add_space(reserve_type, rec.ts,
                                            rec.num_field("bytes"))
                else:
                    space_history.remove_space(reserve_type, rec.ts,
                                               rec.num_field("bytes"))
            if reserve_type not in reservations:
                reservations[reserve_type] = 0
            if reserve == 1:
                reservations[reserve_type] += rec.num_field("bytes")
            else:
                reservations[reserve_type] -= rec.num_field("bytes")
        # For now just ignore btrfs_reserved_extent_alloc because we're not
        # differentiating between bytes_reserved and bytes_used, we're just
        # assuming they are the same.
        if (rec.name == "btrfs_reserved_extent_free" or
            rec.name == "btrfs_reserve_extent"):
            block_group = find_block_group(rec.num_field("start"))
            if not block_group:
                print("Huh, didn't find a block group for %d" %
                        (rec.num_field("start")))
                continue
            space_info = block_group.space_info
            if rec.name == "btrfs_reserve_extent":
                if record_space(space_info.flags, mixed_bg):
                    space_history.add_space("Used", rec.ts,
                                            rec.num_field("len"))
                space_info.bytes_used += rec.num_field("len")
            else:
                if record_space(space_info.flags, mixed_bg):
                    space_history.remove_space("Used", rec.ts,
                                               rec.num_field("len"))
                space_info.bytes_used -= rec.num_field("len")
        if rec.name == "btrfs_trigger_flush":
            if rec.str_field("reason") == "enospc":
                enospc_flushes += 1
            else:
                preempt_flushes += 1
            flush_events.append([rec.ts, rec.pid, rec.cpu, rec.name, rec.str_field("reason")])
        if rec.name == "btrfs_flush_space":
            event_str = flush_event(rec)
            flush_events.append([rec.ts, rec.pid, rec.cpu, rec.name, event_str])

    print("\nNumber of flushes triggered: enospc = %d, preempt = %d" %
          (enospc_flushes, preempt_flushes))
    # If we had a run limit we don't want to do the leak detection as it will be
    # wrong
    if run_limit > 0:
        return

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


def rescale_cb(window, space_history, ts_start, ts_end):
    window.liststore.clear()
    print("ts_start == %ld, ts_end == %ld" % (ts_start, ts_end))
    for events in flush_events:
        if ts_start == 0 or events[0] >= ts_start:
            if ts_end == 0 or events[0] <= ts_end:
                window.add_flush_event(events)
    space_history.build_lists(4096, ts_start, ts_end)
    for n in space_history.times.keys():
        window.darea.update_datapoints(n, space_history.times[n],
                                       space_history.vals[n])

def color_index(index):
    colors = [ (1, 1, 0),
               (0, 1, 0),
               (1, 0, 0),
               (0, 0, 1),
               (1, 0, 1),
               (0, 1, 1),
               (.5, 0, 0),
               (0, .5, 0),
               (0, 0, .5),
               (.5, .5, 0),
               (.5, 0, .5),
               (0, .5, .5),
               (.5, .5, .5)]
    return colors[index]

def visualize_space(args, space_history):
    from graphscreen import GraphWindow
    max_vals = 0
    if args.average:
        # A completely arbitrary limit
        max_vals = 4096
    space_history.build_lists(max_vals)

    window = GraphWindow()
    window.set_rescale_cb(rescale_cb, space_history)
    i = 0
    for n in space_history.times.keys():
        window.add_datapoints(n, space_history.times[n], space_history.vals[n],
                              color_index(i))
        i += 1
    for events in flush_events:
        window.add_flush_event(events)
    window.main()

def record_events():
    events = [ "btrfs:btrfs_add_block_group",
               "btrfs:btrfs_space_reservation",
               "btrfs:btrfs_reserved_extent_free",
               "btrfs:btrfs_trigger_flush",
               "btrfs:btrfs_flush_space",
               "btrfs:btrfs_reserve_extent",
             ]

    cmd = [ 'trace-cmd', 'record', '-B', 'enospc', '-b', '20480', ]
    for e in events:
        cmd.extend(['-e', e])
    subprocess.call(cmd)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Visualizer for space usage "+
                                     "in btrfs during operation.")
    parser.add_argument('-i', '--infile', type=str, default="trace.dat",
                        help="Process a given trace.dat file")
    parser.add_argument('-r', '--record', action='store_true',
                        help="Record events that we can replay later")
    parser.add_argument('-c', '--nogtk', action='store_true',
                        help="Don't display gtk window, just do the leak check")
    parser.add_argument('-t', '--time', type=int,
                        help="Limit the parsing to the given amount of seconds")
    parser.add_argument('-a', '--average', action='store_true',
                        help="Average a large dataset over its time series")
    parser.add_argument('-f', '--fsid', type=str,
                        help="Specify the fsid we care about in the trace file")
    args = parser.parse_args()

    if args.record:
        record_events()
    else:
        py_supress_trace_output()
        space_history = SpaceHistory()
        if args.nogtk:
            space_history.enabled = False
        parse_tracefile(args, space_history)
        if not args.nogtk:
            visualize_space(args, space_history)
