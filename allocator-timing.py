#!/usr/bin/python

import argparse
import re

class Type:
    Data, Metadata, System = range(3)

class Allocation:
    def __init__(self):
        self.process = ""
        self.cpu = 0
        self.root = ""
        self.start_time = 0.0
        self.type = None

parser = argparse.ArgumentParser(description="Get timing info out of an " +
                                    "allocator trace")
parser.add_argument('infile', metavar='file', help='Trace file to process')

args = parser.parse_args()

infile = open(args.infile, "r")

header_re = re.compile("\s+(.*\d+)\s+\[(\d+)\]\s+(\d+\.\d+): .*")
find_re = re.compile(".*find_free_extent: root = (\d+\(.*\)), len = (\d+)," +
                     " empty_size = (\d+), flags = (\d+)\((.*)\)")
reserve_re = re.compile(".*btrfs_reserve_extent:.*")

state_dict = {}
meta_times = []
data_times = []
system_times = []

for line in infile:
    m = header_re.match(line)
    if not m:
        print("That didnt work, m is " + m)
        continue

    process = m.group(1)
    cpu = int(m.group(2))
    time = float(m.group(3))

    m = find_re.match(line)
    if m:
        alloc = Allocation()
        alloc.process = process
        alloc.cpu = cpu
        alloc.root = m.group(1)
        alloc.start_time = time
        if "METADATA" in m.group(5):
            alloc.type = Type.Metadata
        elif "DATA" in m.group(5):
            alloc.type = Type.Data
        elif "SYSTEM" in m.group(5):
            alloc.type = Type.System

        state_dict[process] = alloc
        continue

    m = reserve_re.match(line)
    if m:
        if process in state_dict:
            a = state_dict[process]
            del state_dict[process]
            run_time = time - a.start_time
            if a.type == Type.Data:
                data_times.append(run_time)
            elif a.type == Type.Metadata:
                meta_times.append(run_time)
            elif a.type == Type.System:
                system_times.append(run_time)
            else:
                print("type didnt match")
        else:
            print("Couldn't find process in the state dict")
        continue

total_time = float(sum(data_times) + sum(meta_times) + sum(system_times))
total_len = len(data_times) + len(meta_times) + len(system_times)

meta_times.sort()
data_times.sort()
system_times.sort()

print("Totals:")
print("\tTotal time:\t%f" % total_time)
print("\tAverage time:\t%f" % float(total_time / total_len))
print("\tAllocations:\t%d" % total_len)
print("Metadata:")
print("\tTotal time:\t%f" % float(sum(meta_times)))
print("\tAverage time:\t%f" % float(sum(meta_times) / len(meta_times)))
print("\tMin time:\t%f" % meta_times[0])
print("\tMax time:\t%f" % meta_times[-1])
print("\tAllocations:\t%d" % len(meta_times))
print("Data:")
print("\tTotal time:\t%f" % float(sum(data_times)))
print("\tAverage time:\t%f" % float(sum(data_times) / len(data_times)))
print("\tMin time:\t%f" % data_times[0])
print("\tMax time:\t%f" % data_times[-1])
print("\tAllocations:\t%d" % len(data_times))
if len(system_times) > 0:
    print("System:")
    print("\tTotal time:\t%f" % float(sum(system_times)))
    print("\tAverage time:\t%f" % float(sum(system_times) / len(system_times)))
    print("\tMin time:\t%f" % system_times[0])
    print("\tMax time:\t%f" % system_times[-1])
    print("\tAllocations:\t%d" % len(system_times))

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
