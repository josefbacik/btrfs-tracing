#!/usr/bin/python

import argparse
import re

parser = argparse.ArgumentParser(description="Trace the btrfs cluster allocator")
parser.add_argument('infile', metavar='file', help='Trace file to process')

args = parser.parse_args()

infile = open(args.infile, "r")

find_cluster_re = re.compile(".* (\d+\.\d+): btrfs_find_cluster.*")
cluster_re = re.compile(".* (\d+\.\d+): btrfs_setup_cluster: " +
                        "block_group = (\d+), flags = \d+\(.*\), " +
                        "window_start = (\d+), size = (\d+), " +
                        "max_size = (\d+)")
failed_cluster_re = re.compile(".* (\d+\.\d+): btrfs_failed_cluster_setup.*")
trans_re = re.compile(".*btrfs_transaction_commit.*")

num_setups = 0
avg_setups_per_trans = 0
avg_cluster_size = 0.0
cur_num_setups = 0
max_cluster_size = 0
min_cluster_size = 0
block_groups = []
start_time = 0.0
end_time = 0.0
avg_fail_time = 0.0
avg_setup_time = 0.0
min_setup_time = 0.0
max_setup_time = 0.0
total_setup_time = 0.0
failed_cluster = 0

for line in infile:
    m = find_cluster_re.match(line)
    if m:
        start_time = float(m.group(1))
        continue

    m = cluster_re.match(line)
    if m:
        end_time = float(m.group(1))
        total_setup_time += end_time - start_time
        num_setups += 1
        cur_num_setups += 1
        size = int(m.group(4))
        group = m.group(2)
        if group not in block_groups:
            block_groups.append(group)

        if avg_cluster_size == 0:
            avg_cluster_size = float(size)
        else:
            avg_cluster_size = (avg_cluster_size + float(size)) / 2

        if size > max_cluster_size:
            max_cluster_size = size

        if min_cluster_size == 0 or size < min_cluster_size:
            min_cluster_size = size

        if avg_setup_time == 0:
            avg_setup_time = end_time - start_time
        else:
            avg_setup_time = (avg_setup_time + (end_time - start_time)) / 2

        if min_setup_time == 0 or min_setup_time < (end_time - start_time):
            min_setup_time = end_time - start_time

        if max_setup_time < (end_time - start_time):
            max_start_time = (end_time - start_time)

        continue

    m = failed_cluster_re.match(line)
    if m:
        failed_cluster += 1
        end_time = float(m.group(1))
        if avg_fail_time == 0:
            avg_fail_time = end_time - start_time
        else:
            avg_fail_time = (avg_fail_time + (end_time - start_time)) / 2
        continue

    m = trans_re.match(line)
    if m:
        if avg_setups_per_trans == 0:
            avg_setups_per_trans = cur_num_setups
        elif cur_num_setups != 0:
            avg_setups_per_trans = (avg_setups_per_trans + cur_num_setups) / 2
        cur_num_setups = 0

print("Number of setups:\t\t\t\t%d" % (num_setups))
print("Average cluster size:\t\t\t\t%f" % (avg_cluster_size))
print("Max cluster size:\t\t\t\t%d" % (max_cluster_size))
print("Min cluster size:\t\t\t\t%d" % (min_cluster_size))
print("Average setup time:\t\t\t\t%f" % (avg_setup_time))
print("Min setup time:\t\t\t\t\t%f" % (min_setup_time))
print("Max setup time:\t\t\t\t\t%f" % (max_setup_time))
print("Total setup time:\t\t\t\t%f" % (total_setup_time))
print("Number of failed setups:\t\t\t%d" % (failed_cluster))
print("Average faile time:\t\t\t\t%f" % (avg_fail_time))
print("Average number of setups per transaction:\t%d" %
        (avg_setups_per_trans))
print("Number of block groups used:\t\t\t%d" % (len(block_groups)))
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
