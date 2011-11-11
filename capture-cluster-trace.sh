#!/bin/bash

>/sys/kernel/debug/tracing/trace
echo 1 > /sys/kernel/debug/tracing/events/btrfs/btrfs_transaction_commit/enable
echo 1 > /sys/kernel/debug/tracing/events/btrfs/btrfs_setup_cluster/enable
echo 1 > /sys/kernel/debug/tracing/events/btrfs/btrfs_find_cluster/enable
echo 1 > /sys/kernel/debug/tracing/events/btrfs/btrfs_failed_cluster_setup/enable
echo 1 > /sys/kernel/debug/tracing/events/btrfs/find_free_extent/enable
echo 1 > /sys/kernel/debug/tracing/events/btrfs/btrfs_reserve_extent/enable

cat /sys/kernel/debug/tracing/trace_pipe > $1
