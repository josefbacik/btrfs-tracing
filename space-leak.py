#!/usr/bin/python

import argparse
import re

class ReservationPool:
    def __init__(self, name):
        self.name = name
        self.mydict = {}
        self.pools = 0
        self.over_released = 0
    def handle_action(self, actor, action, size):
        if action == "reserve":
            if size == 0:
                return 0
#            print("%s: reserved %d for %s" % (self.name, size, actor))
            if actor in self.mydict:
                self.mydict[actor] += size
            else:
                self.pools += 1
                self.mydict[actor] = size
        elif action == "release":
            if size == 0:
                return 0
#            print("%s: released %d for %s" % (self.name, size, actor))
            if actor in self.mydict:
                if self.mydict[actor] < size:
                    print("%s: trying to release %d when we only have %d for %s"
                            % (self.name, size, self.mydict[actor], actor))
                    return -1
                else:
                    self.mydict[actor] -= size
                    if self.mydict[actor] == 0:
                        self.pools -= 1
                        del self.mydict[actor]
            else:
                print("%s: trying to release %d for actor %s who isn't there" %
                        (self.name, size, actor))
                return -1
        else:
            print("Unhandled operation")
            return -1
        return 0

class Filesystem:
    def __init__(self, uuid):
        self.uuid = uuid
        self.transactions = ReservationPool("transaction")
        self.delayed_items = ReservationPool("delayed_items")
        self.delayed_inodes = ReservationPool("delayed_inodes")
        self.delalloc = ReservationPool("delalloc")
        self.orphan = ReservationPool("orphan")
        self.ino_cache = ReservationPool("ino_cache")
        self.space_info = ReservationPool("space_info")

        self.types = {"transaction" : self.transactions,
                        "delayed_item" : self.delayed_items,
                        "delayed_inode" : self.delayed_inodes,
                        "delalloc" : self.delalloc,
                        "orphan" : self.orphan,
                        "ino_cache" : self.ino_cache,
                        "space_info" : self.space_info}

parser = argparse.ArgumentParser(description="Detect space leaks")
parser.add_argument('infile', metavar='file', help='Trace file to process')

args = parser.parse_args()

infile = open(args.infile, "r")

line_re = re.compile(".* (.*): (.*): (.*) (.*) (\d+)")
other_line_re = re.compile(".* (.*): (.*): (.*) (.*) (\d+) bytes \d+ flags \d+")

fses = {}

failed_size = 0
for line in infile:
    m = other_line_re.match(line)
    if not m:
        m = line_re.match(line)
    if not m:
        print("didn't recognize that")
        continue
    if m.group(1) not in fses:
        print("Creating fs %s" % m.group(1))
        fses[m.group(1)] = Filesystem(m.group(1))
    fs = fses[m.group(1)]
    if m.group(2) not in fs.types:
        print("Could not find handler for line '%s'" % line)
        continue
    myclass = fs.types[m.group(2)]
    actor = m.group(3)
    action = m.group(4)
    size = int(m.group(5))
    if myclass.handle_action(actor, action, size) == -1:
        print("Failed on fs %s, line '%s'" % (m.group(1), line.rstrip()))
        failed_size += size

print("Total failed size: %d bytes" % failed_size)

total = 0
for name,fs in fses.iteritems():
    print("Dumping leaked info for %s" % name)
    for pname, pool in fs.types.iteritems():
        if pool.pools != 0:
            print("%s has %d outstanding pools" % (pname, pool.pools))
            ptotal = 0
            for actor,size in pool.mydict.iteritems():
                print("%s: %d" % (actor, size))
                total += size
                ptotal += size
            print("%s leaked %d bytes" % (pname, ptotal))

print("Total leaked: %d bytes" % total)

