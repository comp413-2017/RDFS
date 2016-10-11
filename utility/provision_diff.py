#!/usr/bin/env python

from os import chdir
from os.path import exists, join

import shutil

header = ['#!/bin/sh', 'set -e', 'set -x']

DIFF_DIR = '/home/vagrant'
DIFF_FILE = 'provision_diff.sh'
BACKUP_FILE = '.vagrant_provision.backup'

if __name__ == '__main__':
    chdir('/home/vagrant/rdfs/')
    # If backup exists, check if it differs from the provision file.
    if exists(BACKUP_FILE):
        with open(BACKUP_FILE) as backup:
            backup_lines = backup.readlines()
            with open('vagrant_provision.sh') as provision:
                diff = [line for line in provision.readlines() if line not in
                        backup_lines]
                if diff:
                    with open(join(DIFF_DIR, DIFF_FILE), 'w') as fdiff:
                        fdiff.writelines(header + diff)
                    print "Detected changes in vagrant provision since last ssh:"
                    print ''.join(diff)
                    print "Use `sudo sh {}` to execute.".format(DIFF_FILE)
                else:
                    print "No changes in vagrant_provision.sh detected."
    # Otherwise just copy provision file into it.
    else:
        print "No backup exists, creating one now..."
    # Update the backup file.
    shutil.copy('vagrant_provision.sh', BACKUP_FILE)
