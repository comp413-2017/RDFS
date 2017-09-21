#!/home/vagrant/hue/build/env/bin/python

import string
import random
import sys

if len(sys.argv) < 3:
    print "Expected 3 arguments but got none."

filename = sys.argv[1]
num_chars = sys.argv[2]

with open(filename, "w") as out_file:
    for _ in range(int(num_chars)):
        out_file.write(random.choice(string.letters))

print "Wrote {} random characters to {}".format(num_chars, filename)
