#! /bin/bash

# Usage:
# 	./run_multiple.sh ../path/to/executable [number of iterations]
# Example:
# 	./run_multiple.sh ../build/test/ReplicationTest 200

# If you want to see the output of what's being run, as it's running:
#  	less multiple_runs.log
#  Then press SHIFT + F to tail the file and view changes as they happen


echo "Running $1 for $2 iterations"

for (( i=0; i < $2; ++i))
do
	$1 &> multiple_runs.log
	grep -q "FAILED" "multiple_runs.log"
	if [ $? -eq 0 ] ; then
		echo "    Failed $1 on iteration $i"
		exit
	else
		echo "    Passed $1 on iteration: $i"
	fi
done
echo "All $2 iterations of $1 passed." > multiple_runs.log
