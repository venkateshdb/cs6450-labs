Questions to investigate:
1. For regular workload, seems like the commit/abort rations are still too high, is implementation OK?
I'm getting the following numbers:
75k commits/s  for ./run-cluster.sh 2 2 "" "-workers 16"
111k commits/s for ./run-cluster.sh 2 2 "" "-workers 64"

2. Xfer workload works and can achieve linearliablility. To run workload: 
./run-cluster.sh 3 1 "" "-workers 4 -workload xfer"
DO NOT set high worker numbers(e.g.) otherwise it will not make any progress.
One thing interesting about the xfer workload is how the destination account can affect performance.
Using ./run-cluster.sh 3 1 "" "-workers 4 -workload xfer" for both.
If we use the immediate next account as the dest, we get about 1000~1100 commits/s
If we use a random account besides the src account itself, we get about 800 commits/s
Also notice the pattern of account balances, seems like after a few hunder xfers only a few accounts have
non-zero balance.


