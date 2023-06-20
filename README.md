# Leaderboard
Large scale leaderboard stored in cassandra.

## Architecture
Each leaderboard is split into blocks of 1000 entries called buckets.  
Buckets are indexed in acending order by their score.  
Eg. the biggest score in the board is the first in the first bucket.
The top ten are 0-10 in bucket 0.
This grouping allows for fast insertion and selection of entries.

