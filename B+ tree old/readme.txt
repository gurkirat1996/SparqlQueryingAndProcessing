**seems to work now**

Stepwise
dump_numbers.cc is for building the tree
cli.cc is for both fwd and rev querying
Important commands-
Sorting: sort -parallel=n -uo list-sorted.txt list.txt
To run manually: ./bpt_dump_numbers fwd/fwd_ rev/rev_ sorted-data.txt
(change needed - in dump_numbers.cc, comment lines 155-158 and 184)

1)To  make 4 files
$ bash commands.sh

2)To make database (takes care of everything, input should be a compressed file)
$ bash run.sh
enter name of database when asked

Output on terminal of the form (sample): 
Thread 1 will make 100 databases
..
thread 1 200 //meaning thread 1 has completed making 100 dbs (both fwd and rev)
...

4)To query fwd or rev, put all the queries in a file (say "queries")
For fwd : ./bpt_cli fwd/fwd_ rev/rev_ sorted_data_file 1 queries
For rev : ./bpt_cli fwd/fwd_ rev/rev_ sorted_data_file 2 queries


Output : Following files
1) times.txt : time taken to made each database
2) output_fwd/output_rev

Others-
To generate random string, run data_gen.cc
To generate a random list of numbers for reverse queries, you can run
python random_numbers.py > queries

You may need to increase open file limit (upto 20000)
