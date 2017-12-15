mkfifo dbpedia.nt.gz.fifo
zcat dbpedia.nt.gz > dbpedia.nt.gz.fifo &
grep -o '^[^#]*' dbpedia.nt.gz.fifo > main_data_file

cut -d" " -f1 main_data_file | awk '!seen[$0]++' | LC_ALL=C sort > file1
cut -d" " -f2 main_data_file | awk '!seen[$0]++' | LC_ALL=C sort > predicates
cut -d" " -f3 main_data_file | awk '!seen[$0]++' | LC_ALL=C sort > file3

comm -23 file1 file3 > outgoing
comm -13 file1 file3 > incoming
comm -12 file1 file3 > both_in_out

rm file1.txt
rm file3.txt
