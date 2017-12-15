rm -r temp
rm -r fwd 
rm -r rev
mkdir temp
mkdir fwd
mkdir rev

make clean
make bpt_dump_numbers
./bpt_dump_numbers fwd/fwd/ rev/rev_ data.txt
cat temp/temp* > ../offset_file
