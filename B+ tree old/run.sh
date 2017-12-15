rm -r fwd 
rm -r rev
mkdir fwd
mkdir rev

make clean
make
mkfifo data.fifo
echo "Tell path of database\n" 
read var
echo "calculating size of file..."
num=4991235878 #$(gzip -dc $var | wc -c)
zcat $var> data.fifo &
echo "dump_numbers code starting"
./bpt_dump_numbers fwd/fwd_ rev/rev_ data.fifo $num  
