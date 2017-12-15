
**Doubt cleared**
1. generate data.txt (currently 10 gb file) using data_gen.cc using command
		g++ -std=c++11 data_gen.cc
2. run gets.cpp using
		g++ -std=c++11 data_gen.cc

	and follow the steps accordingly
3. run the ececutable using 
		time "executable name"
		
Doubt- Not a major improvement.
File scan time of 100 GB file
no parallel 18m52s
2 parallel- 16m0s
8 parallel- 15m50s
16 parallel- 14m38s

If parallelizatin provides such minor improvements, there is no point in applying it to BPT code as major time will be taken by sorting in precomputation.