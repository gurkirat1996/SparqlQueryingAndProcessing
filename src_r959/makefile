#CFLAGS= -O3 -std=gnu++0x -funroll-loops -m64 -lm -I.
CFLAGS= -O3 -std=c++0x -funroll-loops -m64 -lm -I. -pthread
CFLAGS32 = -O3 -std=gnu++0x -funroll-loops -lm -I.
#CFLAGSDBG=  -g3 -std=gnu++0x -m64 -lm -I.
CFLAGSDBG=  -g3 -std=c++0x -m64 -lm -I.

all: bitmat

bitmat: main.o bitmatops.o graphops.o miscops.o SPARQLParser.o SPARQLLexer.o bpt.o cli.o
	g++ $(CFLAGS) -o bitmat bitmatops.o graphops.o miscops.o  main.o SPARQLParser.o SPARQLLexer.o bpt.o cli.o

bitmatdbg: main.cpp bitmatops.cpp graphops.cpp miscops.cpp bitmat.hpp
	g++ $(CFLAGSDBG) -o bitmatdbg bitmatops.cpp graphops.cpp miscops.cpp main.cpp

main.o: main.cpp bitmat.hpp
	g++ $(CFLAGS) -c main.cpp

graphops.o: graphops.cpp bitmat.hpp SPARQLParser.o SPARQLLexer.o bpt.o cli.o
	g++ $(CFLAGS) -c graphops.cpp SPARQLParser.o SPARQLLexer.o bpt.o cli.o

bitmatops.o: bitmatops.cpp bitmat.hpp
	g++ $(CFLAGS) -c bitmatops.cpp

miscops.o: miscops.cpp bitmat.hpp
	g++ $(CFLAGS) -c miscops.cpp

SPARQLParser.o: ../TripleBit/TripleBit/SPARQLParser.cpp
	g++ $(CFLAGS) -c ../TripleBit/TripleBit/SPARQLParser.cpp

SPARQLLexer.o: ../TripleBit/TripleBit/SPARQLLexer.cpp
	g++ $(CFLAGS) -c -o SPARQLLexer.o ../TripleBit/TripleBit/SPARQLLexer.cpp

bpt.o: ../B+\ tree/bpt.cc
	g++ $(CFLAGS) -c ../B+\ tree/bpt.cc

cli.o: ../B+\ tree/util/cli.cc
	g++ $(CFLAGS) -c ../B+\ tree/util/cli.cc	

clean:
	rm *.o core
