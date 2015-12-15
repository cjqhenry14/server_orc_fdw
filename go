#!/bin/sh

rm -f *.o
rm -f *.a

gcc  -std=c++11  -c orcLibBridge.cpp  -o orcLibBridge.o  -I orcInclude -L orcLib -lz -lsnappy -lorc -lgmock -lprotobuf -lstdc++

ar rsc liborcLibBridge.a orcLibBridge.o

gcc  -c caller.c  -o caller.o

gcc -std=c++11  -o caller caller.o  -I orcInclude -L. -lorcLibBridge -L  orcLib   -lorc -lgmock  -lsnappy -lz -lprotobuf -lm -lstdc++
