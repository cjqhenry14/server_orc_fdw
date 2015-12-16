#!/bin/sh

rm -f *.o
rm -f *.a

gcc  -std=c++11  -c orcLibBridge.cpp  -o orcLibBridge.o  -I orcInclude -L orcLib -lz -lsnappy -lorc -lgmock -lprotobuf -lstdc++

ar rsc liborcLibBridge.a orcLibBridge.o