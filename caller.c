#include <stdio.h>
#include "orcLibBridge.h"
#include <stdlib.h>

void printNextTuple(char** nextTuple, int colNum) {
    unsigned int i; 
    for (i=0; i<colNum; i++)
    {
        if(nextTuple[i]!=NULL) {
            printf("%s,  ", nextTuple[i]);
        }
    }
    printf("\n");
}

void simIterativeScan() {
    unsigned int i;
    unsigned int colNum = 5;
    initOrcReader("/usr/pgsql-9.4/test_data1.orc", 5, 1000);
    char **nextTuple = (char **)malloc(colNum * sizeof(char *));
    for (i=0; i<colNum; i++)
    {
        nextTuple[i] = NULL;
    }

    while(getNextOrcTuple(nextTuple)) {
        printNextTuple(nextTuple, colNum);
    }
    releaseOrcBridgeMem(nextTuple);
}

void simGetTupleCount() {
    printf("row: %lu,  ", getOrcTupleCount("/usr/pgsql-9.4/test_data1.orc"));
}

int main(int argc, char* argv[]) {

    simGetTupleCount();
    return 0;
}
