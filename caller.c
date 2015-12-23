#include <stdio.h>
#include "orcLibBridge.h"
#include <stdlib.h>

void printNextTuple(char** nextTuple, unsigned int colNum) {
    unsigned int i; 
    for (i=0; i<colNum; i++)
    {
        if(nextTuple[i]!=NULL) {
            printf("%s,  ", nextTuple[i]);
        }
    }
    printf("\n");
}

void simIterativeScan(char * filename, unsigned int _colNum) {
    unsigned int i;
    unsigned int colNum = _colNum;
    initOrcReader(filename, colNum, 1000);
    char **tmpNextTuple = (char **)malloc(colNum * sizeof(char *));
    for (i=0; i<colNum; i++)
    {
        tmpNextTuple[i] = NULL;
    }

    while(getNextOrcTuple(tmpNextTuple)) {
        printNextTuple(tmpNextTuple, colNum);
    }

    for(i=0; i<orcState->colNum; i++) {
        free(tmpNextTuple[i]);
    }
    free(tmpNextTuple);
}


int main(int argc, char* argv[]) {

    simGetTupleCount("/usr/pgsql-9.4/nation.orc", 4);
    simGetTupleCount("/usr/pgsql-9.4/region.orc", 5);

    return 0;
}
