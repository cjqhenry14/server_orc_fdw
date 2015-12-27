#include <stdio.h>
#include "orcLibBridge.h"
#include <stdlib.h>
#include <string.h>

void printNextTuple(char nextTuple[10][200], unsigned int colNum) {
    unsigned int i; 
    for (i=0; i<colNum; i++)
    {
        if(nextTuple[i]!=NULL) {
            printf("%s,  ", nextTuple[i]);
        }
    }
    printf("\n");
}
char ttt[10][200];

void simIterativeScan(char * filename, unsigned int _colNum) {
    unsigned int i;
    unsigned int colNum = _colNum;
    initOrcReader(filename, colNum, 1000);

    /*char **tmpNextTuple = (char **)malloc(colNum * sizeof(char *));
    for(i = 0; i < colNum; i++) {
        tmpNextTuple[i] = (char *) malloc(200 * sizeof(char));
        memset(tmpNextTuple[i], 0, 200 * sizeof(char));
    }
*/
    for(i = 0; i < colNum; i++) {
        memset(ttt[i], 0, 200 * sizeof(char));
    }
    /*for (i=0; i<colNum; i++)
    {
        tmpNextTuple[i] = NULL;
    }*/

    while(getOrcNextTuple(filename, ttt)) {
        printNextTuple(ttt, colNum);
    }

    /*for(i=0; i<colNum; i++) {
        free(tmpNextTuple[i]);
    }
    free(tmpNextTuple);*/
}


int main(int argc, char* argv[]) {

    //simIterativeScan("/usr/pgsql-9.4/supplier.orc", 7);
    //printf("rows: %lu\n", getOrcTupleCount("/usr/pgsql-9.4/supplier.orc"));

    simIterativeScan("/usr/pgsql-9.4/nation.orc", 4);
    printf("rows: %lu\n", getOrcTupleCount("/usr/pgsql-9.4/nation.orc"));

    //simIterativeScan("/usr/pgsql-9.4/region.orc", 3);
    //printf("rows: %lu\n", getOrcTupleCount("/usr/pgsql-9.4/region.orc"));

    releaseOrcReader("/usr/pgsql-9.4/nation.orc");
    //releaseOrcReader("/usr/pgsql-9.4/region.orc");
    //releaseOrcReader("/usr/pgsql-9.4/supplier.orc");

    return 0;
}
