#include <stdio.h>
#include "orcLibBridge.h"
#include <stdlib.h>

int main(int argc, char* argv[]) {
    initOrcReader("/usr/pgsql-9.4/city.orc", 2, 1000);

    unsigned int i;
    char **nextTuple = (char **)malloc(2 * sizeof(char *));
    for (i=0; i<2; i++)
    {
        nextTuple[i] = NULL;
    }

    while(getNextOrcTuple(nextTuple)) {

        printf("%s\n", nextTuple[1]);
    }
    releaseOrcBridgeMem(nextTuple);

    return 0;
}