#include <stdio.h>
#include "orcLibBridge.h"

int main(int argc, char* argv[]) {
    initOrcReader("/usr/pgsql-9.4/city.orc", 2, 1000);

    char** nextTuple = getNextOrcTuple();
    while(nextTuple != NULL) {
        //printNextTuple(nextTupe);
        printf("%s, %s\n", nextTuple[0], nextTuple[1]);
        nextTuple = getNextOrcTuple();
    }
    releaseOrcBridgeMem();

    return 0;
}