#include <stdio.h>
#include "orcLibBridge.h"

int main(int argc, char* argv[]) {
    initOrcReader("/usr/pgsql-9.4/city.orc", 2, 1000);

    char** nextTupe = getNextOrcTuple();
    while(nextTupe != NULL) {
        //printNextTuple(nextTupe);
        printf("%s, %s\n", nextTupe[0], nextTupe[1]);
        nextTupe = getNextOrcTuple();
    }
    releaseOrcBridgeMem();

    return 0;
}