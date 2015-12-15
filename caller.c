#include <stdio.h>
#include "orcLibBridge.h"

int main(int argc, char* argv[]) {
    initOrcReader(argv[1]);

      while(1) {
        char* cur = getLine();
        if(cur == NULL)
            break;
        printf("%s\n", cur);
     }

    return 0;
}

