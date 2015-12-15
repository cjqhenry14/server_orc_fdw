//
// Created by cjq on 12/11/15.
//

#include "orc_reader.h"



char buf[101];

char * orcReadNextRow(FILE* file) {
    //std::string str = "abcd";
    if(fgets(buf, sizeof(buf), file)) {
        return buf;
    }
    else
        return NULL;
}



