
#ifndef ORCLIBBRIDGE_H
#define ORCLIBBRIDGE_H
#include <stdbool.h>

#ifdef __cplusplus
extern "C"{
#endif

void initOrcReader(const char* filename, unsigned int fdwColNum, unsigned int fdwMaxRowPerBatch);

bool getNextOrcTuple(char ** tuple);

void releaseOrcBridgeMem(char ** tuple);

#ifdef __cplusplus
};
#endif

#endif
