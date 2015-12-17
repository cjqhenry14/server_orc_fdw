
#ifndef ORCLIBBRIDGE_H
#define ORCLIBBRIDGE_H

#ifdef __cplusplus
extern "C"{
#endif

void initOrcReader(const char* filename, int fdwColNum, int fdwMaxRowPerBatch);

bool getNextOrcTuple(char ** tuple);

void releaseOrcBridgeMem(char ** tuple);

#ifdef __cplusplus
};
#endif

#endif
