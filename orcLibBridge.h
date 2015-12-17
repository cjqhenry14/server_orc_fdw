
#ifndef ORCLIBBRIDGE_H
#define ORCLIBBRIDGE_H

#ifdef __cplusplus
extern "C"{
#endif

void initOrcReader(const char* filename, int fdwColNum, int fdwMaxRowPerBatch);

char** getNextOrcTuple();

void releaseOrcBridgeMem();

#ifdef __cplusplus
};
#endif

#endif
