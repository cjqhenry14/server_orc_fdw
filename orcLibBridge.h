
#ifndef ORCLIBBRIDGE_H
#define ORCLIBBRIDGE_H

#ifdef __cplusplus
extern "C"{
#endif

void initOrcReader(const char* filename);

char* getLine();


#ifdef __cplusplus
};
#endif

#endif
