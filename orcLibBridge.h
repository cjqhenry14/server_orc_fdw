
#ifndef ORCLIBBRIDGE_H
#define ORCLIBBRIDGE_H

#ifdef __cplusplus
extern "C"{
#endif

void initOrcReader(const char* filename);

char* getLine();

void printContents(const char* filename);

#ifdef __cplusplus
};
#endif

#endif
