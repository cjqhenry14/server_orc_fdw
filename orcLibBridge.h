#ifndef ORCLIBBRIDGE_H
#define ORCLIBBRIDGE_H
#include <stdbool.h>

#ifdef __cplusplus
extern "C"{
#endif

/* wrapper functions for fdw*/

/* init global var, should be used in BeginForeignScan() */
void initOrcReader(const char* filename, unsigned int fdwColNum, unsigned int fdwMaxRowPerBatch);

/**
 * iteratively get one line record.
 * @return: false means no next record.
 */
bool getNextOrcTuple(char ** tuple);

/* release tuple memory, should be used in EndForeignScan() */
void releaseOrcBridgeMem();


/**
 * Get the number of rows in the file.
 * @return the number of rows
 */
unsigned long long getOrcTupleCount(const char* filename);


#ifdef __cplusplus
};
#endif

#endif
