#ifndef ORCLIBBRIDGE_H
#define ORCLIBBRIDGE_H
#include <stdbool.h>

#ifndef _UINT64_T
#define _UINT64_T
typedef unsigned long long uint64_t;
#endif /* _UINT64_T */

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
void releaseOrcBridgeMem(char ** tuple);


/**
 * Get the number of rows in the file.
 * @return the number of rows
 */
uint64_t getOrcTupleCount(const char* filename);


#ifdef __cplusplus
};
#endif

#endif
