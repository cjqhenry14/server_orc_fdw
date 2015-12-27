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
 * iteratively get one line record, , should be used in IterativeForeignScan()
 * @return: false means no next record.
 */
bool getOrcNextTuple(const char* filename, char tuple[10][200]);

/* release tuple memory, should be used in EndForeignScan() */
void releaseOrcReader(const char* filename);

/**
 * Get the number of rows in the file.
 * @return the number of rows
 */
unsigned long long getOrcTupleCount(const char* filename);


#ifdef __cplusplus
};
#endif

#endif
