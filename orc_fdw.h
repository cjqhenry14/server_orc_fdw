//
// Created by cjq on 12/10/15.
//

#ifndef ORC_FDW_H
#define ORC_FDW_H

#include "fmgr.h"

#define MYLOGFILE "/usr/pgsql-9.4/mylog.txt"
#define SIM_PAGES 50000// one page = 4kb, 200mb file = 50000 pages
#define ORC_TUPLE_COST_MULTIPLIER 10
#define MAX_ROW_PER_BATCH 1000

#define SIM_TUPLE_FIELD_LEN 200
#define SIM_TUPLE_NUM 16
/* Defines for valid option names */
#define OPTION_NAME_FILENAME "filename"

extern FILE * logfile;

/*actualTotalRowCount is stored as global var, avoid repeat computing
 * can't be stored in OrcFdwOptions: because only can be used if baserel is provided
 * can't be stored in OrcExeState: because OrcExeState is init in BeginForeignScan
 * So, set as global var*/
//uint64 actualTotalRowCount;

/*
 * OrcValidOption keeps an option name and a context. When an option is passed
 * into orc_fdw objects (server and foreign table), we compare this option's
 * name and context against those of valid options.
 */
typedef struct OrcValidOption
{
    const char *optionName;
    Oid optionContextId;

} OrcValidOption;

/* Array of options that are valid for orc_fdw */
static const uint32 ValidOptionCount = 1;//temporary
static const OrcValidOption ValidOptionArray[] =
        {
                /* foreign table options */
                { OPTION_NAME_FILENAME, ForeignTableRelationId }
                //may add more in the fututre, compressionType etc.
        };

/* initialized in fileGetForeignRelSize, stored as baserel->fdw_private = (void *) OrcFdwOptions;
 * can be used only parameters has RelOptInfo *baserel*/
typedef struct OrcFdwOptions
{
    char *filename;
    //these 3 are defined in cstore
    //CompressionType compressionType;
    //uint64 stripeRowCount;
    //uint32 blockRowCount;
} OrcFdwOptions;

/* initialized in BeginForeignScan, stored as node->fdw_state = (void *) orcState; */
typedef struct OrcExeState
{
    //basic
    // hdfsfile * should be added later
    char       *filename;
    int         colNum;//number of columns

    //other
    FmgrInfo   *in_functions;	/* array of input functions for each attrs */
    Oid		   *typioparams;	/* array of element types for in_functions */

    //char        **nextTuple;
    char nextTuple[10][200];

    List *queryRestrictionList; /* init in BeginForeignScan */
    TupleDesc tupleDescriptor;

    MemoryContext orcContext;

} OrcExeState;


#endif //ORC_FDW_H
