//
// Created by cjq on 12/10/15.
//

#ifndef ORC_FDW_H
#define ORC_FDW_H

#include "fmgr.h"

#define MYLOGFILE "/usr/pgsql-9.4/mylog.txt"
#define SIM_ROWS 7
#define SIM_PAGES 1

/* Defines for valid option names and default values */
#define OPTION_NAME_FILENAME "filename"

#define ORC_TUPLE_COST_MULTIPLIER 10
#define MAX_ROW_PER_BATCH 1000

extern FILE * logfile;
/*actualTotalRowCount is stored as global var, avoid repeat computing
 * can't be stored in OrcFdwOptions: because only can be used if baserel is provided
 * can't be stored in OrcExeState: because OrcExeState is init in BeginForeignScan
 * So, set as global var*/
uint64 actualTotalRowCount;

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
    //these 3 are defined in cstorw
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

    char** nextTuple;

    //other
    FmgrInfo   *in_functions;	/* array of input functions for each attrs */
    Oid		   *typioparams;	/* array of element types for in_functions */
    atttypmod;

    List *queryRestrictionList; /* init in BeginForeignScan */


} OrcExeState;


#endif //ORC_FDW_H
