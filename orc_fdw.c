/*-------------------------------------------------------------------------
 *
 * orc_fdw.c
 *		  foreign-data wrapper for orc files.
 *
 * Copyright (c) 2010-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/orc_fdw/orc_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "storage/fd.h"
#include "orc_fdw.h"
#include "orcLibBridge.h"

PG_MODULE_MAGIC;

//cjq
//FILE * logfile;

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(orc_fdw_handler);
PG_FUNCTION_INFO_V1(orc_fdw_validator);

/*
 * FDW callback routines
 */
static void fileGetForeignRelSize(PlannerInfo *root,
                                  RelOptInfo *baserel,
                                  Oid foreigntableid);
static void fileGetForeignPaths(PlannerInfo *root,
                                RelOptInfo *baserel,
                                Oid foreigntableid);
static ForeignScan *fileGetForeignPlan(PlannerInfo *root,
                                       RelOptInfo *baserel,
                                       Oid foreigntableid,
                                       ForeignPath *best_path,
                                       List *tlist,
                                       List *scan_clauses);
static void fileExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void fileBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *fileIterateForeignScan(ForeignScanState *node);
static void fileReScanForeignScan(ForeignScanState *node);
static void fileEndForeignScan(ForeignScanState *node);
static bool fileAnalyzeForeignTable(Relation relation,
                                    AcquireSampleRowsFunc *func,
                                    BlockNumber *totalpages);

/*
 * Helper functions
 */

static OrcFdwOptions * OrcGetOptions(Oid foreignTableId);

static char * OrcGetOptionValue(Oid foreignTableId, const char *optionName);

static List *ColumnList(RelOptInfo *baserel, Oid foreignTableId);

//static List * ColumnList(RelOptInfo *baserel);

static TupleTableSlot *simIterateForeignScan(ForeignScanState *node);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
orc_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    fdwroutine->GetForeignRelSize = fileGetForeignRelSize;
    fdwroutine->GetForeignPaths = fileGetForeignPaths;
    fdwroutine->GetForeignPlan = fileGetForeignPlan;
    fdwroutine->ExplainForeignScan = fileExplainForeignScan;
    fdwroutine->BeginForeignScan = fileBeginForeignScan;
    //fdwroutine->IterateForeignScan = fileIterateForeignScan;
    fdwroutine->IterateForeignScan = simIterateForeignScan;
    fdwroutine->ReScanForeignScan = fileReScanForeignScan;
    fdwroutine->EndForeignScan = fileEndForeignScan;
    fdwroutine->AnalyzeForeignTable = fileAnalyzeForeignTable;// only for ANALYZE foreign table

    PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
orc_fdw_validator(PG_FUNCTION_ARGS)
{
    Datum optionArray = PG_GETARG_DATUM(0);
    Oid optionContextId = PG_GETARG_OID(1);
    List *optionList = untransformRelOptions(optionArray);
    ListCell *optionCell = NULL;
    bool filenameFound = false;

    foreach(optionCell, optionList)
    {
        DefElem *optionDef = (DefElem *) lfirst(optionCell);
        char *optionName = optionDef->defname;
        bool optionValid = false;

        int32 optionIndex = 0;
        for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
        {
            const OrcValidOption *validOption = &(ValidOptionArray[optionIndex]);

            if ((optionContextId == validOption->optionContextId)
                && (strncmp(optionName, validOption->optionName, NAMEDATALEN) == 0))
            {
                optionValid = true;
                break;
            }
        }

        /* if invalid option, display an informative error message */
        if (!optionValid)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                            errmsg("invalid option")));
        }

        if (strncmp(optionName, OPTION_NAME_FILENAME, NAMEDATALEN) == 0)
        {
            filenameFound = true;
        }
    }

    if (optionContextId == ForeignTableRelationId)
    {
        if (!filenameFound)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                            errmsg("filename is required for orc_fdw foreign tables")));
        }
    }

    PG_RETURN_VOID();
}

/*
 * OrcGetOptionValue walks over foreign table and foreign server options, and
 * looks for the option with the given name. If found, the function returns the
 * option's value. This function is unchanged from mongo_fdw.
 */
static char *
OrcGetOptionValue(Oid foreignTableId, const char *optionName)
{
    ForeignTable *foreignTable = NULL;
    ForeignServer *foreignServer = NULL;
    List *optionList = NIL;
    ListCell *optionCell = NULL;
    char *optionValue = NULL;

    foreignTable = GetForeignTable(foreignTableId);
    foreignServer = GetForeignServer(foreignTable->serverid);

    optionList = list_concat(optionList, foreignTable->options);
    optionList = list_concat(optionList, foreignServer->options);

    foreach(optionCell, optionList)
    {
        DefElem *optionDef = (DefElem *) lfirst(optionCell);
        char *optionDefName = optionDef->defname;

        if (strncmp(optionDefName, optionName, NAMEDATALEN) == 0)
        {
            optionValue = defGetString(optionDef);
            break;
        }
    }

    return optionValue;
}

/*
 * OrcGetOptions returns the option values to be used when reading and parsing
 * the orc file.
 * first and only used in fileGetForeignRelSize, after that, use
 */
static OrcFdwOptions *
OrcGetOptions(Oid foreignTableId)
{
    OrcFdwOptions *orcFdwOptions = NULL;
    char *filename = NULL;

    filename = OrcGetOptionValue(foreignTableId, OPTION_NAME_FILENAME);

    orcFdwOptions = (OrcFdwOptions *) palloc0(sizeof(OrcFdwOptions));
    orcFdwOptions->filename = filename;

    return orcFdwOptions;
}

/*
 * fileGetForeignRelSize
 *		Obtain relation size estimates for a foreign table
 */
static void
fileGetForeignRelSize(PlannerInfo *root,
                      RelOptInfo *baserel,
                      Oid foreigntableid)
{
    OrcFdwOptions *options = OrcGetOptions(foreigntableid);
    /* OrcFdwOptions is stored as baserel->fdw_private for future use
     * OrcFdwOptions *options = (OrcFdwOptions *) node->fdw_private; */
    baserel->fdw_private = (void *) options;

    /* Estimate relation size */
    /*actualTotalRowCount is stored as global var, avoid repeat computing
     * can't be stored in OrcFdwOptions: because only can be used if baserel is provided*/
    actualTotalRowCount = getOrcTupleCount(options->filename);

    double tupleCount = (double) actualTotalRowCount;

    double rowSelectivity = clauselist_selectivity(root, baserel->baserestrictinfo, 0, JOIN_INNER,
                                                   NULL);

    double outputRowCount = clamp_row_est(tupleCount * rowSelectivity);
    baserel->rows = outputRowCount;
}

/*
 * fileGetForeignPaths
 *		Create possible access paths for a scan on the foreign table
 *
 *		Currently we don't support any push-down feature, so there is only one
 *		possible access path, which simply returns all records in the order in
 *		the data file.
 */
static void
fileGetForeignPaths(PlannerInfo *root,
                    RelOptInfo *baserel,
                    Oid foreigntableid)
{
    Path *foreignScanPath = NULL;

    /*
     * We estimate costs almost the same way as cost_seqscan(), thus assuming
     * that I/O costs are equivalent to a regular table file of the same size.
     * However, we take per-tuple CPU costs as 10x of a seqscan to account for
     * the cost of parsing records.
     */
    //List *queryColumnList = ColumnList(baserel, foreigntableid);
    //uint32 queryColumnCount = (uint32)list_length(queryColumnList);
    //TODO: can't get page count!!!
    BlockNumber relationPageCount = SIM_PAGES;
    Relation relation = heap_open(foreigntableid, AccessShareLock);
    uint32 relationColumnCount = (uint32) RelationGetNumberOfAttributes(relation);

    double queryColumnRatio = (double) 1/2;
    double queryPageCount = relationPageCount * queryColumnRatio;
    double totalDiskAccessCost = seq_page_cost * queryPageCount;

    double tupleCountEstimate = (double) actualTotalRowCount;

    /*
     * We estimate costs almost the same way as cost_seqscan(), thus assuming
     * that I/O costs are equivalent to a regular table file of the same size.
     */
    double filterCostPerTuple = baserel->baserestrictcost.per_tuple;
    double cpuCostPerTuple = cpu_tuple_cost + filterCostPerTuple;
    double totalCpuCost = cpuCostPerTuple * tupleCountEstimate;

    double startupCost = baserel->baserestrictcost.startup;
    double totalCost  = startupCost + totalCpuCost + totalDiskAccessCost;

    /* create a foreign path node and add it as the only possible path */
    foreignScanPath = (Path *) create_foreignscan_path(root, baserel, baserel->rows, startupCost,
                                                       totalCost,
                                                       NIL, /* no known ordering */
                                                       NULL, /* not parameterized */
                                                       NIL); /* no fdw_private */

    add_path(baserel, foreignScanPath);
    heap_close(relation, AccessShareLock);
}

/*
 * fileGetForeignPlan
 *		Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
fileGetForeignPlan(PlannerInfo *root,
                   RelOptInfo *baserel,
                   Oid foreigntableid,
                   ForeignPath *best_path,
                   List *tlist,
                   List *scan_clauses)
{
    ForeignScan *foreignScan = NULL;
    List *columnList = NULL;
    //List *opExpressionList = NIL;
    List *foreignPrivateList = NIL;

    /*
     * We have no native ability to evaluate restriction clauses, so we just
     * put all the scanClauses into the plan node's qual list for the executor
     * to check.
     */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /*
     * We construct the query document to have MongoDB filter its rows. We could
     * also construct a column name document here to retrieve only the needed
     * columns. However, we found this optimization to degrade performance on
     * the MongoDB server-side, so we instead filter out columns on our side.
     */
    //opExpressionList = ApplicableOpExpressionList(baserel);

    /*
     * As an optimization, we only add columns that are present in the query to
     * the column mapping hash. To find these columns, we need baserel. We don't
     * have access to baserel in executor's callback functions, so we get the
     * column list here and put it into foreign scan node's private list.
     */
    //columnList = ColumnList(baserel, foreigntableid);

    //foreignPrivateList = list_make1(columnList);

    /* create the foreign scan node */
    foreignScan = make_foreignscan(tlist, scan_clauses, baserel->relid,
                                   NIL, /* no expressions to evaluate */
                                   foreignPrivateList);

    return foreignScan;
}

/*
 * fileExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
fileExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
    /* Fetch options --- we only need filename at this point */
    Oid foreignTableId = RelationGetRelid(node->ss.ss_currentRelation);
    OrcFdwOptions *options = OrcGetOptions(foreignTableId);
    ExplainPropertyText("Orc File", options->filename, es);

    /* Suppress file size if we're not showing cost details */
    /*if (es->costs)
    {
        struct stat stat_buf;

        if (stat(options->filename, &stat_buf) == 0)
            ExplainPropertyLong("Orc File Size", (long) stat_buf.st_size, es);
    }*/
}

/*
 * fileBeginForeignScan
 *		Initiate access to the file by creating CopyState
 */
static void
fileBeginForeignScan(ForeignScanState *node, int eflags)
{
    //cjq
    //logfile = fopen(MYLOGFILE, "w");

    //fprintf(logfile, "%d\n", 12);
    //fflush(logfile);
    //fclose(logfile);

    OrcExeState *orcState;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

    /*
     * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    /* Fetch options of foreign table */
    Oid foreignTableId = RelationGetRelid(node->ss.ss_currentRelation);
    OrcFdwOptions *options = OrcGetOptions(foreignTableId);

    unsigned int i;
    /*
     * Save state in node->fdw_state.  We must save enough information to call
     * BeginCopyFrom() again.
     */
    orcState = (OrcExeState *) palloc(sizeof(OrcExeState));
    orcState->filename = options->filename;
    //orcState->file = AllocateFile(orcState->filename, "r");

    //get colNum
    orcState->colNum = slot->tts_tupleDescriptor->natts;

    /*init orc reader (filename, column number, maxRowPerBatch) */
    initOrcReader(orcState->filename, orcState->colNum, MAX_ROW_PER_BATCH);
    /*
    orcState->nextTuple = (char **)malloc(orcState->colNum * sizeof(char *));

    unsigned int i;
    for (i=0; i<orcState->colNum; i++)
    {
        orcState->nextTuple[i] = NULL;
    }
    */
    //init in_functions, typioparams
    FmgrInfo   *in_functions = (FmgrInfo *) palloc(orcState->colNum * sizeof(FmgrInfo));
    Oid			in_func_oid;
    Oid		   *typioparams = (Oid *) palloc(orcState->colNum * sizeof(Oid));
    Oid typeid;
    for(i = 0; i < orcState->colNum; i++) {
        typeid = slot->tts_tupleDescriptor->attrs[i]->atttypid;
        getTypeInputInfo(typeid, &in_func_oid, &typioparams[i]);
        fmgr_info(in_func_oid, &in_functions[i]);
    }

    orcState->in_functions = in_functions;
    orcState->typioparams = typioparams;

    /* store query restriction list */
    ForeignScan *foreignScan = NULL;
    foreignScan = (ForeignScan *) node->ss.ps.plan;

    List *foreignPrivateList = NIL;
    foreignPrivateList = (List *) foreignScan->fdw_private;

    /* real selected column list, which will be passed to orc lib */
    /* TODO: the difference between columnList and foreignPrivateList, orcState->queryRestrictionList
     * TODO: print 3 them out, why orcState->queryRestrictionList = (List *) lsecond(foreignPrivateList)*/
    //List *columnList = NIL;
    //linitial: get the list's head's data
    //columnList = (List *) linitial(foreignPrivateList);

    //TODO: why add this line, fdw doesn't work.
    //orcState->queryRestrictionList = (List *) lsecond(foreignPrivateList);

    node->fdw_state = (void *) orcState;
}

void itoa(int i, char * s)
{
    int len=0, sign=1, k;
    if(i<0)
    {
        i*=-1;
        sign=-1;
    }
    int j=i;
    while(j>0)
    {
        len++;
        j/=10;
    }

    for(k=len; k>=1; k--)
    {
        s[k-1]=i%10+'0';
        i/=10;
    }
    s[len]='\0';
}

int count = 0;
char ss[5][55] = {"1", "mike", "23", "hehe", "2013-01-01"};

static TupleTableSlot *
simIterateForeignScan(ForeignScanState *node)
{
    OrcExeState *orcState = (OrcExeState *) node->fdw_state;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    bool		found;

    ExecClearTuple(slot);


    //nation: int, string, int, string
    //region: int, string, string
    TupleDesc tupledes = slot->tts_tupleDescriptor;
    int colNum = tupledes->natts;

    char** tmpNextTuple = (char **)malloc(orcState->colNum * sizeof(char *));

    unsigned int i;
    for (i=0; i<orcState->colNum; i++)
    {
        tmpNextTuple[i] = NULL;
    }
    getNextOrcTuple(tmpNextTuple);

    if(colNum == 4) {//nation
        ss[1][0] = '1' + count;
    }
    else {//region
        ss[1][0] = '1' + count;
    }
    //orcState->typioparams[i] 是正确的
    //tupledes->attrs[i]->atttypmod 都是156....第3列

    //itoa(orcState->typioparams[2], ss[1]);

    Datum *columnValues = slot->tts_values;
    bool *columnNulls = slot->tts_isnull;
    /* initialize all values for this row to null */
    memset(columnValues, 0, colNum * sizeof(Datum));

    count++;
    if(count < 6) {
        memset(columnNulls, false, colNum * sizeof(bool));
        found = true;
    }
    else {
        found = false;
        memset(columnNulls, true, colNum * sizeof(bool));
    }


    for(i = 0; i < colNum; i++) {
        Datum columnValue = 0;

        if(i==2 && colNum == 3) {
            columnValue = InputFunctionCall(&orcState->in_functions[i],
                                            "33", orcState->typioparams[i],
                                            tupledes->attrs[i]->atttypmod);
        }
        else {
            columnValue = InputFunctionCall(&orcState->in_functions[i],
                                            tmpNextTuple[i], orcState->typioparams[i],
                                            tupledes->attrs[i]->atttypmod);
        }


        slot->tts_values[i] = columnValue;
    }

    if (found)
        ExecStoreVirtualTuple(slot);

    for(i=0; i<orcState->colNum; i++) {
        free(tmpNextTuple[i]);
    }
    free(tmpNextTuple);

    return slot;
}


/*
 * fileIterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
fileIterateForeignScan(ForeignScanState *node)
{
    OrcExeState *orcState = (OrcExeState *) node->fdw_state;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    bool		found;
    /*
     * The protocol for loading a virtual tuple into a slot is first
     * ExecClearTuple, then fill the values/isnull arrays, then
     * ExecStoreVirtualTuple.  If we don't find another row in the file, we
     * just skip the last step, leaving the slot empty as required.
     *
     * We can pass ExprContext = NULL because we read all columns from the
     * file, so no need to evaluate default expressions.
     *
     * We can also pass tupleOid = NULL because we don't allow oids for
     * foreign tables.
     */
    ExecClearTuple(slot);

    TupleDesc tupledes = slot->tts_tupleDescriptor;
    int colNum = tupledes->natts;

    Datum *columnValues = slot->tts_values;
    bool *columnNulls = slot->tts_isnull;
    /* initialize all values for this row to null */
    memset(columnValues, 0, colNum * sizeof(Datum));

    /*has next tuple*/
    char** tmpNextTuple = (char **)malloc(orcState->colNum * sizeof(char *));

    unsigned int i;
    for (i=0; i<orcState->colNum; i++)
    {
        tmpNextTuple[i] = NULL;
    }

    if(getNextOrcTuple(tmpNextTuple)) {
        memset(columnNulls, false, colNum * sizeof(bool));
        found = true;
    }
    else {
        found = false;
        memset(columnNulls, true, colNum * sizeof(bool));
    }

    //read and fill next line's record
    for(i = 0; i < colNum; i++) {
        Datum columnValue = 0;
        if(tmpNextTuple[i] != NULL) {
            columnValue = InputFunctionCall(&orcState->in_functions[i],
                                            tmpNextTuple[i], orcState->typioparams[i],
                                            tupledes->attrs[i]->atttypmod);
        }
        else {
            slot->tts_isnull[i] = true;
        }

        slot->tts_values[i] = columnValue;
    }

    for(i=0; i<orcState->colNum; i++) {
        free(tmpNextTuple[i]);
    }
    free(tmpNextTuple);

    if (found)
        ExecStoreVirtualTuple(slot);

    return slot;
}

/*
 * fileReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
fileReScanForeignScan(ForeignScanState *node)
{
    fileEndForeignScan(node);
    fileBeginForeignScan(node, 0);
}

/*
 * fileEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
fileEndForeignScan(ForeignScanState *node)
{
    count = 0;
    OrcExeState *orcState = (OrcExeState *) node->fdw_state;

    /* if festate is NULL, we are in EXPLAIN; nothing to do */
    if (orcState == NULL) {
        return;
    }

    /*TODO: clear all file related memory */
    releaseOrcBridgeMem();

    /*int i;
    for(i=0; i<orcState->colNum; i++) {
        pfree(orcState->nextTuple[i]);
    }
    pfree(orcState->nextTuple);*/

    /*if (orcState->file)
    {
        FreeFile(orcState->file);
    }*/
    pfree(orcState->typioparams);

    pfree(orcState);
}

/*
 * fileAnalyzeForeignTable
 *		Test whether analyzing this foreign table is supported
 */
static bool
fileAnalyzeForeignTable(Relation relation,
                        AcquireSampleRowsFunc *func,
                        BlockNumber *totalpages)
{
    return false;
}



/*
 * ColumnList takes in the planner's information about this foreign table. The
 * function then finds all columns needed for query execution, including those
 * used in projections, joins, and filter clauses, de-duplicates these columns,
 * and returns them in a new list. This function is unchanged from mongo_fdw.
 */
static List *
ColumnList(RelOptInfo *baserel, Oid foreignTableId)
{
    List *columnList = NIL;
    List *neededColumnList = NIL;
    AttrNumber columnIndex = 1;
    AttrNumber columnCount = baserel->max_attr;
    List *targetColumnList = baserel->reltargetlist;
    List *restrictInfoList = baserel->baserestrictinfo;
    ListCell *restrictInfoCell = NULL;
    const AttrNumber wholeRow = 0;
    Relation relation = heap_open(foreignTableId, AccessShareLock);
    TupleDesc tupleDescriptor = RelationGetDescr(relation);
    Form_pg_attribute *attributeFormArray = tupleDescriptor->attrs;

    /* first add the columns used in joins and projections */
    neededColumnList = list_copy(targetColumnList);

    /* then walk over all restriction clauses, and pull up any used columns */
    foreach(restrictInfoCell, restrictInfoList)
    {
        RestrictInfo *restrictInfo = (RestrictInfo *) lfirst(restrictInfoCell);
        Node *restrictClause = (Node *) restrictInfo->clause;
        List *clauseColumnList = NIL;

        /* recursively pull up any columns used in the restriction clause */
        clauseColumnList = pull_var_clause(restrictClause,
                                           PVC_RECURSE_AGGREGATES,
                                           PVC_RECURSE_PLACEHOLDERS);

        neededColumnList = list_union(neededColumnList, clauseColumnList);
    }

    /* walk over all column definitions, and de-duplicate column list */
    for (columnIndex = 1; columnIndex <= columnCount; columnIndex++)
    {
        ListCell *neededColumnCell = NULL;
        Var *column = NULL;

        /* look for this column in the needed column list */
        foreach(neededColumnCell, neededColumnList)
        {
            Var *neededColumn = (Var *) lfirst(neededColumnCell);
            if (neededColumn->varattno == columnIndex)
            {
                column = neededColumn;
                break;
            }
            else if (neededColumn->varattno == wholeRow)
            {
                Form_pg_attribute attributeForm = attributeFormArray[columnIndex - 1];
                Index tableId = neededColumn->varno;

                column = makeVar(tableId, columnIndex, attributeForm->atttypid,
                                 attributeForm->atttypmod, attributeForm->attcollation,
                                 0);
                break;
            }
        }

        if (column != NULL)
        {
            columnList = lappend(columnList, column);
        }
    }

    heap_close(relation, AccessShareLock);

    return columnList;
}
