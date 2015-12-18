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
//#include "commands/copy.h"
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
 * FDW-specific information for RelOptInfo.fdw_private.
 */
typedef struct FileFdwPlanState
{
    char	   *filename;		/* file to read */
    List	   *options;		/* merged COPY options, excluding filename */
    BlockNumber pages;			/* estimate of file's physical size */
    double		ntuples;		/* estimate of number of rows in file */
} FileFdwPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */

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

static int file_acquire_sample_rows(Relation onerel, int elevel,
                                    HeapTuple *rows, int targrows,
                                    double *totalrows, double *totaldeadrows);

static OrcFdwOptions * OrcGetOptions(Oid foreignTableId);

static char * OrcGetOptionValue(Oid foreignTableId, const char *optionName);


static List * ColumnList(RelOptInfo *baserel);



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
    fdwroutine->IterateForeignScan = fileIterateForeignScan;
    fdwroutine->ReScanForeignScan = fileReScanForeignScan;
    fdwroutine->EndForeignScan = fileEndForeignScan;
    fdwroutine->AnalyzeForeignTable = fileAnalyzeForeignTable;

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
    FileFdwPlanState *fdw_private;
    fdw_private = (FileFdwPlanState *) palloc(sizeof(FileFdwPlanState));

    OrcFdwOptions *options = OrcGetOptions(foreigntableid);
    fdw_private->filename = options->filename;

    /*
     * Fetch options.  We only need filename at this point, but we might as
     * well get everything and not need to re-fetch it later in planning.
     */
    baserel->fdw_private = (void *) fdw_private;

    /* Estimate relation size */
    baserel->rows = SIM_ROWS;
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
    //OrcFdwOptions *options = OrcGetOptions(foreigntableid);

    BlockNumber pageCount = SIM_PAGES;
    double tupleCount = SIM_ROWS;

    /*
     * We estimate costs almost the same way as cost_seqscan(), thus assuming
     * that I/O costs are equivalent to a regular table file of the same size.
     * However, we take per-tuple CPU costs as 10x of a seqscan to account for
     * the cost of parsing records.
     */
    double tupleParseCost = cpu_tuple_cost * ORC_TUPLE_COST_MULTIPLIER;
    double tupleFilterCost = baserel->baserestrictcost.per_tuple;
    double cpuCostPerTuple = tupleParseCost + tupleFilterCost;
    double executionCost = (seq_page_cost * pageCount) + (cpuCostPerTuple * tupleCount);

    double startupCost = baserel->baserestrictcost.startup;
    double totalCost = startupCost + executionCost;

    /* create a foreign path node and add it as the only possible path */
    foreignScanPath = (Path *) create_foreignscan_path(root, baserel, baserel->rows, startupCost,
                                                       totalCost,
                                                       NIL, /* no known ordering */
                                                       NULL, /* not parameterized */
                                                       NIL); /* no fdw_private */

    add_path(baserel, foreignScanPath);
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
    columnList = ColumnList(baserel);

    foreignPrivateList = list_make1(columnList);

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
    if (es->costs)
    {
        struct stat stat_buf;

        if (stat(options->filename, &stat_buf) == 0)
            ExplainPropertyLong("Orc File Size", (long) stat_buf.st_size, es);
    }
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

    //ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
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
    orcState->nextTuple = (char **)malloc(2 * sizeof(char *));

    unsigned int i;
    for (i=0; i<orcState->colNum; i++)
    {
        orcState->nextTuple[i] = NULL;
    }
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

    node->fdw_state = (void *) orcState;
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

    //char *ss[2]={"1","abcdef"};//simulate orc block data, 2d array

    TupleDesc tupledes = slot->tts_tupleDescriptor;
    int colNum = tupledes->natts;

    Datum *columnValues = slot->tts_values;
    bool *columnNulls = slot->tts_isnull;
    /* initialize all values for this row to null */
    memset(columnValues, 0, colNum * sizeof(Datum));

    /*has next tuple*/
    if(getNextOrcTuple(orcState->nextTuple)) {
        memset(columnNulls, false, colNum * sizeof(bool));
        found = true;
    }
    else {
        found = false;
        memset(columnNulls, true, colNum * sizeof(bool));
    }

    //read and fill next line's record
    int i;
    for(i = 0; i < colNum; i++) {
        Datum columnValue = 0;
        if(orcState->nextTuple[i] != NULL) {
            columnValue = InputFunctionCall(&orcState->in_functions[i],
                                            orcState->nextTuple[i], orcState->typioparams[i],
                                            tupledes->attrs[i]->atttypmod);
        }
        else {
            slot->tts_isnull[i] = true;
        }

        slot->tts_values[i] = columnValue;
        /*test null*/
        /*if(strcmp(orcState->nextTuple[i], "bb") == 0) {
            slot->tts_isnull[i] = true;
            //slot->tts_values[i] = 0;
        }*/
    }

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
    //FileFdwExecutionState *festate = (FileFdwExecutionState *) node->fdw_state;
    OrcExeState *orcState = (OrcExeState *) node->fdw_state;

    /* if festate is NULL, we are in EXPLAIN; nothing to do */
    if (orcState == NULL) {
        return;
    }

    releaseOrcBridgeMem(orcState->nextTuple);
    /*TODO: clears all file related memory */

    //if (orcState->file)
    //{
       // FreeFile(orcState->file);
    //}

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
    struct stat stat_buf;
    Oid foreignTableId = RelationGetRelid(relation);
    OrcFdwOptions *options = OrcGetOptions(foreignTableId);

    /*
     * Get size of the file.  (XXX if we fail here, would it be better to just
     * return false to skip analyzing the table?)
     */
    int statResult = stat(options->filename, &stat_buf);
    if (statResult < 0)
    {
        ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", options->filename)));
    }

    /*
     * Convert size to pages.  Must return at least 1 so that we can tell
     * later on that pg_class.relpages is not default.
     */
    *totalpages = (stat_buf.st_size + (BLCKSZ - 1)) / BLCKSZ;
    if (*totalpages < 1)
        *totalpages = 1;

    *func = file_acquire_sample_rows;

    return true;
}

/*
 * OrcAcquireSampleRows acquires a random sample of rows from the foreign
 * table. Selected rows are returned in the caller allocated sampleRows array,
 * which must have at least target row count entries. The actual number of rows
 * selected is returned as the function result. We also count the number of rows
 * in the collection and return it in total row count. We also always set dead
 * row count to zero.
 *
 * Note that the returned list of rows does not always follow their actual order
 * in the Orc file. Therefore, correlation estimates derived later could be
 * inaccurate, but that's OK. We currently don't use correlation estimates (the
 * planner only pays attention to correlation for index scans).
 */
static int
file_acquire_sample_rows(Relation relation, int logLevel, HeapTuple *sampleRows,
                     int targetRowCount, double *totalRowCount, double *totalDeadRowCount)
{
    int sampleRowCount = 0;
    double rowCount = 0.0;
    double rowCountToSkip = -1; /* -1 means not set yet */
    double selectionState = 0;
    MemoryContext oldContext = CurrentMemoryContext;
    MemoryContext tupleContext = NULL;
    Datum *columnValues = NULL;
    bool *columnNulls = NULL;
    TupleTableSlot *scanTupleSlot = NULL;
    List *columnList = NIL;
    List *opExpressionList = NIL;
    List *foreignPrivateList = NULL;
    ForeignScanState *scanState = NULL;
    ForeignScan *foreignScan = NULL;
    char *relationName = NULL;
    int executorFlags = 0;

    TupleDesc tupleDescriptor = RelationGetDescr(relation);
    int columnCount = tupleDescriptor->natts;
    Form_pg_attribute *attributes = tupleDescriptor->attrs;

    /* create list of columns of the relation */
    int columnIndex = 0;
    for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
    {
        Var *column = (Var *) palloc0(sizeof(Var));

        /* only assign required fields for column mapping hash */
        column->varattno = columnIndex + 1;
        column->vartype = attributes[columnIndex]->atttypid;
        column->vartypmod = attributes[columnIndex]->atttypmod;

        columnList = lappend(columnList, column);
    }

    /* setup foreign scan plan node */
    // TODO is giving an empty expression list ok?
    foreignPrivateList = list_make2(columnList,opExpressionList);
    foreignScan = makeNode(ForeignScan);
    foreignScan->fdw_private = foreignPrivateList;

    /* set up tuple slot */
    columnValues = (Datum *) palloc0(columnCount * sizeof(Datum));
    columnNulls = (bool *) palloc0(columnCount * sizeof(bool));
    scanTupleSlot = MakeTupleTableSlot();
    scanTupleSlot->tts_tupleDescriptor = tupleDescriptor;
    scanTupleSlot->tts_values = columnValues;
    scanTupleSlot->tts_isnull = columnNulls;

    /* setup scan state */
    scanState = makeNode(ForeignScanState);
    scanState->ss.ss_currentRelation = relation;
    scanState->ss.ps.plan = (Plan *) foreignScan;
    scanState->ss.ss_ScanTupleSlot = scanTupleSlot;

    fileBeginForeignScan(scanState, executorFlags);

    /*
     * Use per-tuple memory context to prevent leak of memory used to read and
     * parse rows from the file using ReadLineFromFile and FillTupleSlot.
     */
    tupleContext = AllocSetContextCreate(CurrentMemoryContext, "orc_fdw temporary context",
                                         ALLOCSET_DEFAULT_MINSIZE,
                                         ALLOCSET_DEFAULT_INITSIZE,
                                         ALLOCSET_DEFAULT_MAXSIZE);

    /* prepare for sampling rows */
    selectionState = anl_init_selection_state(targetRowCount);

    for (;;)
    {
        /* check for user-requested abort or sleep */
        vacuum_delay_point();

        memset(columnValues, 0, columnCount * sizeof(Datum));
        memset(columnNulls, true, columnCount * sizeof(bool));

        MemoryContextReset(tupleContext);
        MemoryContextSwitchTo(tupleContext);

        /* read the next record */
        fileIterateForeignScan(scanState);

        MemoryContextSwitchTo(oldContext);

        /* if there are no more records to read, break */
        if (scanTupleSlot->tts_isempty)
        {
            break;
        }

        /*
         * The first targetRowCount sample rows are simply copied into the
         * reservoir. Then we start replacing tuples in the sample until we
         * reach the end of the relation. This algorithm is from Jeff Vitter's
         * paper (see more info in commands/analyze.c).
         */
        if (sampleRowCount < targetRowCount)
        {
            sampleRows[sampleRowCount++] = heap_form_tuple(tupleDescriptor, columnValues,
                                                           columnNulls);
        }
        else
        {
            /*
             * t in Vitter's paper is the number of records already processed.
             * If we need to compute a new S value, we must use the "not yet
             * incremented" value of rowCount as t.
             */
            if (rowCountToSkip < 0)
            {
                rowCountToSkip = anl_get_next_S(rowCount, targetRowCount, &selectionState);
            }

            if (rowCountToSkip <= 0)
            {
                /*
                 * Found a suitable tuple, so save it, replacing one old tuple
                 * at random.
                 */
                int rowIndex = (int) (targetRowCount * anl_random_fract());
                Assert(rowIndex >= 0);
                Assert(rowIndex < targetRowCount);

                heap_freetuple(sampleRows[rowIndex]);
                sampleRows[rowIndex] = heap_form_tuple(tupleDescriptor, columnValues, columnNulls);
            }

            rowCountToSkip -= 1;
        }

        rowCount += 1;
    }

    /* clean up */
    MemoryContextDelete(tupleContext);
    pfree(columnValues);
    pfree(columnNulls);

    fileEndForeignScan(scanState);

    /* emit some interesting relation info */
    relationName = RelationGetRelationName(relation);
    ereport(logLevel,
            (errmsg("\"%s\": file contains %.0f rows; %d rows in sample", relationName, rowCount, sampleRowCount)));

    (*totalRowCount) = rowCount;
    (*totalDeadRowCount) = 0;

    return sampleRowCount;
}

/*
 * ColumnList takes in the planner's information about this foreign table. The
 * function then finds all columns needed for query execution, including those
 * used in projections, joins, and filter clauses, de-duplicates these columns,
 * and returns them in a new list. This function is unchanged from mongo_fdw.
 */
static List *
ColumnList(RelOptInfo *baserel)
{
    List *columnList = NIL;
    List *neededColumnList = NIL;
    AttrNumber columnIndex = 1;
    AttrNumber columnCount = baserel->max_attr;
    List *targetColumnList = baserel->reltargetlist;
    List *restrictInfoList = baserel->baserestrictinfo;
    ListCell *restrictInfoCell = NULL;

    /* first add the columns used in joins and projections */
    neededColumnList = list_copy(targetColumnList);

    /* then walk over all restriction clauses, and pull up any used columns */
    foreach(restrictInfoCell, restrictInfoList)
    {
        RestrictInfo *restrictInfo = (RestrictInfo *) lfirst(restrictInfoCell);
        Node *restrictClause = (Node *) restrictInfo->clause;
        List *clauseColumnList = NIL;

        /* recursively pull up any columns used in the restriction clause */
        clauseColumnList = pull_var_clause(restrictClause, PVC_RECURSE_AGGREGATES,
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
        }

        if (column != NULL)
        {
            columnList = lappend(columnList, column);
        }
    }

    return columnList;
}
