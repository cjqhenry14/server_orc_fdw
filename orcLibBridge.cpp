#include "orcLibBridge.h"
#include "orcInclude/ColumnPrinter.hh"

#include <memory>
#include <string>
#include <iostream>
#include <exception>

/*global variable*/
unsigned int colNum = 0;
unsigned int maxRowPerBatch = 1000;
std::string line;
unsigned long curRow;

char fakefilename[50] = "/usr/pgsql-9.4/city.orc";

orc::ReaderOptions opts;
/*init first, avoid init with abstract obj. Then change in initOrcReader()*/
std::unique_ptr<orc::Reader> reader = orc::createReader(orc::readLocalFile(std::string(fakefilename)), opts);
std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(maxRowPerBatch);
std::unique_ptr<orc::ColumnPrinter> printer = createColumnPrinter(line, reader->getType());

/*allocate memory space, and init with NULL ptr*/
void deleteTuple(char** tuple) {
    for (unsigned int i=0; i<colNum; i++)
    {
        if(tuple[i]!=NULL) {
            delete[] tuple[i];
        }
    }
    delete [] tuple;
}

void clearTuple(char** tuple) {
    for (unsigned int i=0; i<colNum; i++)
    {
        if(tuple[i] != NULL) {
            delete [] tuple[i];
            tuple[i] = NULL;
        }
    }
}

void printNextTuple(char** nextTuple) {
    for (unsigned int i=0; i<colNum; i++)
    {
        if(nextTuple[i]!=NULL) {
            printf("%s,  ", nextTuple[i]);
        }
    }
    printf("\n");
}


/*For fdw: init tuple memory, and other global var, should be used in BeginForeignScan() */
void initOrcReader(const char* filename, unsigned int fdwColNum, unsigned int fdwMaxRowPerBatch) {
    colNum = fdwColNum;
    curRow = 0;//don't forget
    maxRowPerBatch = fdwMaxRowPerBatch;

    reader = orc::createReader(orc::readLocalFile(std::string(filename)), opts);
    batch = reader->createRowBatch(maxRowPerBatch);
    printer = createColumnPrinter(line, reader->getType());

    reader->next(*batch);
    printer->reset(*batch);
}

/*For fdw: iteratively get one line record, for using
 * return: false means no next record.
 * */
bool getNextOrcTuple(char ** tuple) {
    clearTuple(tuple);

    if(batch->numElements == 0)
        return false;

    if(curRow == batch->numElements) {
        if (reader->next(*batch)) {

            printer->reset(*batch);

            if(batch->numElements == 0)
                return false;

            curRow = 0;
            /* my modified printRow(int rowId, char** tuple, int curColId) */
            printer->printRow(curRow, tuple, 0);
            curRow++;

            return true;
        }
        else
            return false;
    }
        /*curRow < batch->numElements*/
    else {
        printer->printRow(curRow, tuple, 0);
        curRow++;
        return true;
    }
}

/*For fdw: release tuple memory, should be used in EndForeignScan() */
void releaseOrcBridgeMem(char **tuple) {
    deleteTuple(tuple);
}

/* end server: for fdw wrapper functions*/
