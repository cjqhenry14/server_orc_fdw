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

/*only use for init, the real filename will be passed from orc_fdw*/
char fakefilename[50] = "/usr/pgsql-9.4/city.orc";

orc::ReaderOptions opts;
/*init first, avoid init with abstract obj. Then change in initOrcReader()*/
std::unique_ptr<orc::Reader> reader = orc::createReader(orc::readLocalFile(std::string(fakefilename)), opts);
std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(maxRowPerBatch);
std::unique_ptr<orc::ColumnPrinter> printer = createColumnPrinter(line, reader->getType());

/* Helper functions */
void deleteTuple(char** tuple);
void clearTuple(char** tuple);
void printNextTuple(char** nextTuple);


/* wrapper functions for fdw*/

/* init global var, should be used in BeginForeignScan() */
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

/* iteratively get one line record.
 * return: false means no next record.
 * */
bool getNextOrcTuple(char ** tuple) {
    if(colNum == 4) {//nation

    }
    else {//region: int, string, string
        tuple[2] = new char[2];
        tuple[2][0] = '9';
        tuple[2][1] = '\0';
    }
    //clearTuple(tuple);

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

/* release tuple memory, should be used in EndForeignScan() */
void releaseOrcBridgeMem() {
    //deleteTuple(tuple);
}

/**
 * Get the number of rows in the file.
 * @return the number of rows
 */
unsigned long long getOrcTupleCount(const char* filename) {
    orc::ReaderOptions gtc_opts;
    std::unique_ptr<orc::Reader> gtc_reader;
    gtc_reader = orc::createReader(orc::readLocalFile(std::string(filename)), gtc_opts);

    return gtc_reader->getNumberOfRows();
}


/* Helper functions */

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