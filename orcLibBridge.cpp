#include "orcLibBridge.h"
#include "orcInclude/ColumnPrinter.hh"

#include <memory>
#include <string>
#include <iostream>
#include <exception>
#include <unordered_map>

/*only use for init, the real filename will be passed from orc_fdw*/
char fakefilename[50] = "/usr/pgsql-9.4/city.orc";

class OrcReader {
public:
/*global variable*/
    unsigned int colNum;
    unsigned int maxRowPerBatch;
    std::string line;
    unsigned long curRow;

    orc::ReaderOptions opts;
/*init first, avoid init with abstract obj. Then change in initOrcReader()*/
    std::unique_ptr<orc::Reader> reader = orc::createReader(orc::readLocalFile(std::string(fakefilename)), opts);
    std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(1000);
    std::unique_ptr<orc::ColumnPrinter> printer = createColumnPrinter(line, reader->getType());

/* init global var, should be used in BeginForeignScan() */
    OrcReader(const char* filename, unsigned int fdwColNum, unsigned int fdwMaxRowPerBatch) {
        colNum = fdwColNum;
        curRow = 0;//don't forget
        maxRowPerBatch = fdwMaxRowPerBatch;

        reader = orc::createReader(orc::readLocalFile(std::string(filename)), opts);
        batch = reader->createRowBatch(maxRowPerBatch);
        printer = createColumnPrinter(line, reader->getType());

        reader->next(*batch);
        printer->reset(*batch);
    }

    ~OrcReader() {
        orc::Reader * rd = reader.release();
        delete rd;

        orc::ColumnVectorBatch * cvb = batch.release();
        delete cvb;

        orc::ColumnPrinter * cp = printer.release();
        delete cp;
    }


/* iteratively get one line record.
 * return: false means no next record.
 * */
    bool OrcGetNext(char ** tuple) {
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
        else {/*curRow < batch->numElements*/
            printer->printRow(curRow, tuple, 0);
            curRow++;

            return true;
        }
    }
/**
 * Get the number of rows in the file.
 * @return the number of rows
 */
    unsigned long long OrcGetTupleCount(const char* filename) {
        return reader->getNumberOfRows();
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
};

std::unordered_map<const char*, OrcReader*> readerMap;//filename, OrcReader

// wrapper functions:

void initOrcReader(const char* filename, unsigned int fdwColNum, unsigned int fdwMaxRowPerBatch) {
    OrcReader * orcreader = new OrcReader(filename, fdwColNum, fdwMaxRowPerBatch);
    readerMap[filename] = orcreader;
}

void releaseOrcReader(const char* filename) {
    if(readerMap.find(filename) != readerMap.end()) {// already existed
        free(readerMap[filename]);
        readerMap[filename] = NULL;
        readerMap.erase(filename);
    }
}

bool getOrcNextTuple(const char* filename, char ** tuple) {
    if(readerMap.find(filename) == readerMap.end()) {
        // haven't initialized
        return false;
    }

    OrcReader* orcreader = readerMap[filename];
    return orcreader->OrcGetNext(tuple);
}

unsigned long long getOrcTupleCount(const char* filename) {
    if(readerMap.find(filename) == readerMap.end()) {
        // haven't initialized
        return 0;
    }

    OrcReader* orcreader = readerMap[filename];
    return orcreader->OrcGetTupleCount(filename);
}
