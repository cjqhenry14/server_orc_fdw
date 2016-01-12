#include "orcLibBridge.h"
#include "orcInclude/ColumnPrinter.hh"

#include <memory>
#include <string>
#include <iostream>
#include <exception>
#include <unordered_map>


class OrcReader {
public:
/*global variable*/
    unsigned int colNum;
    unsigned int maxRowPerBatch;
    std::string line;
    unsigned long curRow;

    orc::ReaderOptions opts;

    std::unique_ptr<orc::Reader> reader;
    std::unique_ptr<orc::ColumnVectorBatch> batch;
    std::unique_ptr<orc::ColumnPrinter> printer;

    /* init global var, should be used in BeginForeignScan() */
    OrcReader(const char* filename, unsigned int fdwColNum, unsigned int fdwMaxRowPerBatch) {
        colNum = fdwColNum;
        curRow = 0;//don't forget
        maxRowPerBatch = fdwMaxRowPerBatch;

        reader = orc::createReader(orc::readLocalFile(std::string(filename)), opts);
        batch = reader->createRowBatch(maxRowPerBatch);
        printer = createColumnPrinter(line, reader->getType());

        /* get first batch of record */
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
    bool OrcGetNext(char **tuple) {
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
};

std::unordered_map<const char*, OrcReader*> readerMap;//<filename, OrcReader>


// wrapper functions:

/* init global var, should be used in BeginForeignScan() */
void initOrcReader(const char* filename, unsigned int fdwColNum, unsigned int fdwMaxRowPerBatch) {
    if(readerMap.find(filename) != readerMap.end()) {// already existed
        free(readerMap[filename]);
        readerMap[filename] = NULL;
        readerMap.erase(filename);
    }

    OrcReader * orcreader = new OrcReader(filename, fdwColNum, fdwMaxRowPerBatch);
    readerMap[filename] = orcreader;
}

/* release tuple memory, should be used in EndForeignScan() */
void releaseOrcReader(const char* filename) {
    if(readerMap.find(filename) != readerMap.end()) {// already existed
        free(readerMap[filename]);
        readerMap[filename] = NULL;
        readerMap.erase(filename);
    }
}

/**
 * iteratively get one line record, should be used in IterativeForeignScan()
 * @return: false means no next record.
 */
bool getOrcNextTuple(const char* filename, char **tuple) {
    if(readerMap.find(filename) == readerMap.end()) {
        // haven't initialized
        return false;
    }

    OrcReader* orcreader = readerMap[filename];
    return orcreader->OrcGetNext(tuple);
}

/**
 * Get the number of rows in the file.
 * @return the number of rows
 */
unsigned long long getOrcTupleCount(const char* filename) {
    if(readerMap.find(filename) == readerMap.end()) {
        // haven't initialized
        return 0;
    }

    OrcReader* orcreader = readerMap[filename];
    return orcreader->OrcGetTupleCount(filename);
}
