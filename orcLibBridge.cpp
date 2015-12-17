#include "orcLibBridge.h"
#include "orcInclude/ColumnPrinter.hh"

#include <memory>
#include <string>
#include <iostream>
#include <exception>


std::string line;
char * fakefilename = "/usr/pgsql-9.4/city.orc";

orc::ReaderOptions opts;
/*init first, avoid init with abstract obj. Then change in initOrcReader()*/
std::unique_ptr<orc::Reader> reader = orc::createReader(orc::readLocalFile(std::string(fakefilename)), opts);
std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(1000);
std::unique_ptr<orc::ColumnPrinter> printer = createColumnPrinter(line, reader->getType());


void initOrcReader(const char* filename) {
    reader = orc::createReader(orc::readLocalFile(std::string(filename)), opts);
    batch = reader->createRowBatch(1000);
    printer = createColumnPrinter(line, reader->getType());

    reader->next(*batch);
    printer->reset(*batch);
}

unsigned long curRow=0;

/*iteratively get one line record, for testing*/
char* getLine() {

    line.clear();

    if(batch->numElements == 0)
        return NULL;

    if(curRow == batch->numElements) {
        if (reader->next(*batch)) {

            printer->reset(*batch);

            if(batch->numElements == 0)
                return NULL;

            curRow = 0;
            printer->printRow(curRow);
            curRow++;

            char *p = const_cast<char*>(line.c_str());
            return p;
        }
        else
            return NULL;
    }
    else {
        printer->printRow(curRow);
        curRow++;
        char *p = const_cast<char*>(line.c_str());
        return p;
    }
}
