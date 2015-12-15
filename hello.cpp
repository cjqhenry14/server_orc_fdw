#include "hello.h"
#include <string>

class people {
public:
    int show(int age) {
        int mg = age + 1;
        return mg;
    }
};

int hello(int age)
{
    std::string str = "abc";
    people p;
    age = p.show(age);
    return age;
}
