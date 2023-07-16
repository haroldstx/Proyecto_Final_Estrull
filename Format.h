#include <string>;
using std::string;

struct Structure {
    string name;
    string type;
    int length;
};

struct Meta_Data {
    int fieldsNumber;
    int recordLength;
    string indexFile;
    string secIndexFile;
    string secIndexFile2;
    int recordsNumber;
};

struct Index_OfS {
    string key;
    int offset;
};

void format_of_Field(std::string& field, int size) {
    field.resize(size, ' ');
}