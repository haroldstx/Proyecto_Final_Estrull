#include "Format.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>

#include "nlohmann/json.hpp"

using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::ifstream;
using std::ofstream;
using std::stringstream;
using std::vector;
using nlohmann::json;
using std::to_string;

void json_Help(string name, string data, int type, bool isLast) {
    if (type == 0) {
        if (isLast == true)
            cout << "   \"" << name << "\": " << "\"" << data << "\"" << endl;
        else
            cout << "   \"" << name << "\": " << "\"" << data << "\"" << "," << endl;
    }
    else {
        if (isLast == true)
            cout << "   \"" << name << "\": " << data << endl;
        else
            cout << "   \"" << name << "\": " << data << "," << endl;
    }

}

string getHeader(string Name_F) {
    string headerData = "";
    ifstream inputFile(Name_F);
    getline(inputFile, headerData, '\n');
    return headerData;
}
string trim_camp(const string& field) {
    string trimmedField = field;
    size_t lastNonSpace = trimmedField.find_last_not_of(' ');
    if (lastNonSpace != std::string::npos) {
        trimmedField.erase(lastNonSpace + 1);
    }
    else {
        trimmedField.clear();
    }
    return trimmedField;
}

string getM_Data(string headerData, int data) {
    string fileInformation = "";
    stringstream ss(headerData);
    getline(ss, fileInformation, '/');
    stringstream ss2(fileInformation);
    string fieldsNumber, recordLength, primaryKey, secondaryKey, secondaryKey2, numberOfRecords;
    ss2 >> fieldsNumber >> recordLength >> primaryKey >> secondaryKey >> secondaryKey2 >> numberOfRecords;
    switch (data) {
    case 0:
        fileInformation = fieldsNumber;
        break;
    case 1:
        fileInformation = recordLength;
        break;
    case 2:
        fileInformation = primaryKey;
        break;
    case 3:
        fileInformation = secondaryKey;
        break;
    case 4:
        fileInformation = secondaryKey2;
        break;
    case 5:
        fileInformation = numberOfRecords;
        break;
    }
    return fileInformation;
}

string getNameFile(string fileName, string secondary, string type, int charToRemove) {
    string newFile = "";
    string name = "";
    if (type == "BIN") {
        newFile = fileName.substr(0, fileName.length() - charToRemove);
        stringstream ss(newFile);
        getline(ss, newFile, '.');
        getline(ss, newFile, '.');
        newFile = "." + newFile + ".bin";
    }
    else if (type == "IDX") {
        newFile = fileName.substr(0, fileName.length() - charToRemove);
        stringstream ss(newFile);
        getline(ss, newFile, '.');
        getline(ss, newFile, '.');
        newFile = "." + newFile + ".idx";
    }
    else if (type == "CSV") {
        newFile = fileName.substr(0, fileName.length() - charToRemove);
        stringstream ss(newFile);
        getline(ss, newFile, '.');
        getline(ss, newFile, '.');
        newFile = "." + newFile + ".csv";
    }
    else if (type == "SDX") {
        newFile = fileName.substr(0, fileName.length() - charToRemove);
        stringstream ss(newFile);
        getline(ss, newFile, '.');
        getline(ss, newFile, '.');
        newFile = "." + newFile + "-" + secondary + ".sdx";
    }
    return newFile;
}


void setIndex(string dataFile, string indexFile, int keyPosition, int initialOffset, int recordsize) {
    ifstream inputFile(dataFile);
    ofstream outputFile(indexFile);
    int offset = initialOffset;
    int length = 0;
    string line;
    getline(inputFile, line, '\n');
    while (getline(inputFile, line, '\n')) {
        int pos = 0;
        stringstream ss(line);
        string field;
        offset = offset + length + 1;
        while (getline(ss, field, ',')) {
            if (pos == keyPosition) {
                outputFile.write(field.c_str(), field.length());
                outputFile.write(",", 1);
                outputFile.write(to_string(offset).c_str(), to_string(offset).length());
                outputFile.write("\n", 1);
            }
            pos++;
        }
        length = recordsize;
    }
}
void getData(string fileName, int initialOffset, int recordLength, vector<Structure> fileInformation, int numberOfRecords, bool found) {
    ifstream inputFile(fileName);
    int offset = initialOffset;
    int fieldsNumber = stoi(getM_Data(getHeader(fileName), 0));
    if (found == true) {
        cout << "[" << endl;
        for (int i = 0; i < numberOfRecords; i++) {
            int pos = 1;
            bool isLast = false;
            cout << " {" << endl;
            for (Structure f : fileInformation) {
                inputFile.seekg(offset);
                string field(f.length, '\0');
                //inputFile.read(&field[0], f.length);
                inputFile.read(reinterpret_cast<char*>(&field[0]), f.length);
                if (pos == fieldsNumber) {
                    isLast = true;
                }
                if (f.type == "char") {
                    json_Help(f.name, trim_camp(field), 0, isLast);
                }
                else {
                    json_Help(f.name, trim_camp(field), 1, isLast);
                }
                if (pos == fieldsNumber) {
                    offset = offset + f.length + 1;
                }
                else {
                    offset = offset + f.length;
                }
                pos++;
            }
            if (i == numberOfRecords - 1) {
                cout << " }" << endl;
            }
            else {
                cout << " }," << endl;
            }
        }
        cout << "]" << endl;
    }
    else {
		cout << "No se encontro el registro" << endl;
    }
}

bool Insert(string header, string record, string mainFile) {
    ofstream outputFile(mainFile, std::ios::app);
    outputFile.write(record.c_str(), record.length());
    outputFile.write("\n", 1);
    outputFile.close();
    return false;
}

int setRecords(string dataFile, string mainFile, string header, vector<Structure> fileStructureList) {
    ofstream outputFile(mainFile);
    outputFile.write(header.c_str(), header.length());
    outputFile.write("\n", 1);
    outputFile.close();
    ifstream inputFile(dataFile);
    string line2, headerCSV, field;
    int numberOfRecords = 0;
    getline(inputFile, headerCSV, '\n');
    while (getline(inputFile, line2, '\n')) {
        string record = "";
        stringstream ss(line2);
        for (Structure fs : fileStructureList) {
            getline(ss, field, ',');
            format_of_Field(field, fs.length);
            record = record + field;
        }
        Insert("", record, mainFile);
        numberOfRecords++;
    }
    inputFile.close();
    return numberOfRecords;
}

vector<Index_OfS> setIndexToMemory(string indexFile) {
    Index_OfS recordIndex;
    vector<Index_OfS> keyVec;
    ifstream inputFile(indexFile);
    string line, key, offset;
    while (getline(inputFile, line, '\n')) {
        stringstream ss(line);
        getline(ss, key, ',');
        getline(ss, offset, ',');
        recordIndex.key = key;
        recordIndex.offset = stoi(offset);
        keyVec.push_back(recordIndex);
    }
    return keyVec;
}

string parseJson(string jsonInput, vector<Structure> fileStructureList) {
    stringstream ss(jsonInput);
    string fieldName, fieldData, field, record;
    int i = 0;
    while (getline(ss, field, ',')) {
        stringstream ss2(field);
        getline(ss2, fieldName, ':');
        getline(ss2, fieldData, ',');
        format_of_Field(fieldData, fileStructureList[i].length);
        record = record + fieldData;
        i++;
    }
    return record;
}

string getKey(string jsonInput, vector<Structure> fileStructureList, int keyPosition) {
    stringstream ss(jsonInput);
    string fieldName, fieldData, field, key;
    int i = 0;
    while (getline(ss, field, ',')) {
        stringstream ss2(field);
        getline(ss2, fieldName, ':');
        getline(ss2, fieldData, ',');
        if (i == keyPosition) {
            key = fieldData;
        }
        i++;
    }
    return key;
}

Meta_Data setMeta_F(string headerData) {
    Meta_Data meta;
    int counter = 0;
    string fieldsNumber, recordLength, index, secIndex, secIndex2;
    string dataSection = "";
    stringstream ss(headerData);
    while (getline(ss, dataSection, '/')) {
        stringstream ss2(dataSection);
        if (counter == 0) {
            ss2 >> fieldsNumber >> recordLength >> index >> secIndex >> secIndex2;
            meta.fieldsNumber = stoi(fieldsNumber);
            meta.recordLength = stoi(recordLength);
            meta.indexFile = index;
            meta.secIndexFile = secIndex;
            meta.secIndexFile2 = secIndex2;
        }
        counter++;
    }
    return meta;
}

void updateIndex(string key, int offset, string indexFileName, bool append, int lineNumber) {
    ofstream outputFile;

    std::ios_base::openmode mode = std::ios::binary;
    string index = key + "," + to_string(offset);
    if (append) {
        mode |= std::ios::app;
    }
    else {
        mode |= std::ios::in | std::ios::out;
    }
    outputFile.open(indexFileName, mode);
    if (outputFile.is_open()) {
        if (!append) {
            vector<Index_OfS> indexList = setIndexToMemory(indexFileName);
            for (Index_OfS is : indexList) {
                if (is.offset == offset) {
                    is.key = key;
                }
                index = is.key + "," + to_string(is.offset);
                outputFile.write(index.c_str(), index.length());
                outputFile.write("\n", 1);
            }
        }
        else {
            outputFile.write(index.c_str(), index.length());
            outputFile.write("\n", 1);
        }
    }
    outputFile.close();
}

int reindexar(int recordLength, vector<Index_OfS> indexList) {
    int offset = indexList[indexList.size() - 1].offset + recordLength + 1;
    return offset;
}




bool insertRecord(string fileName, string record, vector<Structure> fileInformation, vector<Index_OfS> indexList) {
    bool notRepeated = true;
    ofstream outputFile(fileName, std::ios::binary | std::ios::ate | std::ios::app);
    bool insertionOK = true;
    stringstream ss(record);
    string field;
    string formatedRecord;
    int pos = 0;
    while (getline(ss, field, ',')) {
        stringstream ss2(field);
        string name, data;
        getline(ss2, name, ':');
        getline(ss2, data, ',');
        if (pos == 0) {
            for (Index_OfS i : indexList) {
                if (i.key == data) {
                    notRepeated = false;
                }
            }
        }
        format_of_Field(data, fileInformation[pos].length);
        formatedRecord = formatedRecord + data;
        pos++;
    }
    if (notRepeated == false) {
        cerr << "{\"result\": \"ERROR\", \"error\": \"The primary key already exists\"}" << endl;
        return notRepeated;
    }
    else {
        outputFile.write(formatedRecord.c_str(), 49);
        outputFile.write("\n", 1);
        outputFile.close();
        return insertionOK;
    }
}

int get_Number_Records(string fileName) {
    int numberOfRecords = 0;
    string line = "";
    ifstream inputFile(fileName);
    getline(inputFile, line, '\n');
    while (getline(inputFile, line, '\n')) {
        numberOfRecords++;
    }
    return numberOfRecords;
}

int getFieldPosition(string keyName, vector<Structure> fileStructureList) {
    int keyPosition = 0, i = 0;
    string field;
    for (Structure fs : fileStructureList) {
        if (fs.name == keyName) {
            keyPosition = i;
        }
        i++;
    }
    return keyPosition;
}

vector<Structure> setFileStructure(string header) {
    Structure fileStruct;
    vector<Structure> fileInformation;
    int counter = 0;
    string name, type, length;
    string dataSection = "";
    stringstream ss(header);
    while (getline(ss, dataSection, '/')) {
        stringstream ss2(dataSection);
        if (counter != 0) {
            ss2 >> name >> type >> length;
            fileStruct.name = name;
            fileStruct.type = type;
            fileStruct.length = stoi(length);
            fileInformation.push_back(fileStruct);
        }
        counter++;
    }
    return fileInformation;
}

void convertoJson(string dataFile) {
    cout << "{" << endl;
    cout << "  \"fields\": [" << endl;
    string header = getHeader(dataFile);
    vector<Structure> fileInformation = setFileStructure(header);
    size_t i = 0;
    for (Structure f : fileInformation) {
        if (i != fileInformation.size() - 1) {
            cout << "    {\"name\": \"" << f.name << "\", \"type\": \"" << f.type << "\", \"length\": " << f.length << "}, " << endl;
        }
        else {
            cout << "    {\"name\": \"" << f.name << "\", \"type\": \"" << f.type << "\", \"length\": " << f.length << "} " << endl;
        }
        i++;
    }
    cout << "  ]," << endl;
    ifstream inputFile(dataFile);
    string line = "", field = "", primaryKey = "", secondaryKey = "", secondaryKey2 = "";
    int recordsNumber = get_Number_Records(dataFile);
    getline(inputFile, line, '/');
    stringstream ss(line);
    int counter = 0;
    while (getline(ss, field, ' ')) {
        if (counter == 2) {
            primaryKey = field;
        }
        if (counter == 3) {
            secondaryKey = field;
        }
        if (counter == 4) {
            secondaryKey2 = field;
        }
        counter++;
    }
    cout << "  \"PK\": " << "\"" << primaryKey << "\"," << endl;
    cout << "  \"SK\": " << "[\"" << secondaryKey << "\", " << "\"" << secondaryKey2 << "\"]," << endl;
    cout << "  \"Records\": " << recordsNumber << endl;
    cout << "]" << endl;
}

int getOffset(string key, vector<Index_OfS> indexList) {
    int offset = -1;
    for (Index_OfS fs : indexList) {
        if (fs.key == key) {
            offset = fs.offset;
        }
    }

    if (offset == -1)
        cerr << "{\"result\":\"not found\"}" << endl;

    return offset;
}

bool check_Archivos(string fileName, string Type) {
    bool file = true;
    if (Type == "JSON") {
        if (fileName.substr(fileName.length() - 4) != "json") {
            cerr << "{\"result\": \"ERROR\", \"error\": \"Format not recognized\"}" << endl;
            file = false;
        }
    }
    else if (Type == "CSV") {
        if (fileName.substr(fileName.length() - 3) != "csv") {
            cerr << "{\"result\": \"ERROR\", \"error\": \"Format not recognized\"}" << endl;
            file = false;
        }
    }
    else if (Type == "BIN") {
        if (fileName.substr(fileName.length() - 3) != "bin") {
            cerr << "{\"result\": \"ERROR\", \"error\": \"Format not recognized\"}" << endl;
            file = false;
        }
    }
    if (file == true) {
        ifstream fileInput(fileName);
        if (!fileInput) {
            cerr << "{\"result\": \"ERROR\", \"error\": \"" << Type << " file not found\"}" << endl;
            file = false;
        }
    }
    return file;
}

bool check_Campos(string lineData, string fileWrite, vector<Structure> fileInformation, int caso) {
    bool Valid = true;
    string field;
    int fieldNum1 = 0;
    string fieldNum2 = "0";
    if (caso == 0) {
        ifstream fileW(fileWrite);
        string lineFile1, lineFile2;
        getline(fileW, lineFile2, '\n');
        stringstream ss1(lineData);
        stringstream ss2(lineFile2);
        getline(ss2, fieldNum2, ' ');
        while (getline(ss1, field, ',')) {
            fieldNum1++;
        }
        if (fieldNum1 != stoi(fieldNum2)) {
            Valid = false;
        }

        if (Valid == true) {
            int j = 0;
            string line = "";
            stringstream ss3(lineData);
            while (getline(ss3, field, ',')) {
                if (field != fileInformation[j].name) {
                    Valid = false;
                    break;
                }
                j++;
            }
        }
    }
    else if (caso == 1) {
        stringstream ss(lineData);
        string field;
        int j = 0;
        while (getline(ss, field, ',')) {
            stringstream ss2(field);
            string name;
            getline(ss2, name, ':');
            if (name != fileInformation[j].name) {
                Valid = false;
                break;
            }
            j++;
        }
    }
    if (Valid == false) {
        cerr << "{" << "\"result\": \"ERROR\", \"error\": \"CSV fields do not match the file structure\"}" << endl;
    }
    return Valid;
}