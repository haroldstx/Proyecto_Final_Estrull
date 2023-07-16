#include "Content.h";

using namespace std;

int main(int argc, char* argv[]) {
    bool file_fi = true;
    int contador = 0;
    string flag1 = argv[1];
    if (flag1 == "-create") {
        string fileName = argv[2];
        file_fi = check_Archivos(fileName, "JSON");
        if (file_fi == false)
            return 0;
        ifstream inputFile(fileName);
        int recordSize = 0, incidents = 0;
        string secondaryKey = "", secondaryKey2 = "";
        string mainFile = getNameFile(fileName, "", "BIN", 4);
        string indexFile = getNameFile(fileName, "", "IDX", 4);
        ofstream outputFile(mainFile);

        string fileData((std::istreambuf_iterator<char>(inputFile)), std::istreambuf_iterator<char>());
        json fileDescription = json::parse(fileData);
        int fieldsNumber = fileDescription["fields"].size();
        string primaryKey = fileDescription["primary-key"].get<string>().substr(0, fileDescription["primary-key"].get<string>().length());
        for (auto& element : fileDescription["secondary-key"]) {
            if (contador == 0)
                secondaryKey = element.get<std::string>().substr(0, element.get<std::string>().length());
            else if (contador == 1)
                secondaryKey2 = element.get<std::string>().substr(0, element.get<std::string>().length());
            contador++;
        }

        for (auto& element : fileDescription["fields"]) {
            string name = element["name"].get<std::string>().substr(0, element["name"].get<std::string>().length());
            if (primaryKey == name || secondaryKey == name || secondaryKey2 == name) {
                incidents++;
            }
            recordSize += element["length"].template get<int>();
        }

        string secIndexFile = "";
        string secIndexFile2 = "";
        if (incidents == 3) {
            secIndexFile = getNameFile(fileName, secondaryKey, "SDX", 5);
            secIndexFile2 = getNameFile(fileName, secondaryKey2, "SDX", 5);
            string header = to_string(fieldsNumber) + " " + to_string(recordSize) + " " + primaryKey + " " + secondaryKey + " " + secondaryKey2 + " " + "0";
            outputFile.write(header.c_str(), header.length());
            for (auto& element : fileDescription["fields"]) {
                string name = element["name"].get<std::string>().substr(0, element["name"].get<std::string>().length());
                string type = element["type"].get<std::string>().substr(0, element["type"].get<std::string>().length());
                int length = element["length"];
                outputFile << "/" << name << " ";
                outputFile << type << " ";
                outputFile << length;
            }
            outputFile << endl;
            ofstream outputIndex(indexFile);
            ofstream outputSecIndex(secIndexFile);
            ofstream outputSecIndex2(secIndexFile2);
            inputFile.close();
            outputIndex.close();
            outputSecIndex.close();
            outputSecIndex2.close();
        }
        else {
            cerr << "{\"resultado\": \"ERROR\", \"error\": \"El campo del indice primario no exite\"}" << endl;
            return 0;
        }
        cout << "{" << "\"resultado\": " << "\"Perfecto\", " << "\"Numero de campos\": " << fieldsNumber << ", \" Archivo\": " << "\"" << mainFile.substr(7, mainFile.length()) << "\", " << "\"index\": " << "\"" << indexFile.substr(7, indexFile.length()) << "\", " << "\"secondary\": " << "[\"" << secIndexFile.substr(7, secIndexFile.length()) << "\", " << "\"" << secIndexFile2.substr(7, secIndexFile2.length()) << "\"" << "]}" << endl;
    }
    else if (flag1 == "-file") {
        if (argc == 4) {
            string dataFile = argv[2];
            string flag2 = argv[3];
            file_fi = check_Archivos(dataFile, "BIN");
            if (file_fi == false)
                return 0;
            if (flag2 == "-compact") {
                cout << "Compact file" << endl;
            }
            else if (flag2 == "-reindex") {
                cout << "Reindex file" << endl;
            }
            else if (flag2 == "-describe") {
                convertoJson(dataFile);
            }
            else if (flag2 == "-GET") {
                string header = getHeader(dataFile);
                int numberOfRecords = get_Number_Records(dataFile);
                vector<Structure> fileInformation = setFileStructure(header);

                int offset = header.length() + 1;
                string recordLength = getM_Data(getHeader(dataFile), 1);
                getData(dataFile, offset, stoi(recordLength), fileInformation, numberOfRecords, true);
            }
        }
        else if (argc == 5) {
            string flag1 = argv[1];
            string dataFile = argv[2];
            string flag2 = argv[3];
            file_fi = check_Archivos(dataFile, "BIN");
            if (file_fi == false)
                return 0;
            if (flag2 == "-load") {
                string mainFile = argv[2];
                string dataFile = argv[4];
                string line;
                file_fi = check_Archivos(mainFile, "BIN");
                if (file_fi == false)
                    return 0;
                file_fi = check_Archivos(dataFile, "CSV");
                if (file_fi == false)
                    return 0;
                string indexFile = getNameFile(dataFile, "", "IDX", 3);
                string secIndexFile = getNameFile(dataFile, getM_Data(getHeader(mainFile), 3), "SDX", 4);
                string secIndexFile2 = getNameFile(dataFile, getM_Data(getHeader(mainFile), 4), "SDX", 4);
                string header = getHeader(mainFile);
                vector<Structure> fileStructureList = setFileStructure(header);
                
                bool isValid = check_Campos(getHeader(dataFile), mainFile, fileStructureList, 0);
                if (isValid == false)
                    return 0;
              
                int initialOffset = getHeader(mainFile).length();
                int recordLength = stoi(getM_Data(getHeader(mainFile), 1));
                int pkPosition = 0;
                int skPosition = 0;

                cout << header << endl;
                int numberOfRecords = setRecords(dataFile, mainFile, header, fileStructureList);
                pkPosition = getFieldPosition(getM_Data(getHeader(mainFile), 2), fileStructureList);
                setIndex(dataFile, indexFile, pkPosition, initialOffset, recordLength);
                skPosition = getFieldPosition(getM_Data(getHeader(mainFile), 3), fileStructureList);
                setIndex(dataFile, secIndexFile, skPosition, initialOffset, recordLength);
                skPosition = getFieldPosition(getM_Data(getHeader(mainFile), 4), fileStructureList);
                setIndex(dataFile, secIndexFile2, skPosition, initialOffset, recordLength);
                cout << "{" << "\"result\": " << "\"OK\", " << "\"records\": \"" << numberOfRecords << "\"]}" << endl;
            }
            else {
                cerr << "{" << "\"result\": \"ERROR\", \"error\": \"The entered flags are invalid\"}" << endl;
            }
        }
        else if (argc == 6) {
            string dataFile = argv[2];
            string flag2 = argv[3];
            string flag3 = argv[4];
            string flag4 = argv[5];
            string flag3Temp = flag3.substr(0, 3);
            string keyValue = flag4.substr(7);
            flag4 = flag4.substr(0, 6);
            if (flag2 != "-GET" || (flag3 != "-pk" && flag3Temp != "-sk") || flag4 != "-value") {
                cerr << "{" << "\"result\": \"ERROR\", \"error\": \"The entered flags are invalid\"}" << endl;
                return 0;
            }
            if (flag3 == "-pk") {
                bool found = false;
                string indexFile = getNameFile(dataFile, "", "IDX", 3);
                vector<Index_OfS> indexList = setIndexToMemory(indexFile);
                ifstream inputFile(dataFile);
                int offset = 0;
                for (Index_OfS i : indexList) {
                    if (i.key == keyValue) {
                        offset = i.offset;
                        found = true;
                    }
                }
                string header = "";
                getline(inputFile, header, '\n');
                vector<Structure> fileInformation = setFileStructure(header);
                int recordLength = stoi(getM_Data(getHeader(dataFile), 1));
                getData(dataFile, offset, recordLength, fileInformation, 1, found);
            }
            else if (flag3Temp == "-sk") {
                bool found = false;
                int offset;
                string secondaryKey = flag3.substr(4, flag3.length());
                string csvFileName = getNameFile(dataFile, "", "CSV", 3);
                string header = getHeader(csvFileName);
                string indexFileName = getNameFile(dataFile, secondaryKey, "SDX", 4);
                ifstream inputFile(indexFileName);
                vector<Index_OfS> indexList = setIndexToMemory(indexFileName);
                string header2 = getHeader(dataFile);
                vector<Structure> fileInformation = setFileStructure(header2);
                int recordLength = stoi(getM_Data(getHeader(dataFile), 1));
                for (Index_OfS i : indexList) {
                    if (i.key == keyValue) {
                        offset = i.offset;
                        found = true;
                        getData(dataFile, offset, recordLength, fileInformation, 1, found);
                    }
                }
                if (found == false) {
                    cout << "[]" << endl;
                }
            }
        }
        else {
            string mainFile = argv[2];
            bool fileOK = check_Archivos(mainFile, "BIN");
            if (fileOK == false)
                return 0;
            string flag2 = argv[3];
            string header = getHeader(mainFile);
            string indexFile = getNameFile(mainFile, "", "IDX", 3);
            vector<Index_OfS> indexList = setIndexToMemory(indexFile);
            vector<Structure> fileStructureList = setFileStructure(header);
            int recordLength = stoi(getM_Data(header, 1));
            int fieldPosition = 0;
            string key = "";
            if (flag2 == "-POST") {
                string dataInput = "";
                for (int i = 4; i < argc; i++) {
                    dataInput = dataInput + argv[i];
                }
                string jsonString = dataInput.substr(7, dataInput.length() - 8);
                string flag3 = dataInput.substr(0, 5);
                fileOK = check_Campos(jsonString, "", fileStructureList, 1);
                if (fileOK == false)
                    return 0;
                if (flag3 == "-data") {
                    string record = parseJson(jsonString, fileStructureList);
                    Insert(header, record, mainFile);

                    int offset = reindexar(recordLength, indexList);
                    fieldPosition = getFieldPosition(getM_Data(header, 2), fileStructureList);
                    key = getKey(jsonString, fileStructureList, fieldPosition);
                    updateIndex(key, offset, indexFile, true, 0);

                    fieldPosition = getFieldPosition(getM_Data(header, 3), fileStructureList);
                    key = getKey(jsonString, fileStructureList, fieldPosition);
                    indexFile = getNameFile(mainFile, getM_Data(header, 3), "SDX", 4);
                    updateIndex(key, offset, indexFile, true, 0);

                    fieldPosition = getFieldPosition(getM_Data(header, 4), fileStructureList);
                    key = getKey(jsonString, fileStructureList, fieldPosition);
                    indexFile = getNameFile(mainFile, getM_Data(header, 4), "SDX", 4);
                    updateIndex(key, offset, indexFile, true, 0);
                    cout << "{" << "\"result\": " << "\"OK\"}" << endl;
                }
                else {
                    cerr << "{" << "\"result\": \"ERROR\", \"error\": \"The entered flags are invalid\"}" << endl;
                    return 0;
                }
            }
            else if (flag2 == "-PUT") {
                string flag4 = argv[4];
                string primaryKey = flag4.substr(4, flag4.length());
                flag4 = flag4.substr(0, 3);
                if (flag4 == "-pk") {
                    int offset = getOffset(primaryKey, indexList);
                    if (offset == -1)
                        return 0;
                    string dataInput = "";
                    for (int i = 5; i < argc; i++) {
                        dataInput = dataInput + argv[i];
                    }
                    string jsonString = dataInput.substr(7, dataInput.length() - 8);
                    string flag5 = dataInput.substr(0, 5);
                    fileOK = check_Campos(jsonString, "", fileStructureList, 1);
                    if (fileOK == false)
                        return 0;
                    if (flag5 == "-data") {
                        string record = parseJson(jsonString, fileStructureList);
                        ofstream outputFile(mainFile, std::ios::in | std::ios::out);
                        outputFile.seekp(offset);
                        outputFile.write(record.c_str(), recordLength);
                        outputFile.close();
                        int headerLength = header.length();
                        int recordLength = record.length();

                        fieldPosition = getFieldPosition(getM_Data(header, 2), fileStructureList);
                        key = getKey(jsonString, fileStructureList, fieldPosition);
                        updateIndex(key, offset, indexFile, false, 5);

                        fieldPosition = getFieldPosition(getM_Data(header, 3), fileStructureList);
                        key = getKey(jsonString, fileStructureList, fieldPosition);
                        indexFile = getNameFile(mainFile, getM_Data(header, 3), "SDX", 4);
                        updateIndex(key, offset, indexFile, false, 5);

                        fieldPosition = getFieldPosition(getM_Data(header, 4), fileStructureList);
                        key = getKey(jsonString, fileStructureList, fieldPosition);
                        indexFile = getNameFile(mainFile, getM_Data(header, 4), "SDX", 4);
                        updateIndex(key, offset, indexFile, false, 5);

                        cout << "[" << "resultado :" << "\"Correcto\"]" << endl;
                    }
                    else {
                        cerr << "\"error\": \"No existen esas banderas !\" " << endl;
                        return 0;
                    }
                }
                else {
                    cerr << "[" << "\"resultado\":, \"error\": \"No existen esas banderas!\"]" << endl;
                    return 0;
                }
            }
            else {
                cerr << "[" << "\"resultado\":, \"Las flags ingresadas no existen!\"]" << endl;
                return 0;
            }
        }
    }
    else {
        cerr << "[" << "\"resultado\": \"ERROR\", \"error\": \"Las flags ingresadas no existen!\"]" << endl;
        return 0;
        
    }
    return 0;
}