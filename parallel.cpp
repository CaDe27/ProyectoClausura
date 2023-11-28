/*
    -0. broadcast book titles to all processes
    
    For each process:
    1. calculate sum bytes across all books
    2. calculate which bytes you have to iterate 
    3. locate the book and byte you start at
    4. while not finishing your bytes nor the current book, process they word
    5. if you finished a book, augment the indx, and open the according file

    6. Reduce all arrays in root process 
    7. write to final file  
*/

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <thread>
#include <mpi.h>

using namespace std;

string* broadcastBookTitles(int root, int process_id, int numberOfBooks, char *argv[]){
    char *titles[numberOfBooks]; 
    int titleLengths[numberOfBooks]; 
    
    if(process_id == root){
        for (int i = 0; i < numberOfBooks; i++) {
            titleLengths[i] = strlen(argv[i + 1]) + 1; // +1 for null terminator
            titles[i] = new char[titleLengths[i]];
            strcpy(titles[i], argv[i + 1]);
        }
    }

    // Broadcast lengths of titles to all processes
    MPI_Bcast(titleLengths, numberOfBooks, MPI_INT, 0, MPI_COMM_WORLD);

    if (process_id != 0) {
        // Allocate memory for titles in non-root processes
        for (int i = 0; i < numberOfBooks; i++) {
            titles[i] = new char[titleLengths[i]];
        }
    }

    // Broadcast titles
    for (int i = 0; i < numberOfBooks; i++) {
        MPI_Bcast(titles[i], titleLengths[i], MPI_CHAR, 0, MPI_COMM_WORLD);
    }

    string *bookTitles = new string[numberOfBooks];
    for (int i = 0; i < numberOfBooks; i++) {
        bookTitles[i] = string(titles[i]);
    }

    // Clean up: Delete the allocated memory in all processes
    for (int i = 0; i < numberOfBooks; i++) {
        delete[] titles[i];
    }

    return bookTitles;
}

int getFileSize(const string& filePath) {
    // not closing the file at the end because it ends its scope
    ifstream file(filePath, ios::binary | ios::ate);
    return file.tellg();
}

int getTotalBytesToProcess(const string *bookTitles, int numberOfBooks){
    int totalBytes = 0;
    for(int i = 0; i < numberOfBooks; ++i){
        totalBytes += getFileSize("books/"+bookTitles[i]+".txt");
    }
    return totalBytes;
}

void loadVocabulary(map<string, int> &frequencies){
    ifstream file("vocabulary.csv");
    // the file contains only one line, with all the words
    // separated by commas. We read them into the vocabulary_csv string
    string word;
    // this reads the line until the next comma
    while (getline(file, word, ',')) {
        // we add the word to a dictionary
        frequencies.insert(make_pair(word, 0));
    }
    //we close the file
    file.close();
}

void saveVocabularyToFile(map<string, int> &frequencies, ofstream &outputFile){
    outputFile<<"book";
    for(map<string, int>::iterator word_frequency_it = frequencies.begin(); 
        word_frequency_it != frequencies.end(); ++word_frequency_it){
        outputFile<<","<<word_frequency_it->first;
    }
    outputFile<<"\n";
}

void saveFrequenciesToOutputFile(ofstream &outputFile, int vocabulary_size, int bookIndx, const string bookTitle, int* gatheredValues){
    outputFile<<bookTitle;
    for(int i = vocabulary_size*bookIndx; i < vocabulary_size*(bookIndx+1); ++i){
        outputFile<<","<<gatheredValues[i];
    }
    outputFile<<"\n";
}

ifstream locateAtStartingWordFromByte(const string& bookPath, int startLoadByte, int& accumLoadBytes){
    ifstream file(bookPath, ios::binary);

    int startBookByte = startLoadByte - accumLoadBytes;
    if(startBookByte > 0){
        // Go to one byte before the target start byte
        file.seekg(startBookByte - 1);
        accumLoadBytes = startLoadByte-1;
        // Check if the previous byte is a comma
        char ch;
        do{
            file.get(ch);
            ++accumLoadBytes;
        }while(ch != ',');
    }
    return file;
}

void saveFrequenciesInOutputTable(int* localFrequencyTable, int bookId, int vocabularySize, map<string, int> frequencies){
    int indx = vocabularySize * bookId;
    for(map<string, int>::iterator it = frequencies.begin(); it != frequencies.end(); ++it, ++indx){
        localFrequencyTable[indx] = it->second;
    }
}

int main(int argc, char* argv[]){
    int numberOfBooks = argc-1;
    if(argc != 7){
        cerr<<"There should be exactly six book names in the command line arguments.";
        return 1;
    }

    // MPI Initialization
    const int root = 0;
    int num_processes = 0;
    int process_id = 0;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &num_processes);
    MPI_Comm_rank(MPI_COMM_WORLD, &process_id);

    // send each thread all book titles
    string* bookTitles = broadcastBookTitles(root, process_id, numberOfBooks, argv);    
 
    // calculate the load for each process
    int totalSize = getTotalBytesToProcess(bookTitles, numberOfBooks);
    int bytesPerProcess = (totalSize + num_processes - 1)/num_processes;
    // get the range you have to process    
    int startLoadByte = bytesPerProcess*process_id;
    int endLoadByte = min(startLoadByte+bytesPerProcess-1, totalSize);
    //cout<<process_id<<" "<<startLoadByte<<" "<<endLoadByte<<endl;

    map<string, int> frequencies;
    loadVocabulary(frequencies);
    int vocabularySize = frequencies.size();
    
    // create the final matrix we want
    // it will be reduced at the end across all processes
    double totalTime = 0;
    double start, end;
    int* localFrequencyTable = new int [ vocabularySize * numberOfBooks];
    
    fill(localFrequencyTable, localFrequencyTable + vocabularySize*numberOfBooks, 0);
    if(process_id == root)
        start = MPI_Wtime();

    // ======= preprocessing to iterate over the designated load
    int bookId = 0;
    int accumLoadBytes = 0;
    string currentBookPath = "books/"+bookTitles[bookId]+".txt";
    int currentBookSize = getFileSize(currentBookPath);
    while(startLoadByte > accumLoadBytes + currentBookSize){
        accumLoadBytes += currentBookSize;
        ++bookId;
        currentBookPath = "books/"+bookTitles[bookId]+".txt";
        currentBookSize = getFileSize(currentBookPath);
    }
    // locate yourself at that book's byte
    ifstream fileIterator = locateAtStartingWordFromByte(currentBookPath, startLoadByte, accumLoadBytes);
    // ======= End of preprocessing
    while(accumLoadBytes < endLoadByte){
        string word;
        map<string, int>::iterator mapIt;
        while(getline(fileIterator, word, ',') && accumLoadBytes < endLoadByte){
            accumLoadBytes += word.size() + 1;
            mapIt = frequencies.find(word);
            if(mapIt != frequencies.end())
                ++(mapIt->second);
        }
        
        saveFrequenciesInOutputTable(localFrequencyTable, bookId, vocabularySize, frequencies);
        if(fileIterator.eof()){
            --accumLoadBytes;
            ++bookId;
            if (bookId < numberOfBooks && accumLoadBytes < endLoadByte){
                currentBookPath = "books/"+bookTitles[bookId]+".txt";
                fileIterator = ifstream(currentBookPath, ios::binary);
                for(map<string, int>::iterator clearingIt = frequencies.begin(); clearingIt != frequencies.end(); ++clearingIt){
                    clearingIt->second = 0;
                }
            }     
        } 
        else{
            fileIterator.close();
        }
    }

    if(process_id == root){
        end = MPI_Wtime();
        cout<<setprecision(7)<<(end-start)<<"s"<<endl;
    }

    if(process_id == root){
        MPI_Reduce(MPI_IN_PLACE, localFrequencyTable, vocabularySize*numberOfBooks, MPI_INT, MPI_SUM, root, MPI_COMM_WORLD);
    }
    else{
        MPI_Reduce(localFrequencyTable, NULL, vocabularySize*numberOfBooks, MPI_INT, MPI_SUM, root, MPI_COMM_WORLD);
    }

    if(process_id == root){
        ofstream frequenciesFile("parallelBagOfWords.csv");
        saveVocabularyToFile(frequencies, frequenciesFile);
        for(int i = 0; i < numberOfBooks; ++i){
            saveFrequenciesToOutputFile(frequenciesFile, frequencies.size(), i, bookTitles[i], localFrequencyTable);
        }
        frequenciesFile.close();
    }

    delete[] localFrequencyTable;
    delete[] bookTitles;
    MPI_Finalize();
    return 0;
}