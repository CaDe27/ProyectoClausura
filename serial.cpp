#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
# include <map>
using namespace std;

void loadVocabulary(map<string, int> &frequencies){
    ifstream file("vocabulary.csv");

    if (!file.is_open()) {
        cerr << "Error opening file" << endl;
        return;
    }

    // the file contains only one line, with all the words
    // separated by commas. We read them into the vocabulary_csv string
    string vocabulary_csv;
    getline(file, vocabulary_csv);
    
    // we create a stringstrem, which we will use to get each word
    stringstream ss(vocabulary_csv);
    string word;

    // this reads the line until the next comma
    while (getline(ss, word, ',')) {
        // we first eliminate the first space
        word = word.substr(1, word.size());
        // we add the word to a dictionary
        frequencies.insert(make_pair(word, 0));
    }

    //we close the file
    file.close();
}

// It returns a stream for the output file. We will use it to write to the file
ofstream createOutputStream(){
    ofstream frequenciesFile;
    frequenciesFile.open("serialBagOfWords.csv");

    if(!frequenciesFile.is_open()){
        cerr << "Error opening the output file" << endl;
    }
    return frequenciesFile;
}

void saveVocabularyToFile(map<string, int> &frequencies, ofstream &outputFile){
    outputFile<<"book";
    for(auto word_frequency : frequencies){
        outputFile<<", "<<word_frequency.first;
    }
    outputFile<<"\n";
}

void getBookFrequencies(const string &bookName, map<string, int> &frequencies){
    ifstream file("books/"+bookName);

    if (!file.is_open()) {
        cerr << "Error opening the book: " << bookName << endl;
        return;
    }

    // the file contains only one line, with all the words
    // separated by commas. We read them into the book string
    string book;
    getline(file, book);
    
    // we create a stringstrem, which we will use to get each word
    stringstream ss(book);
    string word;

    // this reads the line until the next comma
    while (getline(ss, word, ',')) {
        // we first eliminate the first space
        word = word.substr(1, word.size());
        // we augment the frequency of the word if it's in the vocabulary
        auto it = frequencies.find(word);
        if(it != frequencies.end()){
            ++it->second;
        }
    }

    //we close the file
    file.close();
}

void saveFrequenciesToOutputFile(ofstream &outputFile, const string &bookTitle, const map<string, int> &frequencies){
    outputFile<<bookTitle;
    for(auto word_frequency : frequencies){
        outputFile<<", "<<word_frequency.second;
    }
    outputFile<<"\n";
}

void setFrequenciesToZero(map<string, int> &frequencies){
    for(auto& word_frequency : frequencies){
        word_frequency.second = 0;
    }
}

int main(int argc, char* argv[]) {
    
    if(argc != 7){
        cerr<<"There should be exactly six book names in the command line arguments.";
        return 1;
    }

    // frequencies is a map that will store the vocabulary and
    // the frequency for each word
    // we will use it for each book. 
    map<string, int> frequencies;
    loadVocabulary(frequencies);

    ofstream frequenciesFile = createOutputStream();
    saveVocabularyToFile(frequencies, frequenciesFile);
    
    string bookTitle;
    // for each book, iterate over it to get the vocabulary
    // frequencies and store it in the file
    for(int i = 1; i < argc; ++i){
        setFrequenciesToZero(frequencies);
        bookTitle = string(argv[i]);
        getBookFrequencies(bookTitle + ".txt", frequencies);
        saveFrequenciesToOutputFile(frequenciesFile, bookTitle, frequencies);
    }
    return 0;
}
