#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <mpi.h>
using namespace std;

void loadVocabulary(map<string, int> &frequencies){
    ifstream file("vocabulary.csv");
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

void saveVocabularyToFile(map<string, int> &frequencies, ofstream &outputFile){
    outputFile<<"book";
    for(map<string, int>::iterator word_frequency_it = frequencies.begin(); 
        word_frequency_it != frequencies.end(); ++word_frequency_it){
        outputFile<<", "<<word_frequency_it->first;
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
        map<string, int>::iterator it = frequencies.find(word);
        if(it != frequencies.end()){
            ++it->second;
        }
    }

    //we close the file
    file.close();
}

void saveFrequenciesToOutputFile(ofstream &outputFile, int vocabulary_size, int bookIndx, const string bookTitle, int* gatheredValues){
    outputFile<<bookTitle;
    for(int i = vocabulary_size*bookIndx; i < vocabulary_size*(bookIndx+1); ++i){
        outputFile<<", "<<gatheredValues[i];
    }
    outputFile<<"\n";
}

int main(int argc, char* argv[]) {
    
    if(argc != 7){
        cerr<<"There should be exactly six book names in the command line arguments.";
        return 1;
    }

    const int root = 0;
    int num_processes = 0;
    int process_id = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &num_processes);
    MPI_Comm_rank(MPI_COMM_WORLD, &process_id);
    
    // send each thread its title
    string bookTitle;
    if(process_id == 0){
        bookTitle = string(argv[1]);
        for (int i = 2; i < argc; ++i){
            MPI_Send(argv[i], strlen(argv[i]) + 1, MPI_CHAR, i - 1, 0, MPI_COMM_WORLD);
        }
    }else{
        char bookTitleCharArray[100];
        MPI_Recv(bookTitleCharArray, 100, MPI_CHAR, root, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        bookTitle = bookTitleCharArray;
    }
    
    double start, end;
    // frequencies is a map that will store the vocabulary and
    // the frequency for each word
    // we will use it for each book. 
    map<string, int> frequencies;
    loadVocabulary(frequencies);
    
    // for each book, iterate over it to get the vocabulary
    // frequencies and store it in the file

    
    start = MPI_Wtime();
    getBookFrequencies(bookTitle + ".txt", frequencies);

    end = MPI_Wtime();
    cout<<process_id<<": "<<setprecision(7)<<(end-start)<<"s\n";
    int indx = -1;
    int* frequencyValue = new int[frequencies.size()];
    for(map<string, int>::iterator word_frequency_it = frequencies.begin(); 
        word_frequency_it != frequencies.end(); ++word_frequency_it){
        frequencyValue[++indx] = word_frequency_it->second;  
    }

    

    int* gatheredValues;
    if(process_id == root) 
        gatheredValues = new int[6*frequencies.size()];
    
    
    MPI_Gather(frequencyValue, frequencies.size(), MPI_INT, 
                gatheredValues, frequencies.size(), MPI_INT, 
                root, MPI_COMM_WORLD);
            
    
    if(process_id == 0){
        ofstream frequenciesFile("parallelBagOfWords.csv");
        saveVocabularyToFile(frequencies, frequenciesFile);
        for(int i = 0; i < 6; ++i){
            saveFrequenciesToOutputFile(frequenciesFile, frequencies.size(), i, argv[i+1], gatheredValues);
        }
        frequenciesFile.close();
    }

   

    delete[] frequencyValue;
    delete[] gatheredValues;
    MPI_Finalize();
    return 0;

}
