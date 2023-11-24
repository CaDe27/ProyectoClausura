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

    const int master = 0;
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
        MPI_Recv(bookTitleCharArray, 100, MPI_CHAR, master, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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
    getBookFrequencies(bookTitle + ".txt", frequencies);

    
    if(process_id == master)
        start = MPI_Wtime();
    MPI_File mpi_file;
    MPI_File_open(MPI_COMM_WORLD, "parallel_output.csv",
                  MPI_MODE_CREATE | MPI_MODE_WRONLY, 
                  MPI_INFO_NULL, &mpi_file);
    if(process_id == master){
        end = MPI_Wtime();
        cout<<"dddnum_processes "<<num_processes<<": "<<setprecision(7)<<(end-start)<<"s\n";
    }
    
    stringstream ss;
    ss << setw(3) << process_id;
    for(map<string, int>::iterator word_frequency_it = frequencies.begin(); 
        word_frequency_it != frequencies.end(); ++word_frequency_it){
        ss << ", " << setw(3) << word_frequency_it->second; 
    }
    ss << "\n";
    string line = ss.str();

    MPI_Offset offset = (process_id+1) * line.size();
    MPI_File_write_at(mpi_file, offset, line.c_str(), line.size(), MPI_CHAR, MPI_STATUS_IGNORE);
    MPI_File_close(&mpi_file);

    
   
    MPI_Finalize();
    return 0;

}
