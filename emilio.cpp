/*Se ejecuta con mpiexec -n 6 .\bolsaDePalabrasParalelo.exe

Este programa también lee y procesa 6 archivos de texto, cuenta las palabras en cada archivo y guarda el resultado en un archivo CSV llamado "bag_of_words_paralelo.csv". 
El tiempo de ejecución se mide y se guarda en un archivo CSV llamado "resultados_paralelo.csv". El programa se ejecuta 10 veces y se calcula el tiempo promedio de ejecución. 
Este código utiliza MPI para distribuir el procesamiento de los archivos de texto en varios procesos.

En este programa, se utiliza MPI para dividir el trabajo de contar las palabras en los archivos de texto entre varios procesos.

El programa se inicia con la inicialización de MPI usando MPI_Init(&argc, &argv). Luego, se obtiene el rango (ID) y el tamaño (cantidad de procesos) del comunicador global 
MPI_COMM_WORLD mediante las funciones MPI_Comm_rank(MPI_COMM_WORLD, &rank) y MPI_Comm_size(MPI_COMM_WORLD, &size).

Se comprueba si el número de procesos es igual al número de archivos que se procesarán. 
Si no es así, se muestra un mensaje de error y se aborta la ejecución con MPI_Abort(MPI_COMM_WORLD, 1).

La paralelización se implementa asignando un archivo de texto a cada proceso, donde cada proceso cuenta las palabras del archivo asignado. 
La asignación se realiza mediante la variable rank (ID del proceso) como índice del vector archivos.

Después de contar las palabras en el archivo asignado, cada proceso almacena los conteos en un vector local (local_counts). 
Para combinar los resultados de todos los procesos, se utiliza la función MPI_Gather() para reunir los conteos locales en el proceso 0.

La función MPI_Gather() toma los siguientes argumentos:
    local_counts.data(): puntero al inicio del vector local.
    vocabulary.size(): número de elementos que se enviarán desde cada proceso.
    MPI_INT: tipo de datos de los elementos que se enviarán.
    global_counts.data(): puntero al inicio del vector global (solo relevante en el proceso 0).
    vocabulary.size(): número de elementos que se recibirán en el proceso 0.
    MPI_INT: tipo de datos de los elementos que se recibirán.
    0: rango del proceso raíz (proceso 0 en este caso).
    MPI_COMM_WORLD: comunicador que se utilizará.

Una vez que se han reunido los resultados en el proceso 0, este proceso escribe los resultados en un archivo CSV (bag_of_words_paralelo.csv).
*/

#include <mpi.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <iterator>
#include <cctype>

// Función auxiliar para determinar si un carácter es válido
bool esCharValido(char c)
{
    return std::isalpha(c) || c == '\'' || c == '-' || c == '_';
}

// Función para dividir una cadena en palabras
std::vector<std::string> split(const std::string &s)
{
    std::vector<std::string> tokens;
    std::string token;
    for (const char c : s)
    {
        if (esCharValido(c))
        {
            token += c;
        }
        else if (!token.empty())
        {
            tokens.push_back(token);
            token.clear();
        }
    }
    if (!token.empty())
    {
        tokens.push_back(token);
    }
    return tokens;
}

int main(int argc, char **argv)
{
    // Inicializar MPI
    MPI_Init(&argc, &argv);

    int rank, size;
    // Obtener el rango y el tamaño del comunicador
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    const int TIMES = 10;
    double total_time = 0.0;
    for (int i = 0; i < TIMES; i++)
    {
        // Medir el tiempo de inicio
        double start_time = MPI_Wtime();

        // Lista de archivos a procesar
        std::vector<std::string> archivos;
        archivos.push_back("books/dickens_a_christmas_carol.txt");
        archivos.push_back("books/dickens_a_tale_of_two_cities.txt");
        archivos.push_back("books/dickens_oliver_twist.txt");
        archivos.push_back("books/shakespeare_hamlet.txt");
        archivos.push_back("books/shakespeare_romeo_juliet.txt");
        archivos.push_back("books/shakespeare_the_merchant_of_venice.txt");

        // Verificar que el número de procesos es igual al número de archivos
        if (size != archivos.size())
        {
            if (rank == 0)
            {
                std::cerr << "El número de procesos debe ser igual al número de archivos." << std::endl;
            }
            // Abortar si no se cumple la condición
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        // Leer el vocabulario
        std::string vocabFile = "vocabulary.csv";
        std::ifstream vocabStream(vocabFile);
        std::string line;

        std::vector<std::string> vocabulary;
        while (std::getline(vocabStream, line, ','))
        {
            vocabulary.push_back(line);
        }

        // Crear un mapa para contar las palabras
        std::map<std::string, int> wordCount;

        // Leer el archivo de texto correspondiente al rango actual
        std::ifstream fileStream("books/"+archivos[rank]);

        std::string text((std::istreambuf_iterator<char>(fileStream)), std::istreambuf_iterator<char>());
        // Convertir el texto a minúsculas

        std::transform(text.begin(), text.end(), text.begin(), ::tolower);
        // Dividir el texto en palabras
        std::vector<std::string> palabras = split(text);
        // Contar las palabras en el archivo
        for (const auto &word : palabras)
        {
            wordCount[word]++;
        }

        // Crear un vector local para almacenar el conteo de palabras del vocabulario
        std::vector<int> local_counts(vocabulary.size());
        for (size_t i = 0; i < vocabulary.size(); ++i)
        {
            local_counts[i] = wordCount[vocabulary[i]];
        }

        // Crear un vector global para almacenar el conteo de palabras de todos los procesos
        std::vector<int> global_counts(vocabulary.size() * size);
        // Recolectar los conteos locales en el proceso 0
        MPI_Gather(local_counts.data(), vocabulary.size(), MPI_INT, global_counts.data(), vocabulary.size(), MPI_INT, 0, MPI_COMM_WORLD);

        // Si el rango es 0, escribir los resultados en el archivo CSV
        if (rank == 0)
        {
            std::ofstream outputFile("bag_of_words_paralelo.csv");

            outputFile << "book,";
            for (const auto &word : vocabulary)
            {
                outputFile << word << ",";
            }
            outputFile << std::endl;

            for (size_t i = 0; i < archivos.size(); ++i)
            {
                outputFile << i << ",";
                for (size_t j = 0; j < vocabulary.size(); ++j)
                {
                    outputFile << global_counts[i * vocabulary.size() + j] << ",";
                }
                outputFile << std::endl;
            }
        }

        // Medir el tiempo de finalización y calcular el tiempo de ejecución
        double end_time = MPI_Wtime();
        double exec_time = end_time - start_time;
        total_time += exec_time;
    }

    // Calcular el tiempo promedio de ejecución
    double avg_time = total_time / TIMES;

    // Si el rango es 0, escribir el tiempo promedio en un archivo CSV
    if (rank == 0)
    {
        std::ofstream csv_file("resultados_paralelo.csv");
        if (csv_file.is_open())
        {
            csv_file << "Promedio de tiempo de ejecucion: " << avg_time << " segundos" << std::endl;
            csv_file.close();
        }
    }

    // Finalizar MPI
    MPI_Finalize();
    return 0;
}
