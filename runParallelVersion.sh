#mpicxx parallel.cpp -o parallel
mpiexec -n 7 ./parallel $(python print_book_titles.py < titles.txt)