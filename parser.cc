#include <cstdlib>
#include <stdexcept>
#include <iostream>

using namespace std;

void parse() {}

int main() {
    try {
	parse();
    } catch (const exception & e) {
	cerr << e.what() << "\n";
	return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
