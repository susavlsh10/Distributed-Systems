#include <iostream>
#include <fstream>
#include <string>
#include <ctime>

void appendPostToFile(const std::string& time, const std::string& user, const std::string& post, const std::string& filename) {
    std::ofstream outputFile(filename, std::ios::app); // Open the file for appending

    if (!outputFile) {
        std::cerr << "Failed to open " << filename << " for appending" << std::endl;
        return;
    }

    // Append the formatted data to the file
    outputFile << "T " << time << "\n";
    outputFile << "U " << user << "\n";
    outputFile << "W " << post << "\n";
    outputFile << "\n"; // Empty line to separate entries

    outputFile.close(); // Close the file when done
}

int main() {
    std::string time = "2009-06-01 00:00:00";
    std::string user = "http://twitter.com/testuser";
    std::string post = "Post content";
    
    std::string sns_dir = "sns_storage/";
    std::string filename = sns_dir + "posts.txt";

    // Call the function to append the formatted post to the file
    appendPostToFile(time, user, post, filename);

    std::string username = "user0";
    std::string file_name = sns_dir + username + ".txt";
    std::cout << file_name << std::endl;

    return 0;
}
