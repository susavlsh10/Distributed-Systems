#include "utils.h"

void createEmptyFile(const std::string& fileName) {
    std::ofstream outputFile(fileName);

    if (outputFile.is_open()) {
        outputFile.close();
        //std::cout << "Empty file created: " << fileName << std::endl;
    } else {
        std::cerr << "Failed to create the empty file: " << fileName << std::endl;
    }
}

std::vector<Post> readPostsFromFile(const std::string& filename) {
    std::ifstream inputFile(filename);

    if (!inputFile) {
        std::cerr << "File empty: " << filename << std::endl;
        return {}; // Return an empty vector if the file cannot be opened
    }

    std::vector<Post> posts;
    Post currentPost;
    std::string line;

    while (std::getline(inputFile, line)) {
        if (line.empty()) {
            // An empty line indicates the end of a post entry
            posts.push_back(currentPost);
            currentPost = {}; // Clear currentPost for the next entry
        } else if (line[0] == 'T') {
            // Extract time
            currentPost.time = line.substr(2); // Skip the 'T ' prefix
        } else if (line[0] == 'U') {
            // Extract user
            currentPost.user = line.substr(2); // Skip the 'U ' prefix
        } else if (line[0] == 'W') {
            // Extract post content
            currentPost.post += line.substr(2) + "\n"; // Skip the 'W ' prefix and add to post content
        }
    }

    inputFile.close();
    return posts;
}

void appendPostToFile(const std::string& time, const std::string& user, const std::string& post, const std::string& filename) {
    std::ofstream outputFile(filename, std::ios::app); // Open the file for appending

    if (!outputFile) {
        std::cerr << "Failed to open " << filename << " for appending" << std::endl;
        return;
    }

    // Append the formatted data to the file
    outputFile << "T " << time << "\n"; // 
    outputFile << "U " << user << "\n";
    outputFile << "W " << post << "\n";
    outputFile << "\n"; // Empty line to separate entries

    outputFile.close(); // Close the file when done
}

void appendStringToFile(const std::string& filename, const std::string& stringToAppend) {
    std::ofstream outputFile(filename, std::ios::app); // Open the file for appending

    if (!outputFile) {
        std::cerr << "Failed to open " << filename << " for appending" << std::endl;
        return;
    }

    // Append the string to the file
    outputFile << stringToAppend << "\n";

    outputFile.close(); // Close the file when done
    std::cout << "Appended to file: " << filename << std::endl;
}

bool directoryExists(const std::string& path) {
    return access(path.c_str(), F_OK) == 0;
}

// Function to list files in a directory and extract client names
std::vector<std::string> extractClientNames(const std::string& directoryPath) {
    std::vector<std::string> clientNames;
    DIR* dir;
    struct dirent* entry;

    // Open the directory
    dir = opendir(directoryPath.c_str());
    if (!dir) {
        std::cerr << "Failed to open directory: " << directoryPath << std::endl;
        return clientNames;
    }

    // Loop through the directory entries
    while ((entry = readdir(dir)) != nullptr) {
        std::string fileName = entry->d_name;

        // Check if the file name ends with ".txt"
        if (fileName.size() >= 4 && fileName.substr(fileName.size() - 4) == ".txt") {
            // Extract the client name from the file name
            size_t underscorePos = fileName.find('_');
            if (underscorePos != std::string::npos) {
                std::string clientName = fileName.substr(0, underscorePos);
                // Add the client name to the list (if not already present)
                if (std::find(clientNames.begin(), clientNames.end(), clientName) == clientNames.end()) {
                    clientNames.push_back(clientName);
                }
            }
        }
    }

    // Close the directory
    closedir(dir);

    return clientNames;
}