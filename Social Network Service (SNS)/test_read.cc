#include <iostream>
#include <fstream>
#include <string>
#include <vector>

struct Post {
    std::string time;
    std::string user;
    std::string post;
};

std::vector<Post> readPostsFromFile(const std::string& filename) {
    std::ifstream inputFile(filename);

    if (!inputFile) {
        std::cerr << "Failed to open " << filename << std::endl;
        return {}; // Return an empty vector if the file cannot be opened
    }

    std::vector<Post> posts;
    Post currentPost;
    std::string line;

    while (std::getline(inputFile, line)) {
        if (line.empty()) {
            // An empty line indicates the end of a post entry
            // Add the currentPost to the vector
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

int main() {
    std::string filename = "posts.txt";

    // Call the function to read and extract posts from the file
    std::vector<Post> posts = readPostsFromFile(filename);

    // Display the extracted posts
    for (const Post& post : posts) {
        std::cout << "Time: " << post.time << std::endl;
        std::cout << "User: " << post.user << std::endl;
        std::cout << "Post: " << post.post << std::endl;
    }

    return 0;
}
