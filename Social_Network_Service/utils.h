#ifndef UTILS_H
#define UTILS_H

#include <ctime>
#include <cstdlib>
#include <sys/stat.h>
#include <sys/types.h>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <thread>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <unordered_map>
#include <dirent.h> // directory operations
#include <set>


struct Post {
    std::string time;
    std::string user;
    std::string post;
};

void createEmptyFile(const std::string& fileName);

std::vector<Post> readPostsFromFile(const std::string& filename);

void appendPostToFile(const std::string& time, const std::string& user, const std::string& post, const std::string& filename);

void appendStringToFile(const std::string& filename, const std::string& stringToAppend);

bool directoryExists(const std::string& directoryName);

std::vector<std::string> extractClientNames(const std::string& directoryPath);

std::vector<int> getOtherIntegers(const std::vector<int>& nums);

std::vector<std::string> get_lines_from_file(std::string filename);

#endif // UTILS_H