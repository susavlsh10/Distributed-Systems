/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"
#include <unordered_map>

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;


struct Client {
  std::string username;
  bool connected = false;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
  std::string user_file; // user.txt
  std::string user_following_file; //user_following.txt
};


struct Post {
    std::string time;
    std::string user;
    std::string post;
};

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


//Vector that stores every client that has been created
std::vector<Client> client_db;
std::unordered_map<std::string, int> clientMap;
int client_count = 0;

class SNSServiceImpl final : public SNSService::Service {
  
  std::string sns_dir = "sns_storage/";
  
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {

  auto user_name = request->username();
  list_reply->add_following(user_name); //add the current user as a follower

  for(const Client& client: client_db)
  {  
    auto user = client.username; //current client in the database
    list_reply->add_all_users(user);
    
  
    if (user == user_name){
      // copy the follower list
      for(const Client* _client: client.client_followers){
        //std::cout << "Followers exists" <<std::endl;
        list_reply->add_followers(_client->username);
      }
      // copy the following list
      for(const Client* _client: client.client_following){
        //std::cout << "Followers exists" <<std::endl;
        list_reply->add_following(_client->username);
      }
    }
  }

  return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
      
    auto user_name = request->username();
    auto follow_list = request->arguments(); std::string follow_name = follow_list[0];

    std::cout << user_name << " attempting to follow " << follow_name << std::endl;
    
    bool follow = true;
    // check if user_name is not attempting to follow user_name
    if (user_name == follow_name){ 
      reply->set_msg("Already following");
      return Status::OK;
    }
    int client_0_idx; int client_1_idx;
    if (clientMap.find(user_name) != clientMap.end() && clientMap.find(follow_name) != clientMap.end()){
      
      client_0_idx = clientMap[user_name];
      client_1_idx = clientMap[follow_name];

      // check if user_name is already following follow_name
      for (const Client* client: client_db[client_0_idx].client_following)
      {
        if (client->username == follow_name){
          follow = false;
          reply->set_msg("Already following");
          std::cout << user_name << " already follows " << client->username << std::endl;
          std::cout << "follow_name = "<< follow_name << ", client->username = " << client->username << std::endl;
          return Status::OK;
        }
      }
      if (follow){
        client_db[client_0_idx].client_following.push_back(&client_db[client_1_idx]);
        client_db[client_1_idx].client_followers.push_back(&client_db[client_0_idx]);
        reply->set_msg("Follow successful");
        std::cout << "Follow successful" <<std::endl;
      }
    }
    else{
      reply->set_msg("Invalid username");
    }

    return Status::OK; 
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
    auto user_name = request->username();
    auto unfollow_list = request->arguments(); std::string unfollow_name = unfollow_list[0];

    std::cout << user_name << " attempting to unfollow " << unfollow_name << std::endl;
    
    bool unfollow = false;
    // check if user_name is not attempting to follow user_name
    if (user_name == unfollow_name){ 
      reply->set_msg("Invalid username");
      return Status::OK;
    }
    int client_0_idx; int client_1_idx;
    if (clientMap.find(user_name) != clientMap.end() && clientMap.find(unfollow_name) != clientMap.end()){
      client_0_idx = clientMap[user_name];
      client_1_idx = clientMap[unfollow_name];

      // check if user_name is following follow_name
      int idx = 0;
      for (Client* client: client_db[client_0_idx].client_following)
      {
        if (client->username == unfollow_name){
          unfollow = true;
          break;
        }
        idx++;
      }

      int idj = 0;
      for (Client* client: client_db[client_1_idx].client_followers)
      {
        if (client->username == user_name){
          unfollow = true;
          break;
        }
        idj++;
      }

      if (unfollow){
        // remove unfollow_name from user_name's following list
        std::cout << "erasing " << unfollow_name << " from " << user_name << "follow list" << std::endl;
        client_db[client_0_idx].client_following.erase(client_db[client_0_idx].client_following.begin()+idx);
        std::cout << "erased 1. " << std::endl;

        //remove user_name from unfollow_name's followers list
        std::cout << "erasing " << user_name << " from " << unfollow_name << " follow list" << std::endl;
        client_db[client_1_idx].client_followers.erase(client_db[client_1_idx].client_followers.begin()+ idj);
        std::cout << "erased 2. " << std::endl;

        reply->set_msg("Unfollow successful");
      }
      else{
        reply->set_msg("Not a follower");
      }
    }
    else{
      reply->set_msg("Invalid username");
    }
    return Status::OK;
  }

  // RPC Login
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {

    auto user_name = request->username();
    std::cout << "Request received for user name " << user_name << " : ";

    if (clientMap.find(user_name) != clientMap.end()){
      std::cout << "User "<<user_name <<" already exists." <<std::endl;
      reply->set_msg("Username already exists.");
    }
    else{
      clientMap[user_name] = client_count;
      
      reply->set_msg("Client added to data base");
      Client new_client;
      new_client.username = user_name;
      new_client.user_file = sns_dir + user_name + ".txt";
      new_client.user_following_file = sns_dir + user_name + "_following.txt";
      //std::cout << "File names " <<  new_client.user_file << ", " << new_client.user_following_file << std::endl;

      //save the new client in the vector database
      client_db.push_back(new_client);
      std::cout << "Client " << user_name << " added to database " <<std::endl;
      
      //increment the client counter
      client_count++;
    }
    return Status::OK;
  }

  Status Timeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* stream) override {

      Message m;
      while (stream->Read(&m)){
        std::string user_name = m.username();
        int client_idx = clientMap[user_name];
        //Client* user = &client_db[client_idx];

        //read the string from the Message and format it for writing

        if (client_db[client_idx].connected == false){ // user enters timeline mode for the first time
          client_db[client_idx].stream = stream;

          //write the string to user.txt
          std::cout << "User " << user_name << " connected to the Timeline " <<std::endl;
          client_db[client_idx].connected = true;

          // read the latest 20 posts from the persistent storage
          std::vector<Post> posts = readPostsFromFile(client_db[client_idx].user_following_file);
          std::cout << "Read from file " << client_db[client_idx].user_following_file << std::endl;

          if (posts.size()){
            std::cout << "Size of posts = " << posts.size() << std::endl;
          
          // create a Message for each text and write it back to the stream
          int start_idx = 0;
          if (posts.size()> 20) {start_idx = posts.size() - 20;}
          
          for(int i = posts.size()-1; i>= start_idx; i--){
          //for(int i = start_idx; i< posts.size(); i++){
            Post post = posts[i];
            Message new_msg;
            new_msg.set_username(post.user);
            new_msg.set_msg(post.post);

            google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
            timestamp->set_seconds(std::stoll(post.time));
            timestamp->set_nanos(0);
            new_msg.set_allocated_timestamp(timestamp);

            stream->Write(new_msg);
            }

          }
        }
        else{ // if the user is making a new post 
          auto t_seconds = m.timestamp().seconds();
          appendPostToFile(std::to_string(t_seconds), user_name, m.msg(), client_db[client_idx].user_file); // add post to user.txt
          appendPostToFile(std::to_string(t_seconds), user_name, m.msg(), client_db[client_idx].user_following_file); // add post to user_following.txt 
          
          //send the same message back to the user timeline
          stream->Write(m);

          // write to all the clients who follow the current user
          for (const Client* _client:  client_db[client_idx].client_followers){
            // check if the client is connected in timeline mode
            if (_client->connected){
              std::cout << "Writing to stream with user message" <<std::endl;
              _client->stream->Write(m);
            }

            //append to formatted message _client_following.txt 
            appendPostToFile(std::to_string(t_seconds), user_name, m.msg(), _client->user_following_file);
          }
        }
      }
    
    return Status::OK;
  }

};

void RunServer(std::string port_no) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on "+server_address);

  server->Wait();
}

int main(int argc, char** argv) {

  std::string port = "3010";
  
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  
  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  RunServer(port);

  return 0;
}
