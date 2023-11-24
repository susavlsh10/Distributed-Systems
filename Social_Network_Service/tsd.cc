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
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include <unordered_map>
#include "utils.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;

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
using csce438::CoordService;
using csce438::Confirmation;
using csce438::ID;
using csce438::ServerInfo;
using csce438::WorkerInfo;

// global variables
std::unique_ptr<CoordService::Stub> coordinator_stub = nullptr;

struct Client {
  std::string username;
  bool connected = false;
  bool online = false;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
  std::string user_file; // user.txt, store timeline here
  std::string user_following_file; //user_following.txt, the users who the current user is following is stored here
  std::string user_follower; // the users who follow the current user is stored here
  std::string user_timeline_file; // the timeline of the current user is stored here

  void create_files(){
    // Create empty files
    createEmptyFile(user_file); createEmptyFile(user_following_file); createEmptyFile(user_follower); createEmptyFile(user_timeline_file);
  };
};

class SNSServiceImpl final : public SNSService::Service {
  
  //std::string sns_dir; // = "sns_storage/";
  
  //Vector that stores every client that has been created
  public:
    std::vector<Client*> client_db;
    std::unordered_map<std::string, int> clientMap;
    int client_count = 0;
    std::string ServerdirName;
    std::string AllUsersFile;
    std::string LocalUsersFile;
    bool isMaster = false;
    bool workerConnected = false;

    // create worker and master stubs and set to null
    std::unique_ptr<SNSService::Stub> master_stub = nullptr;
    std::unique_ptr<SNSService::Stub> worker_stub = nullptr;

    SNSServiceImpl(std::string clusterID, int serverID, std::string server_address){
      // initialize SNS service
      // send grpc message to coordinator to request for master, if master is not available, then become master
    
      ClientContext context;
      ID request;
      ServerInfo s_info;
      
      request.set_id(serverID);
      
      // sleep for 1 second while coordinator_stub == nullptr
      while (coordinator_stub == nullptr){
        std::cout << "Waiting for coordinator stub to initialize" <<std::endl;
        sleep(1);
      }
      coordinator_stub->GetCluster(&context, request, &s_info);
      isMaster = s_info.master();
  
      // if we are the worker, then we need to contact the master to register gRPC service for message forwarding
      if (!isMaster){
        // update stub
        std::string masterAddress = s_info.hostname() + s_info.port();
        auto channel = grpc::CreateChannel(masterAddress,
                          grpc::InsecureChannelCredentials());
        master_stub = csce438::SNSService::NewStub(channel);
        std::cout << " We are the worker server. Master address : " << masterAddress << std::endl;

        // call RegisterWorker RPC to the master server
        ClientContext context;
        WorkerInfo w_info;
        Reply reply;
        w_info.set_server_address(server_address);
        Status status;
        status = master_stub->RegisterWorker(&context, w_info, &reply);
        if (status.ok()){
          std::cout << "Worker registered with master" <<std::endl;
        }
        else{
          std::cout << "Worker not registered with master" <<std::endl;
        }
      }
      else{
        std::cout << "We are the master server" <<std::endl;
      }

        // recover state if available
      RecoverState(clusterID, serverID);


    }

  // RPC RegisterWorker
  Status RegisterWorker(ServerContext* context, const WorkerInfo* request, Reply* reply) override {
    std::cout << "Worker registered with master" <<std::endl;
    // extract the server_address from the request
    std::string workerAddress = request->server_address();
    workerConnected = true;

    // update stub
    auto channel = grpc::CreateChannel(workerAddress,
                          grpc::InsecureChannelCredentials());
    worker_stub = csce438::SNSService::NewStub(channel);

    return Status::OK;
  }

  // RPC Sync
  Status Sync(ServerContext* context, const Request* request, Reply* reply) override {
    //std::cout << "Sync request received" <<std::endl;
    // extract type of sync request
    std::string type = request->type();
    std::string user_name = request->username();

  if (type == "Login") {
    ProcessLoginRequest(user_name, reply);
  } else if (type == "Follow") {
    ProcessFollowRequest(user_name, request->arguments(0), reply);
  } else if (type == "Unfollow") {
    ProcessUnfollowRequest(user_name, request->arguments(0), reply);
  } else if (type == "Timeline") {
    ProcessTimelineRequest(user_name, request->message(), request->timestamp());
  }

    return Status::OK;
  }
  
  // Local function
  void ProcessListRequest(const std::string& user_name, ListReply* list_reply) {
    list_reply->add_following(user_name); // Add the current user as a follower

    for (const Client* client : client_db) {
      auto user = client->username; // Current client in the database
      list_reply->add_all_users(user);

      if (user == user_name) {
        // Copy the follower list
        for (const Client* _client : client->client_followers) {
          list_reply->add_followers(_client->username);
        }
        // Copy the following list
        for (const Client* _client : client->client_following) {
          list_reply->add_following(_client->username);
        }
      }
    }
  }

  // gRPC function
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) {
    auto user_name = request->username();
    
    // Call the local function to process the list request
    ProcessListRequest(user_name, list_reply);

    return Status::OK;
  }

  // Local function
  void ProcessFollowRequest(const std::string& user_name, const std::string& follow_name, Reply* reply) {
    std::cout << user_name << " attempting to follow " << follow_name << std::endl;
    log(INFO, user_name + " attempting to follow " + follow_name);

    // Check if user_name is not attempting to follow itself
    if (user_name == follow_name) {
      reply->set_msg("Already following");
      return;
    }

    // Check if both user_name and follow_name exist in the clientMap
    if (clientMap.find(user_name) != clientMap.end() && clientMap.find(follow_name) != clientMap.end()) {
      int client_0_idx = clientMap[user_name];
      int client_1_idx = clientMap[follow_name];

      // Check if user_name is already following follow_name
      for (const Client* client : client_db[client_0_idx]->client_following) {
        if (client->username == follow_name) {
          reply->set_msg("Already following");
          std::cout << user_name << " already follows " << client->username << std::endl;
          std::cout << "follow_name = " << follow_name << ", client->username = " << client->username << std::endl;
          return;
        }
      }

      // Add follow_name to user_name's following list and user_name to follow_name's followers list
      client_db[client_0_idx]->client_following.push_back(client_db[client_1_idx]);
      client_db[client_1_idx]->client_followers.push_back(client_db[client_0_idx]);

      reply->set_msg("Follow successful");
      std::cout << "Follow successful" << std::endl;
      log(INFO, "Follow successful");

      // save the follower relationship to the persistent storage
      std::string follower_file = client_db[client_0_idx]->user_following_file;
      std::string update = follow_name;
      appendStringToFile(follower_file, update);

      if (client_db[client_1_idx]->online){ // if the user is managed by the current server
        // send a message to the client that user_name is following them
        std::string following_file = client_db[client_1_idx]->user_follower;
        std::string update = user_name;
        appendStringToFile(following_file, update);
      }
    
    } 
    else {
      reply->set_msg("Invalid username");
    }
  }

  // gRPC function
  Status Follow(ServerContext* context, const Request* request, Reply* reply) {
    auto user_name = request->username();
    auto follow_name = request->arguments(0);

    // Call the local function to process the follow request
    ProcessFollowRequest(user_name, follow_name, reply);

    if (isMaster && workerConnected){
      ClientContext context;
      Request request;
      Reply reply;
      request.set_username(user_name);
      request.set_type("Follow");
      request.add_arguments(follow_name);
      Status status = worker_stub->Sync(&context, request, &reply);
      if (status.ok()){
        //std::cout << "Login request sent to all workers" <<std::endl;
      }
      else{
        //std::cout << "Login request not sent to all workers" <<std::endl;
      }
    }

    return Status::OK;
  }

  void ProcessUnfollowRequest(const std::string& user_name, const std::string& unfollow_name, Reply* reply){
    std::cout << user_name << " attempting to unfollow " << unfollow_name << std::endl;
    log(INFO, user_name + " attempting to follow "+ unfollow_name);

    bool unfollow = false;
    // check if user_name is not attempting to follow user_name
    if (user_name == unfollow_name){ 
      reply->set_msg("Invalid username");
      return;
    }
    int client_0_idx; int client_1_idx;
    if (clientMap.find(user_name) != clientMap.end() && clientMap.find(unfollow_name) != clientMap.end()){
      client_0_idx = clientMap[user_name];
      client_1_idx = clientMap[unfollow_name];

      // check if user_name is following follow_name
      int idx = 0;
      for (Client* client: client_db[client_0_idx]->client_following)
      {
        if (client->username == unfollow_name){
          unfollow = true;
          break;
        }
        idx++;
      }

      int idj = 0;
      for (Client* client: client_db[client_1_idx]->client_followers)
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
        client_db[client_0_idx]->client_following.erase(client_db[client_0_idx]->client_following.begin()+idx);

        //remove user_name from unfollow_name's followers list
        std::cout << "erasing " << user_name << " from " << unfollow_name << " follow list" << std::endl;
        client_db[client_1_idx]->client_followers.erase(client_db[client_1_idx]->client_followers.begin()+ idj);

        reply->set_msg("Unfollow successful");

        // save the follower relationship to the persistent storage
        //std::string follower_file = client_db[client_0_idx]->user_follower;
        //std::string update = "U " + unfollow_name;
        //appendStringToFile(follower_file, update);
      }
      else{
        reply->set_msg("Not a follower");
      }
    }
    else{
      reply->set_msg("Invalid username");
    }
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
    auto user_name = request->username();
    auto unfollow_list = request->arguments(); std::string unfollow_name = unfollow_list[0];

    ProcessUnfollowRequest(user_name, unfollow_name, reply);
    return Status::OK;
  }

  // Local function
  void ProcessLoginRequest(const std::string& user_name, Reply* reply) {
    std::cout << "Request received for user name " << user_name << " : ";
    log(INFO, " Login request received for " + user_name);

    if (clientMap.find(user_name) != clientMap.end()) {
      int client_idx = clientMap[user_name];
      /*
      if (client_db[client_idx]->online) {
        reply->set_msg("Username already exists.");
      }       
      */
      //else {
        std::cout << "Welcome back " << user_name << std::endl;
        log(INFO, "Welcome back ");
        reply->set_msg("Welcome back");
        client_db[client_idx]->online = true;
        client_db[client_idx]->username = user_name;
        client_db[client_idx]->user_file = ServerdirName + user_name + ".txt";
        client_db[client_idx]->user_following_file = ServerdirName + user_name + "_following.txt";
        client_db[client_idx]->user_follower = ServerdirName + user_name + "_follower.txt";
        client_db[client_idx]->user_timeline_file = ServerdirName + user_name + "_timeline.txt";
        client_db[client_idx]->connected = false;

      //}
    } else { // new user 
      clientMap[user_name] = client_count;

      reply->set_msg("Client added to database");
      Client* new_client = new Client;
      new_client->username = user_name;
      new_client->user_file = ServerdirName + user_name + ".txt";
      new_client->user_following_file = ServerdirName + user_name + "_following.txt";
      new_client->user_follower = ServerdirName + user_name + "_follower.txt";
      new_client->user_timeline_file = ServerdirName + user_name + "_timeline.txt";
      new_client->online = true;
      new_client->connected = false;
      new_client->create_files();

      client_db.push_back(new_client);
      std::cout << "Client " << user_name << " added to database " << std::endl;
      log(INFO, user_name + " added to database ");

      client_count++;

      // add the user to the AllUsers.txt file
      std::string update = user_name;
      appendStringToFile(AllUsersFile, update);
      appendStringToFile(LocalUsersFile, update);
    }
  }

  // gRPC function
  Status Login(ServerContext* context, const Request* request, Reply* reply) {
    auto user_name = request->username();

    // Call the local function to process the login request
    ProcessLoginRequest(user_name, reply);

    //if we are the master, then we need to send the login request to all the workers
    if (isMaster && workerConnected){
      ClientContext context;
      Request request;
      Reply reply;
      request.set_username(user_name);
      request.set_type("Login");
      Status status = worker_stub->Sync(&context, request, &reply);
      if (status.ok()){
        //std::cout << "Login request sent to all workers" <<std::endl;
      }
      else{
        //std::cout << "Login request not sent to all workers" <<std::endl;
      }
    }

    return Status::OK;
  }

  void ProcessTimelineRequest(const std::string username, const std::string msg, const std::string timestamp){
    // find username in client_db
    int client_idx = clientMap[username];
    Client* user = client_db[client_idx];

    // append to user.txt
    //std::cout << "Syncing timefile for user " << username << " message = "<< msg << std::endl;
    appendPostToFile(timestamp, username, msg, user->user_file);
    appendPostToFile(timestamp, username, msg, user->user_timeline_file);

    for (const Client* _client:  user->client_followers){
      //append to formatted message _client.txt 
      // Check if the client is online
      if (_client->online){
        //std::cout << "Writing to stream with user message" <<std::endl;
        //_client->stream->Write(m);
        appendPostToFile(timestamp, username, msg, _client->user_file); 
      }
         
    }
  }

  void updateTimeline(ServerReaderWriter<Message, Message>* stream, std::string user_name, std::vector<std::string>& timestamps){
    std::string user_file = ServerdirName + user_name + ".txt";
    std::vector<Post> posts = readPostsFromFile(user_file);
    //std::cout << "Updating timeline for user : " << user_name << std::endl;

    // if post in posts not in timestamps, then write to stream
    for (Post post: posts){
      // check if post.time is in timestamps
      if (std::find(timestamps.begin(), timestamps.end(), post.time) == timestamps.end()) {
        //std::cout << "Post time already in timestamps" <<std::endl;
        Message new_msg;
        new_msg.set_username(post.user);
        new_msg.set_msg(post.post);
        //std::cout << "Message : " << post.post << std::endl;

        google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
        timestamp->set_seconds(std::stoll(post.time));
        timestamp->set_nanos(0);
        new_msg.set_allocated_timestamp(timestamp);

        stream->Write(new_msg);
        timestamps.push_back(post.time);
      }
    }
  }

  Status Timeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* stream) override {
    Message m;
    auto local_timestamp = time(NULL);  
    std::vector<std::string> timestamps_recorded;
 
      // start a thread which periodically wakes up and reads from the file client.txt and writes to the stream

      while (stream->Read(&m)){
        std::string user_name = m.username();
        int client_idx = clientMap[user_name];

        if (m.msg() == "UpdateTimeline"){
          updateTimeline(stream, user_name, timestamps_recorded);
        }
        else{
        if (client_db[client_idx]->connected == false){ // user enters timeline mode for the first time
            client_db[client_idx]->connected = true;
            client_db[client_idx]->stream = stream;

            //read the string from the Message and format it for writing

            //write the string to user.txt
            std::cout << "User " << user_name << " connected to the Timeline " <<std::endl;
            log(INFO, user_name + " connected to timeline ");

            // read the latest 20 posts from the persistent storage
            std::vector<Post> posts = readPostsFromFile(client_db[client_idx]->user_file);
            std::cout << "Read from file " << client_db[client_idx]->user_file << std::endl;

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
              //std::cout << "Message : " << post.post << std::endl;

              google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
              timestamp->set_seconds(std::stoll(post.time));
              timestamp->set_nanos(0);
              new_msg.set_allocated_timestamp(timestamp);

              timestamps_recorded.push_back(post.time);

              stream->Write(new_msg);
              }
            }
            //std::thread tThread(&SNSServiceImpl::timelineThread, this, stream, user_name, local_timestamp);
            //tThread.detach();
            
        }
        else{ // if the user is making a new post 
          auto t_seconds = m.timestamp().seconds();
          appendPostToFile(std::to_string(t_seconds), user_name, m.msg(), client_db[client_idx]->user_file); // add post to user.txt
          appendPostToFile(std::to_string(t_seconds), user_name, m.msg(), client_db[client_idx]->user_timeline_file); // add post to user_timeline.txt
          
          //send the same message back to the user timeline
          //stream->Write(m);

          // write to all the clients who follow the current user
          for (const Client* _client:  client_db[client_idx]->client_followers){
            // check if the client is connected in timeline mode
            appendPostToFile(std::to_string(t_seconds), user_name, m.msg(), _client->user_file);
            if (_client->connected){
              //std::cout << "Writing to stream with user message" <<std::endl;
              //_client->stream->Write(m);
            }

            // if we are the master, forward the timeline request to workers if they are connected
            if (isMaster && workerConnected){
              ClientContext context;
              Request request;
              Reply reply;
              request.set_username(user_name);
              request.set_type("Timeline");
              request.set_message(m.msg());
              request.set_timestamp(std::to_string(t_seconds));

              Status status = worker_stub->Sync(&context, request, &reply);
            }  
          }
        }
        }

      }
    
    return Status::OK;
  }
  
  public:
    void RecoverState(std::string clusterID, int serverID){
      
      // create directory name
      std::stringstream directoryNameStream;
      int clusterID_int = (serverID%3)+1;

      directoryNameStream << "server_" << clusterID_int << "_" << serverID;
      std::string directoryName = directoryNameStream.str();
      ServerdirName = directoryName + '/';
      AllUsersFile = ServerdirName + "AllUsers.txt";
      LocalUsersFile = ServerdirName + "LocalUsers.txt";

      // check if recovery file exists
      if (directoryExists(directoryName)) 
      {
        //std::cout << "Server directory exists, recovering state from : " << directoryName << std::endl;

        // recover data from recovery files
        std::vector<std::string> clientNames = extractClientNames(directoryName);
        // Print the extracted client names
        std::cout << "Client Names:" << std::endl;
        for (const std::string& clientName : clientNames) {
            std::cout << clientName << std::endl;
            
            // create all client objects
            Client* new_client = new Client;
            new_client->username = clientName;
            new_client->user_file = ServerdirName + clientName + ".txt";
            new_client->user_following_file = ServerdirName + clientName + "_following.txt";
            new_client->user_follower = ServerdirName + clientName + "_follower.txt";
            new_client->user_timeline_file = ServerdirName + clientName + "_timeline.txt";
            new_client->online = false;

            // add the client to the database
            clientMap[clientName] = client_count;
            client_db.push_back(new_client);
            
            //increment the client counter
            client_count++;
        }
        // update follower relationships

        std::cout << "Server state recovered." <<std::endl;
        log(INFO, " Server state recovered");

      }

      else // if it does not exist create the directory and return
      {
        //std::cout << "Server directory does not exist." << std::endl;

        if (mkdir(directoryName.c_str(), 0777) == 0) {
          std::cout << "Directory created: " << directoryName << std::endl;
        } else {
          std::cerr << "Failed to create the directory." << std::endl;
        }

        createEmptyFile(AllUsersFile);
        createEmptyFile(LocalUsersFile);
      }

      // create all clients object and update datastructures
    }

    void SyncDataBase(){
      
      std::cout << "Sync Thread started" <<std::endl;
      while (true){
      // sleep for 10s 
      sleep(10);
      //std::cout << "Syncing database" <<std::endl;
      // read the AllUsers.txt file and update the clientMap
      auto total_users = get_lines_from_file(AllUsersFile);

      //std::cout << "Total users in" <<  AllUsersFile << " = " << total_users.size() << std::endl;

      // sync users
      for (auto user: total_users){
        // if users in total_users not in clinet_db then create new client and add to client_db
        if (clientMap.find(user) == clientMap.end()){
          // create all client objects
          Client* new_client = new Client;
          new_client->username = user;
          new_client->online = false;

          // add the client to the database
          clientMap[user] = client_count;
          client_db.push_back(new_client);
          
          //increment the client counter
          client_count++;

          // if we are the master, forward the login request to workers if they are connected
          if (isMaster && workerConnected){
            ClientContext context;
            Request request;
            Reply reply;
            request.set_username(user);
            request.set_type("Login");
            Status status = worker_stub->Sync(&context, request, &reply);
            if (status.ok()){
              //std::cout << "Login request sent to all workers" <<std::endl;
            }
            else{
              //std::cout << "Login request not sent to all workers" <<std::endl;
            }
          }
        }
      }

      // sync follower relationships
      auto local_users = get_lines_from_file(LocalUsersFile);
      for(auto local: local_users){
        // find local in client_db
        int client_idx = clientMap[local];
        Client* user = client_db[client_idx];

        // read the user_follower.txt file
        auto followers = get_lines_from_file(user->user_follower);

        // for all the followers, if the follower is not in user->client_followers, then add to user->client_followers
        // Check if user_name is already following follow_name
        for (auto follower: followers){
          bool follower_exists = false;
          for (const Client* client : user->client_followers) {
            if (client->username == follower) {
              follower_exists = true;
              break;
            }
          }

          if(!follower_exists){
            // find follower in client_db
            int follower_idx = clientMap[follower];
            Client* follower_client = client_db[follower_idx];

            // add follower_client to user->client_followers
            user->client_followers.push_back(follower_client);
          
            // if we are the master, forward the follow request to workers if they are connected
            if (isMaster && workerConnected){
              ClientContext context;
              Request request;
              Reply reply;
              request.set_username(user->username);
              request.set_type("Follow");
              request.add_arguments(follower);
              Status status = worker_stub->Sync(&context, request, &reply);
              if (status.ok()){
                //std::cout << "Login request sent to all workers" <<std::endl;
              }
              else{
                //std::cout << "Login request not sent to all workers" <<std::endl;
              }
            }
          }
        }
      }

      // Sync timeline

      }
    }

};

void HeartBeat(std::string coordinator_address, int serverID, std::string hostname, std::string port, std::string type){

  // update stub
  auto channel = grpc::CreateChannel(coordinator_address,
                          grpc::InsecureChannelCredentials());
  coordinator_stub = csce438::CoordService::NewStub(channel);
  //std::cout << " Coordinator address : " << coordinator_address << std::endl;

  while(true){
    ClientContext context;
    ServerInfo s_info;
    Confirmation confirm;
    s_info.set_serverid(serverID);
    s_info.set_hostname(hostname);
    s_info.set_port(port);
    s_info.set_type(type);
  
    Status status;
    status = coordinator_stub->Heartbeat(&context, s_info, &confirm);
    if (status.ok()){
      //std::cout << "Sent KA to coordinator " <<std::endl;
      // if confirm is true, then the server is the master
      if (confirm.status()){
        //std::cout << "Server is the master" <<std::endl;
        
      }
    }
    else{
      std::cout << "KA not sent." <<std::endl;
    }
    // send heartbeat or KA messages to the coordinator
    sleep(5);
  }
}

void RunServer(std::string port_no, std::string clusterID, int serverID) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSServiceImpl service(clusterID ,serverID, server_address);
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on "+server_address);

  // start a thread to run SyncDataBase in service 

  std::thread SyncThread(&SNSServiceImpl::SyncDataBase, &service);
  SyncThread.detach();
  
  log(INFO, "Server starting...");
  server->Wait();
}

int main(int argc, char** argv) {

  std::string port = "3010";
  std::string clusterID = "1";
  int serverID = 1;
  std::string coordinatorIP = "127.0.0.1:";
  std::string coordinatorPort = "4000";

  int opt = 0;
  while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1) {
    switch (opt) {
        case 'c':
            clusterID = optarg;
            break;
        case 's':
            serverID = std::atoi(optarg);
            break;
        case 'h':
            coordinatorIP = optarg;
            break;
        case 'k':
            coordinatorPort = optarg;
            break;
        case 'p':
            port = optarg;
            break;
        case '?':
            std::cerr << "Invalid Command Line Argument\n";
            return 1;
    }
  }
  
  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  
  std::string coordinator_address(coordinatorIP+coordinatorPort);
  std::string hostname = "0.0.0.0:";
  std::string type = "Server";

  //Start Keep Alive Thread and connect with coordinator
  std::thread KeepAlive(HeartBeat, coordinator_address, serverID, hostname , port, type);
  
  // Start seving clients
  RunServer(port, clusterID, serverID);

  return 0;
}
