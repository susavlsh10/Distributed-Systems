#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <algorithm>
#include <unordered_map>

#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include "synchronizer.grpc.pb.h"

#include "utils.h"

namespace fs = std::filesystem;

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;

using csce438::SynchService;
using csce438::SyncRequest;
using csce438::SyncReply;
using csce438::timelineRequest;
using csce438::MasterInfo;

/*
using csce438::ServerList;
//using csce438::`;
using csce438::AllUsers;
using csce438::TLFL;
*/


int synchID = 1;
//std::vector<std::string> get_lines_from_file(std::string);
void run_synchronizer(std::string,std::string,std::string,int);
std::vector<std::string> get_all_users_func(int);
std::vector<std::string> get_tl_or_fl(int, int, bool);

std::string AllUser_file;
std::string LocalUser_file;
std::string ServerdirName; 

class SynchServiceImpl final : public SynchService::Service {
    
    // Implement SyncUsers
    Status SyncUsers(ServerContext* context, const SyncRequest* request, SyncReply* reply) override{
        //std::cout<<"Got SyncUsers"<<std::endl;
        auto users = request->users();

        // read data from Alluser_file and update all_users if user not in all_users
        std::vector<std::string> all_users = get_lines_from_file(AllUser_file);
        
        std::vector <std::string> new_users;
        // add new users to all_users
        for (auto user : users){
            if (std::find(all_users.begin(), all_users.end(), user) == all_users.end()){
                new_users.push_back(user);
            }
        }

        // append new users to all_users file
        for (auto user : new_users){
            appendStringToFile(AllUser_file, user);
        }

        return Status::OK; 
    }

    // Implement SyncFollowers
    Status SyncFollowers(ServerContext* context, const SyncRequest* request, SyncReply* reply) override{
        std::cout<<"Got SyncFollowers"<<std::endl;
        
        auto username = request->username();
        auto followname = request->followname();

        // read local users from LocalUser_file
        std::vector<std::string> local_users = get_lines_from_file(ServerdirName + "LocalUsers.txt");

        // if username in local_users
        //if (std::find(local_users.begin(), local_users.end(), username) == local_users.end()){
            // print all users      
            
            // find username_followers.txt file
        std::string filename = ServerdirName + followname + "_follower.txt";
        std::cout << "SyncFollowers: filename = " << filename << std::endl;

        // read data from file and update followers if user not in followers
        std::vector<std::string> followers = get_lines_from_file(filename);

        // if username not in followers
        if (std::find(followers.begin(), followers.end(), username) == followers.end()){
            // append username to followers
            appendStringToFile(filename, username);
        }

        return Status::OK; 
    }

    // Implement SyncTimeline
    Status SyncTimeline(ServerContext* context, const timelineRequest* request, SyncReply* reply) override{
        //std::cout<<"Got SyncTimeline"<<std::endl;
        
        auto username = request->username();
        auto messages = request->message();
        auto timestamps = request->timestamp();
        auto follower_name = request->follower_name();

        // get follower's timeline file
        std::string filename = ServerdirName + follower_name + ".txt";

        // add messages and timestamps to timeline file
        for (int i = 0; i < messages.size(); i++){
            appendPostToFile(timestamps[i], username, messages[i], filename);
        }
        return Status::OK; 
    }

    // Implement UpdateMaster
    Status UpdateMaster(ServerContext* context, const MasterInfo* request, SyncReply* reply) override{
        //std::cout<<"Got UpdateMaster"<<std::endl;
        
        auto serverID = request->serverid();
        auto clusterID = request->clusterid();

        // upadate serverdirName and AllUser_file
        std::stringstream directoryNameStream;
        directoryNameStream << "server_" << clusterID << "_" << serverID;
        std::string directoryName = directoryNameStream.str();
        ServerdirName = directoryName + '/';
        AllUser_file = ServerdirName + "AllUsers.txt";

        std::cout << "UpdateMaster: ServerdirName = " << ServerdirName << std::endl;
        std::cout << "UpdateMaster: AllUser_file = " << AllUser_file << std::endl;

        return Status::OK; 
    }


};

void RunServer(std::string coordIP, std::string coordPort, std::string port_no, int synchID){
  //localhost = 127.0.0.1
  std::string server_address("127.0.0.1:"+port_no);
  SynchServiceImpl service;
  //grpc::EnableDefaultHealthCheckService(true);
  //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  std::thread t1(run_synchronizer,coordIP, coordPort, port_no, synchID);
  /*
  TODO List:
    -Implement service calls
    -Set up initial single heartbeat to coordinator
    -Set up thread to run synchronizer algorithm
  */

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}



int main(int argc, char** argv) {
  
  int opt = 0;
  std::string coordIP= "127.0.0.1:";
  std::string coordPort= "4000";
  std::string port = "3029";

  while ((opt = getopt(argc, argv, "h:j:p:n:")) != -1){
    switch(opt) {
      case 'h':
          coordIP = optarg;
          break;
      case 'j':
          coordPort = optarg;
          break;
      case 'p':
          port = optarg;
          break;
      case 'n':
          synchID = std::stoi(optarg);
          break;
      default:
	         std::cerr << "Invalid Command Line Argument\n";
    }
  }

  RunServer(coordIP, coordPort, port, synchID);
  return 0;
}

void print_users(std::vector<std::string> users){
    std::cout<<"Users: ";
    for(auto user:users){
        std::cout<<user<<", ";
    }
    std::cout<<std::endl;
}

void synch_users(std::vector<std::string> users, std::vector<std::shared_ptr<SynchService::Stub>> synch_stubs){
    // call SyncUsers and pass all the users
    for(auto stub_: synch_stubs){
        // call SyncUsers and pass all the users
        SyncRequest request;
        for(auto user: users){
            request.add_users(user);
        }
        SyncReply reply;
        grpc::ClientContext context;

        Status status = stub_->SyncUsers(&context, request, &reply);
        if(status.ok()){
            //std::cout<<"Synchronizer "<<synchID<<" successfully synced users to synchronizer "<<std::endl;
        }else{
            //std::cout<<"Synchronizer "<<synchID<<" failed to sync users to synchronizer "<<std::endl;
        }
    }
}


void get_sync_servers(std::shared_ptr<csce438::CoordService::Stub> coord_stub_, std::vector<std::shared_ptr<SynchService::Stub>> synch_stubs, std::vector<int> registered_synchronizers){
    // request all synch servers from coordinator
    auto clusterId_list = getOtherIntegers(registered_synchronizers);
    //std::cout << "Size of clusterId_list: " << clusterId_list.size() << std::endl;

    // request all synch servers from coordinator
    for (auto i : clusterId_list){
        std::cout<<"Requesting synch server "<< i <<std::endl;
        ID request;
        request.set_id(i);
        ServerInfo reply;
        grpc::ClientContext context;

        Status status = coord_stub_->GetSynchronizer(&context, request, &reply);
        if(status.ok()){
            std::cout << "Received synchro "<< i << " server address: " << reply.hostname() << reply.port() << std::endl;
            if (reply.connected()){ // the other synchronizer server is online
            // create stubs for all synch servers
                std::string target_str = reply.hostname() + reply.port();
                std::shared_ptr<SynchService::Stub> synch_stub_;
                synch_stub_ = std::shared_ptr<SynchService::Stub>(SynchService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));
                //synch_stubs.push_back(std::move(synch_stub_));
                synch_stubs.push_back(synch_stub_);

                registered_synchronizers.push_back(i);
            }

        }else{
            std::cout<<"Synchronizer "<<synchID<<" failed to connect to coordinator  "<< i <<std::endl;
            break;
        }
    }
}

void sync_followers(std::vector<std::string> users, std::vector<std::shared_ptr<SynchService::Stub>> synch_stubs, std::string username, std::vector <int> registered_synchronizers){
    // call SyncFollowers and pass all the users

    // calculate which synchronizer is managing username
    int serverid = (std::stoi(username) % 3) + 1;

    // find the index of serverid in registered_synchronizers
    int index = std::find(registered_synchronizers.begin(), registered_synchronizers.end(), serverid) - registered_synchronizers.begin();

    auto stub_ = synch_stubs[index];
    
    // call SyncUsers and pass all the users
    SyncRequest request;
    for(auto user: users){
        request.add_users(user);
    }
    SyncReply reply;
    grpc::ClientContext context;

    Status status = stub_->SyncFollowers(&context, request, &reply);
    if(status.ok()){
        //std::cout<<"Synchronizer "<<synchID<<" successfully synced followers to synchronizer "<<std::endl;
    }else{
        //std::cout<<"Synchronizer "<<synchID<<" failed to sync followers to synchronizer "<<std::endl;
    }
}

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID){
    //setup coordinator stub
    //std::cout<<"synchronizer stub"<<std::endl;
    std::string target_str = coordIP + coordPort;
    std::shared_ptr<CoordService::Stub> coord_stub_;
    coord_stub_ = std::shared_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));
    //std::cout<<"MADE STUB"<<std::endl;

    ServerInfo msg;
    Confirmation c;
    ServerInfo reply;
    grpc::ClientContext context;

    msg.set_serverid(synchID);
    msg.set_hostname("127.0.0.1:");
    msg.set_port(port);
    msg.set_type("Synchronizer");

    //send init heartbeat
    Status status = coord_stub_->RegisterSynchronizer(&context, msg, &reply);
    
    //Status status = coord_stub_->Heartbeat(&context, msg, &c);
    if(status.ok()){
        std::cout<<"Synchronizer "<<synchID<<" connected to coordinator"<<std::endl;
    }else{
        std::cout<<"Synchronizer "<<synchID<<" failed to connect to coordinator"<<std::endl;
        return;
    }

    // access serverid and clusterid from reply
    int serverid = reply.serverid();
    int clusterid = reply.clusterid();

    // create directory name
    std::stringstream directoryNameStream;
    directoryNameStream << "server_" << clusterid << "_" << serverid;
    std::string directoryName = directoryNameStream.str();
    ServerdirName = directoryName + '/';

    AllUser_file = ServerdirName + "AllUsers.txt";
    LocalUser_file = ServerdirName + "LocalUsers.txt";
    
    //std::cout << "AllUser_file: " << AllUser_file << std::endl;

    // create stubs for all synch servers
    std::vector<std::shared_ptr<SynchService::Stub>> synch_stubs;
    std::vector <int> registered_synchronizers;
    
    // create a map from serverid to stub index in synch_stubs
    std::unordered_map<int, int> serverIdToIndexMap;
    int idx = 0;
    registered_synchronizers.push_back(clusterid);

    // get current timestamp
    time_t lastsync_timestamp= time_t(0);//time(NULL);
    std::string lastsync_timestamp_str = std::to_string(lastsync_timestamp);
    std::cout << "lastsync_timestamp_str: " << lastsync_timestamp_str << std::endl;

    
    while(true){
        //change this to 30 eventually
        sleep(10);

        // read data from Alluser_file and update all_users if user not in all_users
        std::vector<std::string> users = get_lines_from_file(AllUser_file);
        std::vector<std::string> local_users = get_lines_from_file(LocalUser_file);


        // if num_synch_servers is less than 3, request all synch servers from coordinator
        if(registered_synchronizers.size() < 3){
            
            auto clusterId_list = getOtherIntegers(registered_synchronizers);
            //std::cout << "Size of clusterId_list: " << clusterId_list.size() << std::endl;

            // request all synch servers from coordinator
            for (auto i : clusterId_list){
                //std::cout<<"Requesting synch server "<<i<<std::endl;
                ID request;
                request.set_id(i);
                ServerInfo reply;
                grpc::ClientContext context;

                Status status = coord_stub_->GetSynchronizer(&context, request, &reply);
                if(status.ok()){
                    std::cout << "Received synchro"<< i << "server address: " << reply.hostname() << reply.port() << std::endl;
                    if (reply.connected()){ // the other synchronizer server is online
                    // create stubs for all synch servers
                        std::string target_str = reply.hostname() + reply.port();
                        std::shared_ptr<SynchService::Stub> synch_stub_;
                        synch_stub_ = std::shared_ptr<SynchService::Stub>(SynchService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));
                        //synch_stubs.push_back(std::move(synch_stub_));
                        synch_stubs.push_back(synch_stub_);

                        registered_synchronizers.push_back(i);
                        serverIdToIndexMap[i] = idx;
                        idx++;
                    }

                }else{
                    std::cout<<"Synchronizer "<<synchID<<" failed to connect to coordinator  "<<i<<std::endl;
                    break;
                }
            }
        }
        
        // sync users
        synch_users(local_users, synch_stubs);

        // sync followers
        for (auto user : local_users){
            std::string following_file = ServerdirName + user + "_following.txt";
            //std::cout << "reading following_file: " << following_file << std::endl;
            std::vector<std::string> following = get_lines_from_file(following_file);

            //std::cout << user << " is following: "; //following << std::endl;
            for (auto f : following){
                std::cout << f << ", ";
            }
            std::cout << std::endl;

            // for each user in the following file, check if the user is in the local_users file
            std::vector<std::string> followers;
            for (auto followed_user : following){
                if (std::find(local_users.begin(), local_users.end(), followed_user) == local_users.end()){
                    followers.push_back(followed_user);
                }
            }

            for (auto follower : followers){
                int serverid = (std::stoi(follower) % 3) + 1;
                int index = serverIdToIndexMap[serverid];
                
                //std::cout << follower << " is managed by serverid: " << serverid << " at index " << index << std::endl;

                auto stub_ = synch_stubs[index];
                
                // call SyncUsers and pass all the users
                SyncRequest request;
                SyncReply reply;
                grpc::ClientContext context;
                
                request.set_username(user);
                request.set_followname(follower);

                Status status = stub_->SyncFollowers(&context, request, &reply);
            }
        }
        
        // sync timelines
        for (auto user : local_users){
            std::string timeline_file = ServerdirName + user + "_timeline.txt";
            std::vector<Post> timeline = readPostsFromFile(timeline_file);
            
            std::string follower_file = ServerdirName + user + "_follower.txt";
            std::vector<std::string> followers = get_lines_from_file(follower_file);

            // find all the posts in timeline that have timestamp value greater than lastsync_timestamp
            std::vector<Post> new_posts;
            for (auto post : timeline){
                if (std::stoi(post.time) > lastsync_timestamp){
                    new_posts.push_back(post);
                    lastsync_timestamp = std::stoi(post.time);
                }
            }

            for (auto follower : followers){
                int serverid = (std::stoi(follower) % 3) + 1;
                int index = serverIdToIndexMap[serverid];
                auto stub_ = synch_stubs[index];
                
                // call SyncTimeline and pass all the new posts
                timelineRequest request;
                SyncReply reply;
                grpc::ClientContext context;

                request.set_username(user);
                request.set_follower_name(follower);
                for (auto msg : new_posts){
                    // Remove newline character from the end of msg.post if it exists
                    if (!msg.post.empty() && msg.post.back() == '\n') {
                        msg.post.erase(msg.post.size() - 1);
                    }
                    request.add_message(msg.post);
                    request.add_timestamp(msg.time);
                }

                Status status = stub_->SyncTimeline(&context, request, &reply);                
            }
        }
        // update lastsync_timestamp
        //lastsync_timestamp = time(NULL);

    } 

    return;
}

bool file_contains_user(std::string filename, std::string user){
    std::vector<std::string> users;
    //check username is valid
    users = get_lines_from_file(filename);
    for(int i = 0; i<users.size(); i++){
      //std::cout<<"Checking if "<<user<<" = "<<users[i]<<std::endl;
      if(user == users[i]){
        //std::cout<<"found"<<std::endl;
        return true;
      }
    }
    //std::cout<<"not found"<<std::endl;
    return false;
}

std::vector<std::string> get_all_users_func(int synchID){
    //read all_users file master and client for correct serverID
    std::string master_users_file = "./master"+std::to_string(synchID)+"/all_users";
    std::string slave_users_file = "./slave"+std::to_string(synchID)+"/all_users";
    //take longest list and package into AllUsers message
    std::vector<std::string> master_user_list = get_lines_from_file(master_users_file);
    std::vector<std::string> slave_user_list = get_lines_from_file(slave_users_file);

    if(master_user_list.size() >= slave_user_list.size())
        return master_user_list;
    else
        return slave_user_list;
}

std::vector<std::string> get_tl_or_fl(int synchID, int clientID, bool tl){
    std::string master_fn = "./master"+std::to_string(synchID)+"/"+std::to_string(clientID);
    std::string slave_fn = "./slave"+std::to_string(synchID)+"/" + std::to_string(clientID);
    if(tl){
        master_fn.append("_timeline");
        slave_fn.append("_timeline");
    }else{
        master_fn.append("_follow_list");
        slave_fn.append("_follow_list");
    }

    std::vector<std::string> m = get_lines_from_file(master_fn);
    std::vector<std::string> s = get_lines_from_file(slave_fn);

    if(m.size()>=s.size()){
        return m;
    }else{
        return s;
    }

}
