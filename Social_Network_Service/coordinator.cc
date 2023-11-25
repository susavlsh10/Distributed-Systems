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
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "coordinator.grpc.pb.h"
//#include "synchronizer.grpc.pb.h"

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
using csce438::MasterInfo;
using csce438::SyncReply;
using csce438::SynchService;


struct zNode{
  int serverID;
  int serverIndex;
  int clusterID;
  std::string hostname;
  std::string port;
  std::string type;
  std::time_t last_heartbeat;
  bool missed_heartbeat;
  bool isActive();
  bool master;
  bool connected = false;
  std::shared_ptr<SynchService::Stub> synchro_stub_;

};

struct ServerCluster{
  //potentially thread safe
  std::mutex v_mutex;
  
  // clusters and server information
  int master_idx[3] = {0,0,0}; // default master index for each cluster
  std::vector<zNode*> cluster1;
  std::vector<zNode*> cluster2;
  std::vector<zNode*> cluster3;

  // follower synchronizer data structures
  // array of zNode syncronizers
  zNode synchro[3]; 

};

ServerCluster SNS_Cluster;

//func declarations
int findServer(std::vector<zNode*> v, int id);
std::time_t getTimeNow();
void checkHeartbeat();

std::unordered_map<std::string, int> serverMap;

/*
bool ServerStruct::isActive(){
    bool status = false;
    if(!missed_heartbeat){
        status = true;
    }else if(difftime(getTimeNow(),last_heartbeat) < 10){
        status = true;
    }
    return status;
}
*/


bool zNode::isActive(){
    bool status = false;
    if(!missed_heartbeat){
        status = true;
    }else if(difftime(getTimeNow(),last_heartbeat) < 10){
        status = true;
    }
    return status;
}

class CoordServiceImpl final : public CoordService::Service {

  bool updateheartbeat(std::vector<zNode*>& cluster, std::string hostname, std::string port){
    // check the cluster with the hostname, if the hostname and port matches, update the heartbeat
    bool status = false;
    for (auto node: cluster){
      if (node->hostname == hostname && node->port == port){
        node->last_heartbeat = getTimeNow();
        if (node->master){
          status = true;
        }
        break;
      }
    }
    return status;
  }

  // function which given a clusterid, updates the heartbeat using updateheartbeat
  bool updateHB(int clusterid, std::string hostname, std::string port){
    bool status = false;
    switch(clusterid){
      case 1:
        status = updateheartbeat(SNS_Cluster.cluster1, hostname, port);
        break;

      case 2:
        status = updateheartbeat(SNS_Cluster.cluster2, hostname, port);
        break;

      case 3:
        status = updateheartbeat(SNS_Cluster.cluster3, hostname, port);
        break;
    }
    return status;
  }

  // function which given a clusterid, adds the server to the cluster
  void addServer(int clusterid, zNode* node){
      // add server to cluster
      switch(clusterid){
        case 1:
          // if cluster is empty, make this server the master, else make it a slave
          if (SNS_Cluster.cluster1.empty()){
            node->master = true;
            node->serverIndex = 0;
          }
          node->serverIndex = SNS_Cluster.cluster1.size();
          node->clusterID = 1;
          SNS_Cluster.cluster1.push_back(node);
          break;

        case 2:
          if (SNS_Cluster.cluster2.empty()){
            node->master = true;
            node->serverIndex = 0;
          }
          node->serverIndex = SNS_Cluster.cluster2.size();
          node->clusterID = 2;
          SNS_Cluster.cluster2.push_back(node);
          break;
        
        case 3:
          if (SNS_Cluster.cluster3.empty()){
            node->master = true;
            node->serverIndex = 0;
          }
          node->serverIndex = SNS_Cluster.cluster3.size();
          node->clusterID = 3;
          SNS_Cluster.cluster3.push_back(node);
          break;
      }
  } 

  Status RegisterSynchronizer(ServerContext* context, const ServerInfo* serverinfo, ServerInfo* replyinfo) override {
    //std::cout<<"Got RegisterSynchronizer! "<<serverinfo->type()<<"("<<serverinfo->serverid()<<")"<<std::endl;
    log(INFO, " Synchronizer added: "+ serverinfo->hostname());
    std::string SynchroIP(serverinfo->hostname() + serverinfo->port()); 
    int clusterID =  (serverinfo->serverid()%3)+1;
    std::cout << "Synchronizer at " << serverinfo->hostname() << serverinfo->port() << " added to cluster" << clusterID << std::endl;

    SNS_Cluster.synchro[clusterID-1].hostname = serverinfo->hostname();
    SNS_Cluster.synchro[clusterID-1].port = serverinfo->port();
    SNS_Cluster.synchro[clusterID-1].serverID = serverinfo->serverid();
    SNS_Cluster.synchro[clusterID-1].type = serverinfo->type();
    SNS_Cluster.synchro[clusterID-1].last_heartbeat = getTimeNow();
    SNS_Cluster.synchro[clusterID-1].missed_heartbeat = false;
    SNS_Cluster.synchro[clusterID-1].master = false;
    SNS_Cluster.synchro[clusterID-1].connected = true;

    //create synchronizer stub
    SNS_Cluster.synchro[clusterID-1].synchro_stub_ = std::shared_ptr<SynchService::Stub>(SynchService::NewStub(grpc::CreateChannel(SynchroIP, grpc::InsecureChannelCredentials())));

    // access master froom the given cluster, and return the master info in replyinfo
    int master_idx = SNS_Cluster.master_idx[clusterID-1];
    zNode* master;

    switch (clusterID){
      case 1:
        if (SNS_Cluster.cluster1.empty()){
          replyinfo->set_type("None");
          return Status::OK;
        }
        master = SNS_Cluster.cluster1[master_idx];
        break;

      case 2:
        if (SNS_Cluster.cluster2.empty()){
          replyinfo->set_type("None");
          return Status::OK;
        }
        master = SNS_Cluster.cluster2[master_idx];
        break;

      case 3:
        if (SNS_Cluster.cluster3.empty()){
          replyinfo->set_type("None");
          return Status::OK;
        }
        master = SNS_Cluster.cluster3[master_idx];
        break;
    }
    
    replyinfo->set_hostname(master->hostname);
    replyinfo->set_port(master->port);
    replyinfo->set_serverid(master->serverID);
    replyinfo->set_type(master->type);
    replyinfo->set_master(true);
    replyinfo->set_clusterid(master->clusterID);
    //replyinfo->set_connected(true);


    //confirmation->set_status(true);
    return Status::OK;
  }

  // implement GetSynchronizer
  Status GetSynchronizer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
    //std::cout<<"Got GetSynchronizer for Cluster ID: "<<id->id()<<std::endl; 
    log(INFO, "Got GetSynchronizer for Cluster ID "+ id->id());
    int clusterID = id->id();

    serverinfo->set_hostname(SNS_Cluster.synchro[clusterID-1].hostname);
    serverinfo->set_port(SNS_Cluster.synchro[clusterID-1].port);
    serverinfo->set_connected(SNS_Cluster.synchro[clusterID-1].connected);
    return Status::OK;
  }


  Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
    //std::cout<<"Got Heartbeat! "<<serverinfo->type()<<"("<<serverinfo->serverid()<<")"<<std::endl;
    
    // If the server is not registered, add it to the database. 
    std::string ServerIP(serverinfo->hostname() + serverinfo->port()); 
    if (serverMap.find(ServerIP)!= serverMap.end()){
      // update HB
      int clusterid = serverMap[ServerIP];
      
      bool status = updateHB(clusterid, serverinfo->hostname(), serverinfo->port());
      confirmation->set_status(status);
      //std::cout << "Heartbeat received from " << serverinfo->hostname() << serverinfo->port() << std::endl;
    }
    else{
      // create node and add to cluster
      zNode* node = new zNode;
      node->hostname = serverinfo->hostname();
      node->port = serverinfo->port();
      node->serverID = serverinfo->serverid();
      node->type = serverinfo->type();
      node->last_heartbeat = getTimeNow();
      node->missed_heartbeat = false;
      node->master = false;

      int clusterID =  (serverinfo->serverid()%3)+1;
      serverMap[ServerIP] = clusterID;
      addServer(clusterID, node);
      confirmation->set_status(true);
      // if node is a master, print master added
      if (node->master){
        std::cout << "Master at " << ServerIP <<  " added to cluster " << clusterID <<std::endl;
        log(INFO, " Master added to cluster "+ std::to_string(clusterID));
      }
      else{
        std::cout << "Slave at " << ServerIP << " added to cluster " << clusterID <<std::endl;
        log(INFO, " Slave added to cluster "+ std::to_string(clusterID));
      }
    }



    return Status::OK;
  }
  
  //function returns the server information for requested client id
  //this function assumes there are always 3 clusters and has math
  //hardcoded to represent this.
  Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
    std::cout<<"Got GetServer for clientID: "<<id->id()<<std::endl; 
    log(INFO, "Got GetServer for clientID "+ id->id());
    int serverID = (id->id()%3)+1;

    // Your code here
    // If server is active, return serverinfo
    zNode* server_node;
    int index;
    switch (serverID){
      case 1:
        if (SNS_Cluster.cluster1.empty()){
          serverinfo->set_type("None");
          return Status::OK;
        }
        index = SNS_Cluster.master_idx[0];
        server_node = SNS_Cluster.cluster1[index];
        break;

      case 2:
        if (SNS_Cluster.cluster2.empty()){
          serverinfo->set_type("None");
          return Status::OK;
        }
        index = SNS_Cluster.master_idx[1];
        server_node = SNS_Cluster.cluster2[index];
        break;

      case 3:
        if (SNS_Cluster.cluster3.empty()){
          serverinfo->set_type("None");
          return Status::OK;
        }
        index = SNS_Cluster.master_idx[2];
        server_node = SNS_Cluster.cluster3[index];
        break;
    }

    if (server_node->isActive()){
      serverinfo->set_hostname(server_node->hostname);
      serverinfo->set_port(server_node->port);
      serverinfo->set_serverid(server_node->serverID);
      serverinfo->set_type(server_node->type);
      std::cout << " Client assigned to " << server_node->hostname << server_node->port << std::endl;
      log(INFO, " Client assigned to "+ server_node->hostname);
    }
    else {
      serverinfo->set_type("None"); 
    }
    return Status::OK;
  }

  // implement GetCluster 
  Status GetCluster(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
    std::cout<<"Got GetCluster from Server ID: "<<id->id()<<std::endl; 
    log(INFO, "Got GetCluster from Server ID "+ id->id());
    int serverID = (id->id()%3)+1;

    // Your code here
    // If server is active, return serverinfo
    zNode* server_node;
    int index;
    switch (serverID){
      case 1:
        if (SNS_Cluster.cluster1.empty()){
          serverinfo->set_type("None");
          return Status::OK;
        }
        index = SNS_Cluster.master_idx[0];
        server_node = SNS_Cluster.cluster1[index];
        break;

      case 2:
        if (SNS_Cluster.cluster2.empty()){
          serverinfo->set_type("None");
          return Status::OK;
        }
        index = SNS_Cluster.master_idx[1];
        server_node = SNS_Cluster.cluster2[index];
        break;

      case 3:
        if (SNS_Cluster.cluster3.empty()){
          serverinfo->set_type("None");
          return Status::OK;
        }
        index = SNS_Cluster.master_idx[2];
        server_node = SNS_Cluster.cluster3[index];
        break;
    }

    if (server_node->isActive()){
      serverinfo->set_hostname(server_node->hostname);
      serverinfo->set_port(server_node->port);
      serverinfo->set_serverid(server_node->serverID);
      serverinfo->set_type(server_node->type);
      if (id->id() == server_node->serverID){
        serverinfo->set_master(true);
      }
      else{
        serverinfo->set_master(false);
      }
    }
    else {
      serverinfo->set_type("None"); 
    }
    return Status::OK;
  }

};

void RunServer(std::string port_no){
  //start thread to check heartbeats
  std::thread hb(checkHeartbeat);
  //localhost = 127.0.0.1
  std::string server_address("127.0.0.1:"+port_no);
  CoordServiceImpl service;
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
  log(INFO, " Coordinator listening on "+ server_address);
  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  
  std::string port = "4000";
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;
          break;
      default:
	std::cerr << "Invalid Command Line Argument\n";
    }
  }
  std::string log_file_name = std::string("coordinator-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Coordinator starting...");
  RunServer(port);
  return 0;
}

bool checkCluster(zNode* cluster){
  time_t last_hb = cluster->last_heartbeat;
  if(difftime(getTimeNow(),last_hb)>10 && !cluster->missed_heartbeat)
  {
    cluster->missed_heartbeat = true;
    std::cout << "Server: " << cluster->hostname << cluster->port <<" Server ID: " <<cluster->serverID << " disconnected (last HB>10)" <<std::endl;
    log(INFO, " Server disconnected: "+ cluster->hostname);
    return false;
  }
  return true;
}

void checkClusterHB(std::vector<zNode*> cluster, int clusterID){
  bool master_alive = false;
  
  //vector of zNode pointers to alive nodes
  std::vector<zNode*> alive_node;

  for (auto node: cluster){
    bool status = checkCluster(node);
    if (node->master){
      master_alive = status;
    }
    if (status){
      alive_node.push_back(node);
    }
  }

  // if master is dead, assign new master from the alive nodes
  if (!master_alive){
    // check if there are any alive nodes
    if (alive_node.empty()){
      // if no alive nodes, set master to null
      SNS_Cluster.master_idx[clusterID] = -1;
      std::cout << "No alive nodes in cluster " << clusterID << std::endl;
    }
    else{
      std::cout << "Master at " << cluster[0]->hostname << cluster[0]->port << " for cluster " << clusterID +1<< " disconnected" << std::endl;
      // if there are alive nodes, assign new master
      int new_master_idx = alive_node[0]->serverIndex;
      SNS_Cluster.master_idx[clusterID] = new_master_idx;
      alive_node[0]->master = true;
      std::cout << "New master at " << alive_node[0]->hostname << alive_node[0]->port << " for cluster " << clusterID+1 << std::endl;
      log(INFO, " New master at "+ alive_node[0]->hostname);

      // send new master info to synchronizer
      SyncReply reply;
      grpc::ClientContext context;
      grpc::Status status;
      MasterInfo master_info;
      
      master_info.set_serverid(alive_node[0]->serverID);
      master_info.set_clusterid(clusterID+1);

      status = SNS_Cluster.synchro[clusterID].synchro_stub_->UpdateMaster(&context, master_info, &reply);
    }
  }

}

void checkHeartbeat(){
    
    while(true){
      //check cluster 1
      if (!SNS_Cluster.cluster1.empty()){
        checkClusterHB(SNS_Cluster.cluster1, 0);
      }
      //check cluster 2
      if (!SNS_Cluster.cluster2.empty()){
        checkClusterHB(SNS_Cluster.cluster2, 1);
      }
      //check cluster 3
      if (!SNS_Cluster.cluster3.empty()){
        checkClusterHB(SNS_Cluster.cluster3, 2);
      }
      sleep(3);
    }
}


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

