#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <csignal>
#include <grpc++/grpc++.h>
#include <sstream>
#include "client.h"

#include "sns.grpc.pb.h"
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "coordinator.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
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

std::unique_ptr<CoordService::Stub> coordinator_stub;

ServerInfo getServerAddress(std::string coordinator_address, int userid);

void sig_ignore(int sig) {
  std::cout << "Signal caught " + sig;
}

Message MakeMessage(const std::string& username, const std::string& msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}


class Client : public IClient
{
public:
  Client(const std::string& hname,
	 const std::string& uname,
	 const std::string& p)
    :hostname(hname), username(uname), port(p) {}

  
protected:
  virtual int connectTo();
  virtual IReply processCommand(std::string& input);
  virtual void processTimeline();

private:
  std::string hostname;
  std::string username;
  std::string port;
  
  // You can have an instance of the client stub
  // as a member variable.
  std::unique_ptr<SNSService::Stub> stub_;
  
  IReply Login();
  IReply List();
  IReply Follow(const std::string &username);
  IReply UnFollow(const std::string &username);
  void   Timeline(const std::string &username);
  void ReconnectServer(std::string coordinator_address, int userid);
};


///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////
int Client::connectTo()
{
  //std::string login_info = hostname + ":" + port;
  std::string login_info = hostname + port;
  auto channel = grpc::CreateChannel(login_info,
                          grpc::InsecureChannelCredentials());
  stub_ =  csce438::SNSService::NewStub(channel);
  
  //auto reply = channel->GetState(true);
  IReply ire = Login();
  if (ire.comm_status == IStatus::SUCCESS)
    return 1;
  else
    return -1;
}

IReply Client::processCommand(std::string& input)
{

  IReply ire;
  
  // create stringstream to read the commands
  std::stringstream streamcommand(input);

  std::string command, username2;

  streamcommand >> command;

  if (command == "FOLLOW"){
    streamcommand >> username2;
    ire = Follow(username2);
  } 
  else if (command == "UNFOLLOW"){
    streamcommand >> username2;
    ire = UnFollow(username2);
  } 
  else if (command == "LIST"){
    ire = List();
  }
  else if (command == "TIMELINE"){
    Timeline(username);
  }

  return ire;
}


void Client::processTimeline()
{
    Timeline(username);
}

// List Command
IReply Client::List() {

  IReply ire;
  ClientContext context;
  Request list_request;
  ListReply list_reply;
  list_request.set_username(username);
  
  grpc::Status status = stub_->List(&context, list_request, &list_reply);
  //ire.all_users = list_reply.all_users();

  // print the list of users and current following clients

  ire.grpc_status = status;
  if (status.ok()){
    ire.comm_status = IStatus::SUCCESS;

    auto all_users = list_reply.all_users();
    for (const std::string& user: all_users){
      ire.all_users.push_back(user);
    }

    auto all_followers = list_reply.followers();
    for (const std::string& user: all_followers){
      ire.followers.push_back(user);
    }

    auto all_following = list_reply.following();
    for (const std::string& user: all_following){
      ire.following.push_back(user);
    }

  }
  else{
    ire.comm_status = IStatus::FAILURE_INVALID;
  }

  return ire;
}

// Follow Command        
IReply Client::Follow(const std::string& username2) {

  IReply ire; 
  Request follow_request;
  Reply follow_reply;
  ClientContext context;

  follow_request.set_username(username);
  follow_request.add_arguments(username2);
  Status status = stub_->Follow(&context, follow_request, &follow_reply);
  ire.grpc_status = status;

  if (status.ok()){
    if (follow_reply.msg() == "Follow successful"){
      //std::cout << "Following " << username2 <<std::endl;
      ire.comm_status = IStatus::SUCCESS;
    }
    else if(follow_reply.msg() == "Already following"){
      ire.comm_status = IStatus::FAILURE_ALREADY_EXISTS;
    }
    else{
      //std::cout << follow_reply.msg() <<std::endl;
      ire.comm_status = IStatus::FAILURE_INVALID_USERNAME;
    }

  }


  return ire;
}

// UNFollow Command  
IReply Client::UnFollow(const std::string& username2) {

  IReply ire;

  Request unfollow_request;
  Reply unfollow_reply;
  ClientContext context;

  unfollow_request.set_username(username);
  unfollow_request.add_arguments(username2);
  Status status = stub_->UnFollow(&context, unfollow_request, &unfollow_reply);
  ire.grpc_status = status;

  if (status.ok()){
    if (unfollow_reply.msg() == "Unfollow successful"){
      ire.comm_status = IStatus::SUCCESS;
    }
    else if (unfollow_reply.msg() =="Not a follower"){
      ire.comm_status = IStatus::FAILURE_NOT_A_FOLLOWER;
    }
    else{
      //std::cout << unfollow_reply.msg() <<std::endl;
      ire.comm_status = IStatus::FAILURE_INVALID_USERNAME;
    }

  }

  return ire;
}

// Login Command  
IReply Client::Login() {

    IReply ire;

    Request request;
    Reply reply;
    ClientContext context;

    request.set_username(username);
    log(INFO, " Client requesting to login");
    std::cout << "Client requesting to login with name : "<< username << std::endl;
    Status status = stub_->Login(&context, request, &reply);

    ire.grpc_status = status;
    if (status.ok()){
      if (reply.msg() == "Username already exists."){
        ire.comm_status = IStatus::FAILURE_ALREADY_EXISTS;
      }
      else{
        ire.comm_status = IStatus::SUCCESS;
        //std::cout << "Response from server : "<< reply.msg() << std::endl;
        //std::cout << "Client added to server." <<std::endl;
      }
    }
    else {
      std::cout << "RPC failed. " <<std::endl;
      ire.comm_status = IStatus::FAILURE_UNKNOWN;
    }


    return ire;
}

// Timeline Command
void Client::Timeline(const std::string& username) {

  ClientContext context;
  std::shared_ptr<ClientReaderWriter<Message, Message>> stream(stub_->Timeline(&context));

  
  log(INFO, " Client attempting to enter timeline");
  std::string input_text;
  Message msg;
  std::cout << "User : " << username << " entered timeline mode." <<std::endl;
  msg = MakeMessage(username, "Start");
  if (stream->Write(msg))
  {
    std::cout << "========= Timeline mode =========" <<std::endl;
    
    std::thread writer([stream, username] (){
    std::string input_text;
    Message msg;
    while(true){
      //std::cout << "Enter text to Timeline: ";
      std::getline(std::cin, input_text);
      msg = MakeMessage(username, input_text);

      stream->Write(msg);
    }
    stream->WritesDone();
  });

    std::thread timeline_updater([stream, username] (){
    std::string input_text;
    Message msg;
    while(true){
      //std::cout << "Enter text to Timeline: ";
      //std::getline(std::cin, input_text);
      sleep(4);
      input_text = "UpdateTimeline";
      msg = MakeMessage(username, input_text);
      stream->Write(msg);
    }
    stream->WritesDone();
  });

    std::thread reader([stream, username] (){
    std::string read_text;
    Message read_msg;
    while(stream->Read(&read_msg)){
      auto time_int = read_msg.timestamp().seconds();
      //std::cout << "Incoming message :  " << std::endl;
      displayPostMessage(read_msg.username(), read_msg.msg(), time_int);
      }
    });

    writer.join();
    reader.join();
    timeline_updater.join();
  }
}

void Client::ReconnectServer(std::string coordinator_address, int userid){
  ServerInfo info = getServerAddress(coordinator_address, userid);

  //std::cout << "Reconnecting to server at : " << info.hostname() << info.port() << std::endl;
  log(INFO, " Reconnecting to server at " + info.hostname() + info.port());

}

ServerInfo getServerAddress(std::string coordinator_address, int userid){
  auto channel = grpc::CreateChannel(coordinator_address,
                          grpc::InsecureChannelCredentials());
  coordinator_stub = csce438::CoordService::NewStub(channel);
  //std::cout << " Connecting to coordinator at : " << coordinator_address << std::endl;
  log(INFO, " GetServer request to coordinator sent at " + coordinator_address);
  ID request;
  ServerInfo response;
  ClientContext context;

  request.set_id(userid);

  Status status = coordinator_stub->GetServer(&context, request, &response);

  if (status.ok()){ 
    
    if (response.type() == "None"){
      std::cout << "Server not available at coordinator" << std::endl;
      exit(1);
      }
    std::cout << "Got server from coordinator : " << response.hostname() << response.port() << std::endl;
  }
  else{
    std::cout << "Error connecting to coordinator at " << coordinator_address << std::endl;
    exit(1);
  }
  return response;
}

//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {

  std::string hostname = "localhost";
  std::string username = "default";
  std::string port = "3010";
  
  std::string coordinatorIP = "127.0.0.1:";
  std::string coordinatorPort = "4000";
    
  int opt = 0;
  while ((opt = getopt(argc, argv, "h:u:p:")) != -1){
    switch(opt) {
    case 'u':
      username = optarg;break;
    case 'p':
      port = optarg;break;
    case 'h':
      coordinatorIP = optarg;
      break;
    case 'k':
      coordinatorPort = optarg;
      break;
    default:
      std::cout << "Invalid Command Line Argument\n";
    }
  }
      
  std::cout << "Logging Initialized. Client starting with username "<< username <<std::endl;
  
  // connect to coordinator and get server address

  std::string coordinator_address(coordinatorIP+coordinatorPort);
  std::string log_file_name = std::string("client-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Client starting...");
  
  ServerInfo info = getServerAddress(coordinator_address, std::stoi(username));
  hostname = info.hostname();
  port = info.port();

  Client myc(hostname, username, port);

  
  myc.run();
  
  return 0;
}
