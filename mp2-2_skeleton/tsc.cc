#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <csignal>
#include <algorithm>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include "client.h"

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity);
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
using csce438::ServerInfo;
using csce438::ID;

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
  IReply Timeline(const std::string &username);
};


///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////
int Client::connectTo()
{
  // ------------------------------------------------------------
  // In this function, you are supposed to create a stub so that
  // you call service methods in the processCommand/porcessTimeline
  // functions. That is, the stub should be accessible when you want
  // to call any service methods in those functions.
  // Please refer to gRpc tutorial how to create a stub.
  // ------------------------------------------------------------

  // Contact coordinator to get assigned server
  std::string coord_address = hostname + ":" + port;
  log(INFO, "Contacting coordinator at: " + coord_address);

  std::shared_ptr<Channel> coord_channel = grpc::CreateChannel(coord_address, grpc::InsecureChannelCredentials());
  std::unique_ptr<CoordService::Stub> coord_stub = CoordService::NewStub(coord_channel);

  // Call GetServer RPC with user ID
  ID client_id;
  client_id.set_id(std::stoi(username)); // username is numeric user ID

  ServerInfo server_info;
  ClientContext context;

  Status status = coord_stub->GetServer(&context, client_id, &server_info);

  if (!status.ok()) {
    log(ERROR, "Failed to get server from coordinator: " + status.error_message());
    return -1;
  }

  log(INFO, "Coordinator assigned server: " + server_info.hostname() + ":" + server_info.port() +
            " (Cluster " + std::to_string((std::stoi(username) - 1) % 3 + 1) +
            ", Server " + std::to_string(server_info.serverid()) + ")");

  // Connect to the assigned SNS server
  std::string server_address = server_info.hostname() + ":" + server_info.port();
  log(INFO, "Connecting to SNS server at: " + server_address);

  std::shared_ptr<Channel> server_channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  stub_ = SNSService::NewStub(server_channel);
  log(INFO, "SNS server stub created successfully");

  // Test connection by calling Login
  IReply login_reply = Login();

  if (login_reply.comm_status != SUCCESS) {
    log(WARNING, "Connection or Login failed for user: " + username);
    return -1;  // Connection / Login failed
  }

  log(INFO, "Successfully connected and logged in user: " + username);
  return 1;  // Success
}

IReply Client::processCommand(std::string& input)
{
  // ------------------------------------------------------------
  // GUIDE 1:
  // In this function, you are supposed to parse the given input
  // command and create your own message so that you call an 
  // appropriate service method. The input command will be one
  // of the followings:
  //
  // FOLLOW <username>
  // UNFOLLOW <username>
  // LIST
  // TIMELINE
  // ------------------------------------------------------------
  
  // ------------------------------------------------------------
  // GUIDE 2:
  // Then, you should create a variable of IReply structure
  // provided by the client.h and initialize it according to
  // the result. Finally you can finish this function by returning
  // the IReply.
  // ------------------------------------------------------------
  
  
  // ------------------------------------------------------------
  // HINT: How to set the IReply?
  // Suppose you have "FOLLOW" service method for FOLLOW command,
  // IReply can be set as follow:
  // 
  //     // some codes for creating/initializing parameters for
  //     // service method
  //     IReply ire;
  //     grpc::Status status = stub_->FOLLOW(&context, /* some parameters */);
  //     ire.grpc_status = status;
  //     if (status.ok()) {
  //         ire.comm_status = SUCCESS;
  //     } else {
  //         ire.comm_status = FAILURE_NOT_EXISTS;
  //     }
  //      
  //      return ire;
  // 
  // IMPORTANT: 
  // For the command "LIST", you should set both "all_users" and 
  // "following_users" member variable of IReply.
  // ------------------------------------------------------------

    IReply ire;
    
    // Parse the command
    std::size_t index = input.find_first_of(" ");
    std::string cmd = input.substr(0, index);
    
    if (cmd == "LIST") {
        return List();
    } else if (cmd == "FOLLOW") {
        if (index != std::string::npos) {
            std::string target_user = input.substr(index + 1);
            return Follow(target_user);
        } else {
            ire.grpc_status = Status::OK;
            ire.comm_status = FAILURE_INVALID;
            return ire;
        }
    } else if (cmd == "UNFOLLOW") {
        if (index != std::string::npos) {
            std::string target_user = input.substr(index + 1);
            return UnFollow(target_user);
        } else {
            ire.grpc_status = Status::OK;
            ire.comm_status = FAILURE_INVALID;
            return ire;
        }
    } else if (cmd == "TIMELINE") {
        // Timeline command - check connection and enter timeline mode
        return Timeline(username);
    } else {
        // Invalid command
        ire.grpc_status = Status::OK;
        ire.comm_status = FAILURE_INVALID;
        return ire;
    }

    return ire;
}


void Client::processTimeline()
{
    // Timeline is now called directly from processCommand
    // This function is kept for compatibility but does nothing
}

// List Command
IReply Client::List() {
    IReply ire;
    log(INFO, "Requesting user list for: " + username);
    
    // Create request
    Request request;
    request.set_username(username);
    
    // Create reply
    ListReply list_reply;
    
    // Create context
    ClientContext context;
    
    // Call the List RPC
    Status status = stub_->List(&context, request, &list_reply);
    
    // Set grpc status
    ire.grpc_status = status;
    
    if (status.ok()) {
        ire.comm_status = SUCCESS;
        
        // Copy all_users from list_reply to ire
        for (int i = 0; i < list_reply.all_users_size(); i++) {
            ire.all_users.push_back(list_reply.all_users(i));
        }
        
        // Copy followers from list_reply to ire
        for (int i = 0; i < list_reply.followers_size(); i++) {
            ire.followers.push_back(list_reply.followers(i));
        }
        
        log(INFO, "List request successful for " + username + 
                  ", received " + std::to_string(ire.all_users.size()) + " users and " + 
                  std::to_string(ire.followers.size()) + " followers");
    } else {
        ire.comm_status = FAILURE_UNKNOWN;
        log(WARNING, "Command failed\n");
    }
    
    return ire;
}

// Follow Command        
IReply Client::Follow(const std::string& username2) {
    IReply ire;
    log(INFO, "User " + username + " requesting to follow: " + username2);
    
    // Create request
    Request request;
    request.set_username(username);
    request.add_arguments(username2);  // Target user to follow
    
    // Create reply
    Reply reply;
    
    // Create context
    ClientContext context;
    
    // Call the Follow RPC
    Status status = stub_->Follow(&context, request, &reply);
    
    // Set grpc status
    ire.grpc_status = status;
    
    // Parse server response and set communication status
    if (status.ok()) {
        std::string msg = reply.msg();
        if (msg == "SUCCESS") {
            ire.comm_status = SUCCESS;
            log(INFO, "Follow request successful: " + username + " now follows " + username2);
        } else if (msg == "FAILURE_ALREADY_EXISTS") {
            ire.comm_status = FAILURE_ALREADY_EXISTS;
            log(WARNING, "Follow request failed: " + username + " already follows " + username2);
        } else if (msg == "FAILURE_NOT_EXISTS") {
            ire.comm_status = FAILURE_NOT_EXISTS;
            log(WARNING, "Follow request failed: user " + username2 + " does not exist");
        } else if (msg == "FAILURE_INVALID_USERNAME") {
            ire.comm_status = FAILURE_INVALID_USERNAME;
            log(WARNING, "Follow request failed: invalid username " + username2);
        } else {
            ire.comm_status = FAILURE_UNKNOWN;
            log(ERROR, "Follow request failed: unknown error");
        }
    } else {
        ire.comm_status = FAILURE_UNKNOWN;
        log(ERROR, "Follow request failed: gRPC error - " + status.error_message());
    }
    
    return ire;
}

// UNFollow Command  
IReply Client::UnFollow(const std::string& username2) {
    IReply ire;
    log(INFO, "User " + username + " requesting to unfollow: " + username2);
    
    // Create request
    Request request;
    request.set_username(username);
    request.add_arguments(username2);  // Target user to unfollow
    
    // Create reply
    Reply reply;
    
    // Create context
    ClientContext context;
    
    // Call the UnFollow RPC
    Status status = stub_->UnFollow(&context, request, &reply);
    
    // Set grpc status
    ire.grpc_status = status;
    
    // Parse server response and set communication status
    if (status.ok()) {
        std::string msg = reply.msg();
        if (msg == "SUCCESS") {
            ire.comm_status = SUCCESS;
            log(INFO, "UnFollow request successful: " + username + " no longer follows " + username2);
        } else if (msg == "FAILURE_NOT_EXISTS") {
            ire.comm_status = FAILURE_NOT_EXISTS;
            log(WARNING, "UnFollow request failed: user " + username2 + " does not exist");
        } else if (msg == "FAILURE_NOT_A_FOLLOWER") {
            ire.comm_status = FAILURE_NOT_A_FOLLOWER;
            log(WARNING, "UnFollow request failed: " + username + " was not following " + username2);
        } else if (msg == "FAILURE_INVALID_USERNAME") {
            ire.comm_status = FAILURE_INVALID_USERNAME;
            log(WARNING, "UnFollow request failed: invalid username " + username2);
        } else {
            ire.comm_status = FAILURE_UNKNOWN;
            log(ERROR, "UnFollow request failed: unknown error");
        }
    } else {
        ire.comm_status = FAILURE_UNKNOWN;
        log(ERROR, "UnFollow request failed: gRPC error - " + status.error_message());
    }
    
    return ire;
}

// Login Command  
IReply Client::Login() {
    IReply ire;
    log(INFO, "Attempting login for user: " + username);
    
    // Create request
    Request request;
    request.set_username(username);
    
    // Create reply
    Reply reply;
    
    // Create context
    ClientContext context;
    
    // Call the Login RPC
    Status status = stub_->Login(&context, request, &reply);
    
    // Set grpc status
    ire.grpc_status = status;
    
    // Parse server response and set communication status
    if (status.ok()) {
        std::string msg = reply.msg();
        if (msg == "SUCCESS") {
            ire.comm_status = SUCCESS;
            log(INFO, "Login successful for user: " + username);
        } else if (msg == "FAILURE_ALREADY_EXISTS") {
            ire.comm_status = FAILURE_ALREADY_EXISTS;
            log(WARNING, "Login failed: user " + username + " already exists");
        } else if (msg == "FAILURE_INVALID_USERNAME") {
            ire.comm_status = FAILURE_INVALID_USERNAME;
            log(WARNING, "Login failed: invalid username " + username);
        } else {
            ire.comm_status = FAILURE_UNKNOWN;
            log(ERROR, "Login failed: unknown error for user " + username);
        }
    } else {
        ire.comm_status = FAILURE_UNKNOWN;
        log(WARNING, "Login failed: gRPC error for user " + username + " - " + status.error_message());
    }
    
    return ire;
}

// Timeline Command
IReply Client::Timeline(const std::string& username) {
    IReply ire;
    log(INFO, "Entering timeline mode for user: " + username);

    // ------------------------------------------------------------
    // In this function, you are supposed to get into timeline mode.
    // You may need to call a service method to communicate with
    // the server. Use getPostMessage/displayPostMessage functions
    // in client.cc file for both getting and displaying messages
    // in timeline mode.
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // IMPORTANT NOTICE:
    //
    // Once a user enter to timeline mode , there is no way
    // to command mode. You don't have to worry about this situation,
    // and you can terminate the client program by pressing
    // CTRL-C (SIGINT)
    // ------------------------------------------------------------

    // Create context
    ClientContext context;

    // Start bidirectional streaming
    std::shared_ptr<ClientReaderWriter<Message, Message>> stream(
        stub_->Timeline(&context));

    // Send initial message to identify ourselves and verify connection
    Message init_message = MakeMessage(username, "");
    if (!stream->Write(init_message)) {
        log(WARNING, "Failed to send initial timeline message - server may be unavailable");
        ire.grpc_status = Status::CANCELLED;
        ire.comm_status = FAILURE_UNKNOWN;
        return ire;
    }

    // Connection successful
    log(INFO, "Sent initial timeline message for user: " + username);
    log(INFO, "Now you are in the timeline");
    ire.grpc_status = Status::OK;
    ire.comm_status = SUCCESS;

    // Create a thread to handle reading messages from server
    std::thread reader_thread([&]() {
        Message server_message;
        while (stream->Read(&server_message)) {
            std::time_t time = server_message.timestamp().seconds();
            displayPostMessage(server_message.username(), server_message.msg(), time);
        }
    });

    // Create a thread to handle writing messages to server
    std::thread writer_thread([&]() {
        while (true) {
            std::string input = getPostMessage();

            // Remove newline if present
            if (!input.empty() && input.back() == '\n') {
                input.pop_back();
            }

            if (!input.empty()) {
                Message user_message = MakeMessage(username, input);
                stream->Write(user_message);
                log(INFO, "Sent message from " + username + ": " + input);
            }
        }
    });

    log(INFO, "Timeline threads started for user: " + username);

    // Wait for the reader thread to finish (when server closes connection)
    reader_thread.join();

    // Cleanup
    stream->WritesDone();
    Status status = stream->Finish();

    if (!status.ok()) {
        log(ERROR, "Timeline stream error for user " + username + ": " + status.error_message());
    } else {
        log(INFO, "Timeline stream finished for user: " + username);
    }

    // Note: writer_thread will be terminated when the program exits

    return ire;
}



//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {

  std::string hostname = "localhost";
  std::string username = "default";
  std::string port = "9090";

  int opt = 0;
  while ((opt = getopt(argc, argv, "h:u:k:")) != -1){
    switch(opt) {
    case 'h':
      hostname = optarg;
      break;
    case 'u':
      username = optarg;
      break;
    case 'k':
      port = optarg;
      break;
    default:
      std::cout << "Invalid Command Line Argument\n";
    }
  }
      
  std::cout << "Logging Initialized. Client starting...";
  
  // Initialize Google Logging
  std::string log_file_name = std::string("client-") + username + "-" + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Client logging initialized for user: " + username);
  
  Client myc(hostname, username, port);
  
  myc.run();
  
  return 0;
}
