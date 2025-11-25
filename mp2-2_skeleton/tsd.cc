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
#include <semaphore.h>
#include <fcntl.h>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <map>
#include <thread>
#include <sys/stat.h>
#include <sys/types.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"


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
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using grpc::Channel;
using grpc::ClientContext;

// Global variables for server configuration
std::string cluster_id_str;
std::string server_id_str;
std::string coordinator_hostname;
std::string coordinator_port;
std::string server_port;
std::string server_directory;
std::string clusterSubdirectory; // "1" for Master, "2" for Slave
bool is_master = false;
std::string slave_hostname;
std::string slave_port;
std::unique_ptr<SNSService::Stub> global_slave_stub;
std::unique_ptr<CoordService::Stub> global_coord_stub;

struct Client {
  std::string username;
  bool connected = true;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  std::map<std::string, std::time_t> follow_times; // Track when we started following each user
  ServerReaderWriter<Message, Message>* stream = 0;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

//Vector that stores every client that has been created
std::vector<Client*> client_db;

// Helper function to find client by username
Client* find_user(const std::string& username) {
  for (Client* client : client_db) {
    if (client->username == username) {
      return client;
    }
  }
  return nullptr;
}



std::string get_sem_name(std::string file_type, std::string client) {
  if (file_type == "users") {
    return "/" + cluster_id_str + "_" + clusterSubdirectory + "_" + "users";
  } 
  else if (file_type == "followers") {
    return  "/" + cluster_id_str + "_" + clusterSubdirectory + "_" + client + "_followers";
  } 
  else if (file_type == "timeline") {
    return  "/" + cluster_id_str + "_" + clusterSubdirectory + "_" + client + "_timeline";
  }
  else if (file_type == "following") {
    return  "/" + cluster_id_str + "_" + clusterSubdirectory + "_" + client + "_following";
  }
  return ".invalid/sem/name.fuk";
}

std::string get_filepath(std::string file_type, std::string client) {

  // Create server directory if it doesn't exist
  // Use same directory structure as synchronizer: ./cluster_{clusterID}/{clusterSubdirectory}/
  // clusterSubdirectory is "1" (Master) or "2" (Slave) based on is_master flag

  std::string cluster_dir = "./cluster_" + cluster_id_str;
  mkdir(cluster_dir.c_str(), 0777);

  std::string server_directory = "./cluster_" + cluster_id_str + "/" + clusterSubdirectory + "/";
  mkdir(server_directory.c_str(), 0777);

  if (file_type == "users") {
    return server_directory + "all_users.txt";
  }
  else if (file_type == "followers") {
    return  server_directory + "_" + client + "_followers.txt";
  }
  else if (file_type == "timeline") {
    return  server_directory + "_" + client + "_timeline.txt";
  } 
  else if (file_type == "following") {
    return server_directory + "_" + client + "following.txt";
  }
  return NULL;
}

// Helper function to write message to file in the required format
void write_message_to_file(const std::string& username, const Message& message) {
  std::string filename = server_directory + "/" + username + ".txt";
  std::ofstream file(filename, std::ios::app);

  if (file.is_open()) {
    // Convert timestamp to string
    std::time_t time = message.timestamp().seconds();
    std::string time_str = std::ctime(&time);
    time_str.pop_back(); // Remove newline

    // Write in the required format
    file << "T " << time_str << std::endl;
    file << "U " << message.username() << std::endl;
    file << "W " << message.msg() << std::endl;
    file << std::endl; // Empty line

    file.close();
  }
}

// Helper function to write username to all_users.txt
void add_user_to_all_users_file(const std::string& username) {
  std::string filename = get_filepath("user", "INVALID");
  std::string semName = get_sem_name("user", "INVALID");
  sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

  log(INFO, "TSD acquiring lock (semaphore: " + semName + ")");
  // Wait for lock before reading/writing
  sem_wait(fileSem);

  log(INFO, "TSD reading from file: " + filename + " (semaphore: " + semName + ")");
  // Check if user already exists in file
  std::ifstream infile(filename);
  std::string line;
  while (std::getline(infile, line)) {
    if (line == username) {
      infile.close();
      sem_post(fileSem);
      sem_close(fileSem);
      return; // User already in file
    }
  }
  infile.close();

  // User not in file, append it
  log(INFO, "TSD writing to file: " + filename + " (semaphore: " + semName + ")");
  std::ofstream file(filename, std::ios::app);
  if (file.is_open()) {
    file << username << std::endl;
    file.close();
  }

  // Release lock
  sem_post(fileSem);
  sem_close(fileSem);
}

// Helper function to add follower relationship
void add_follower_to_file(const std::string& target_username, const std::string& follower_username) {
  std::string filename = get_filepath("followers", target_username);
  std::string semName = get_sem_name("followers", target_username);

  sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

  log(INFO, "TSD acquiring lock (semaphore: " + semName + ")");
  // Wait for lock before reading/writing
  sem_wait(fileSem);

  log(INFO, "TSD reading from file: " + filename + " (semaphore: " + semName + ")");
  // Check if follower already exists in file
  std::ifstream infile(filename);
  std::string line;
  while (std::getline(infile, line)) {
    if (line == follower_username) {
      infile.close();
      sem_post(fileSem);
      sem_close(fileSem);
      return; // Follower already in file
    }
  }
  infile.close();

  // Follower not in file, append it
  log(INFO, "TSD writing to file: " + filename + " (semaphore: " + semName + ")");
  std::ofstream file(filename, std::ios::app);
  if (file.is_open()) {
    file << follower_username << std::endl;
    file.close();
  }

  // Release lock
  sem_post(fileSem);
  sem_close(fileSem);
}

// Helper function to add following relationship
void add_following_to_file(const std::string& username, const std::string& target_username) {
  std::string filename = get_filepath("following", username);
  std::string semName = get_sem_name("following", username); 
  sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

  log(INFO, "TSD acquiring lock (semaphore: " + semName + ")");
  // Wait for lock before reading/writing
  sem_wait(fileSem);

  log(INFO, "TSD reading from file: " + filename + " (semaphore: " + semName + ")");
  // Check if target user already exists in file
  std::ifstream infile(filename);
  std::string line;
  while (std::getline(infile, line)) {
    if (line == target_username) {
      infile.close();
      sem_post(fileSem);
      sem_close(fileSem);
      return; // Already following
    }
  }
  infile.close();

  // Not following yet, append it
  log(INFO, "TSD writing to file: " + filename + " (semaphore: " + semName + ")");
  std::ofstream file(filename, std::ios::app);
  if (file.is_open()) {
    file << target_username << std::endl;
    file.close();
  }

  // Release lock
  sem_post(fileSem);
  sem_close(fileSem);
}

// Helper function to get file modification time using stat()
std::time_t get_file_mtime(const std::string& filename) {
  struct stat file_stat;
  if (stat(filename.c_str(), &file_stat) == 0) {
    return file_stat.st_mtime;
  }
  return 0; // File doesn't exist or error
}

// Helper function to read last N messages from file after a specific time
std::vector<Message> read_last_messages(const std::string& username, int count, std::time_t after_time = 0) {
  std::vector<Message> messages;
  std::string filename = server_directory + "/" + username + ".txt";
  std::ifstream file(filename);

  if (!file.is_open()) {
    return messages; // Return empty vector if file doesn't exist
  }

  std::vector<std::string> lines;
  std::string line;

  // Read all lines
  while (std::getline(file, line)) {
    lines.push_back(line);
  }
  file.close();

  // Parse messages from the end (newest first)
  for (int i = lines.size() - 1; i >= 0 && messages.size() < count; ) {
    if (i >= 3 && lines[i].empty() &&
        lines[i-1].substr(0, 2) == "W " &&
        lines[i-2].substr(0, 2) == "U " &&
        lines[i-3].substr(0, 2) == "T ") {

      Message msg;

      // Parse timestamp
      std::string time_str = lines[i-3].substr(2);
      google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
      // Convert time string back to time_t and set timestamp
      struct tm tm = {};
      strptime(time_str.c_str(), "%a %b %d %H:%M:%S %Y", &tm);
      std::time_t message_time = mktime(&tm);
      timestamp->set_seconds(message_time);
      timestamp->set_nanos(0);
      msg.set_allocated_timestamp(timestamp);

      // Only include messages after the follow time
      if (message_time > after_time) {
        // Parse username and message
        msg.set_username(lines[i-2].substr(2));
        msg.set_msg(lines[i-1].substr(2));

        messages.push_back(msg);
      }
      i -= 4; // Move to next message block
    } else {
      i--;
    }
  }

  // Messages are already newest to oldest, no need to reverse
  return messages;
}

// Function to send a single heartbeat to the coordinator
bool sendHeartbeat(std::unique_ptr<CoordService::Stub>& coord_stub) {
  ServerInfo server_info;
  server_info.set_serverid(std::stoi(server_id_str));
  server_info.set_hostname("localhost");
  server_info.set_port(server_port);
  server_info.set_type("server");
  server_info.set_is_master(is_master);

  Confirmation confirmation;
  ClientContext context;

  // Add cluster ID as metadata
  context.AddMetadata("clusterid", cluster_id_str);

  Status status = coord_stub->Heartbeat(&context, server_info, &confirmation);

  if (status.ok() && confirmation.status()) {
    is_master = confirmation.is_master();
    log(INFO, "Heartbeat sent successfully to coordinator, role: " +
              std::string(is_master ? "MASTER" : "SLAVE"));
    return true;
  } else {
    log(ERROR, "Failed to send heartbeat to coordinator: " + status.error_message());
    return false;
  }
}

// Mirror request to Slave (only if Master)
bool mirrorToSlave(const Request& request, Reply* reply, const std::string& method) {
    log(INFO, "I AM " + std::string(is_master ? "MASTER" : "SLAVE") + " calling mirrorToSlave ");

  if (!is_master) return true;

  // If global_slave_stub not yet initialized, query coordinator for Slave info
  if (!global_slave_stub && global_coord_stub) {
    csce438::ID server_id;
    server_id.set_id(std::stoi(server_id_str));
    ServerInfo slave_info;
    ClientContext coord_context;
    coord_context.AddMetadata("clusterid", cluster_id_str);

    Status status = global_coord_stub->GetSlave(&coord_context, server_id, &slave_info);
    if (status.ok()) {
      slave_hostname = slave_info.hostname();
      slave_port = slave_info.port();
      std::string slave_address = slave_hostname + ":" + slave_port;
      std::shared_ptr<Channel> slave_channel = grpc::CreateChannel(slave_address, grpc::InsecureChannelCredentials());
      global_slave_stub = SNSService::NewStub(slave_channel);
      log(INFO, "Master discovered Slave at " + slave_address + " for mirroring");
    } else {
      log(WARNING, "Failed to get Slave info from coordinator: " + status.error_message());
      return false;
    }
  }

  if (!global_slave_stub) {
      log(INFO, "NO SLAVE to mirror request");
      return false;
  }

  ClientContext context;
  if (method == "Login") {
    return global_slave_stub->Login(&context, request, reply).ok();
  } 
  else if (method == "Follow") {
    return global_slave_stub->Follow(&context, request, reply).ok();
  } 
  else if (method == "UnFollow") {
    return global_slave_stub->UnFollow(&context, request, reply).ok();
  }
  // else if (method == "List") {
  //   return global_slave_stub->List(&context, request, reply).ok();
  // }
  return true;
}

// Thread function to periodically send heartbeats
void heartbeatThread() {
  // Create coordinator stub
  std::string coord_address = coordinator_hostname + ":" + coordinator_port;
  std::shared_ptr<Channel> channel = grpc::CreateChannel(coord_address, grpc::InsecureChannelCredentials());
  global_coord_stub = CoordService::NewStub(channel);

  log(INFO, "Heartbeat thread started, sending to coordinator at " + coord_address);

  // Send initial registration heartbeat
  if (sendHeartbeat(global_coord_stub)) {
    log(INFO, "Server registered with coordinator (Cluster " + cluster_id_str +
              ", Server " + server_id_str + ")");
  } else {
    log(ERROR, "Failed to register with coordinator");
  }

  // Send periodic heartbeats every 5 seconds
  while (true) {
    sleep(30);
    sendHeartbeat(global_coord_stub);
  }
}


class SNSServiceImpl final : public SNSService::Service {
  
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
    std::string username = request->username();
    log(INFO, std::string(is_master ? "MASTER" : "SLAVE") + " List request from user: " + username);
    
    // Find the requesting user
    Client* requesting_user = nullptr;
    for (Client* client : client_db) {
      if (client->username == username) {
        requesting_user = client;
        break;
      }
    }
    
    if (requesting_user == nullptr) {
      log(WARNING, "List request from non-existent user: " + username);
      return Status::OK;
    }
    
    // Add all users to the reply
    for (Client* client : client_db) {
      list_reply->add_all_users(client->username);
    }
    
    // Add followers of the requesting user to the reply
    for (Client* follower : requesting_user->client_followers) {
      list_reply->add_followers(follower->username);
    }
    
    log(INFO, std::string(is_master ? "MASTER" : "SLAVE") + " List request completed for user: " + username + 
              ", total users: " + std::to_string(client_db.size()) + 
              ", followers: " + std::to_string(requesting_user->client_followers.size()));
    
    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
    std::string follower_username = request->username();
    std::string target_username = request->arguments(0);

    log(INFO, std::string(is_master ? "MASTER" : "SLAVE") + " Follow request: " + follower_username + " wants to follow " + target_username);

    // Check if target username is valid
    if (target_username.empty()) {
      reply->set_msg("FAILURE_INVALID_USERNAME");
      log(WARNING, "Follow request with empty target username from: " + follower_username);
      return Status::OK;
    }

    // Check if user is trying to follow themselves
    if (follower_username == target_username) {
      reply->set_msg("FAILURE_ALREADY_EXISTS");
      log(WARNING, "Follow request: user " + follower_username + " trying to follow themselves");
      return Status::OK;
    }

    // Find the follower and target users
    Client* follower_client = nullptr;
    Client* target_client = nullptr;

    for (Client* client : client_db) {
      if (client->username == follower_username) {
        follower_client = client;
      }
      if (client->username == target_username) {
        target_client = client;
      }
    }

    // Check if target user exists
    if (target_client == nullptr) {
      reply->set_msg("FAILURE_INVALID_USERNAME");
      log(WARNING, "Follow request: target user " + target_username + " does not exist");
      return Status::OK;
    }

    // Check if follower exists
    // should never be null since they must be logged in to follow
    if (follower_client == nullptr) {
      reply->set_msg("FAILURE_INVALID_USERNAME");
      log(WARNING, "Follow request: follower user " + follower_username + " does not exist");
      return Status::OK;
    }

    // Check if already following
    for (Client* following : follower_client->client_following) {
      if (following->username == target_username) {
        reply->set_msg("FAILURE_ALREADY_EXISTS");
        log(INFO, "Follow request: " + follower_username + " already follows " + target_username);
        return Status::OK;
      }
    }

    // Add the relationship
    follower_client->client_following.push_back(target_client);
    target_client->client_followers.push_back(follower_client);

    // Persist follow relationship to files
    add_following_to_file(follower_username, target_username);
    add_follower_to_file(target_username, follower_username);

    // Record the follow time
    std::time_t follow_time = std::time(nullptr);
    follower_client->follow_times[target_username] = follow_time;

    reply->set_msg("SUCCESS");
    log(INFO, std::string(is_master ? "MASTER" : "SLAVE") + " Follow request successful: " + follower_username + " now follows " + target_username +
              " at time " + std::to_string(follow_time));

    // Mirror to Slave if Master
    mirrorToSlave(*request, reply, "Follow");

    return Status::OK;
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
    std::string follower_username = request->username();
    std::string target_username = request->arguments(0);
    
    log(INFO, std::string(is_master ? "MASTER" : "SLAVE") + " UnFollow request: " + follower_username + " wants to unfollow " + target_username);
    
    // Check if target username is valid
    if (target_username.empty()) {
      reply->set_msg("FAILURE_INVALID_USERNAME");
      log(WARNING, "UnFollow request with empty target username from: " + follower_username);
      return Status::OK;
    }
    
    // Check if user is trying to unfollow themselves
    if (follower_username == target_username) {
      reply->set_msg("FAILURE_INVALID_USERNAME");
      log(WARNING, "UnFollow request: user " + follower_username + " trying to unfollow themselves");
      return Status::OK;
    }
    
    // Find the follower and target users
    Client* follower_client = nullptr;
    Client* target_client = nullptr;
    
    for (Client* client : client_db) {
      if (client->username == follower_username) {
        follower_client = client;
      }
      if (client->username == target_username) {
        target_client = client;
      }
    }
    
    // Check if target user exists
    if (target_client == nullptr) {
      reply->set_msg("FAILURE_INVALID_USERNAME");
      log(WARNING, "UnFollow request: target user " + target_username + " does not exist");
      return Status::OK;
    }
    
    // Check if follower exists
    if (follower_client == nullptr) {
      reply->set_msg("FAILURE_INVALID_USERNAME");
      log(WARNING, "UnFollow request: follower user " + follower_username + " does not exist");
      return Status::OK;
    }
    
    // Check if currently following and remove the relationship
    bool was_following = false;
    
    // Remove from follower's following list
    for (auto it = follower_client->client_following.begin(); it != follower_client->client_following.end(); ++it) {
      if ((*it)->username == target_username) {
        follower_client->client_following.erase(it);
        was_following = true;
        break;
      }
    }
    
    // Remove from target's followers list
    if (was_following) {
      for (auto it = target_client->client_followers.begin(); it != target_client->client_followers.end(); ++it) {
        if ((*it)->username == follower_username) {
          target_client->client_followers.erase(it);
          break;
        }
      }
      
      // Remove the follow time record
      follower_client->follow_times.erase(target_username);
    }

    if (!was_following) {
      reply->set_msg("FAILURE_NOT_A_FOLLOWER");
      log(INFO, "UnFollow request: " + follower_username + " was not following " + target_username);
      return Status::OK;
    }

    reply->set_msg("SUCCESS");
    log(INFO, std::string(is_master ? "MASTER" : "SLAVE") + " UnFollow request successful: " + follower_username + " no longer follows " + target_username);

    // Mirror to Slave if Master
    mirrorToSlave(*request, reply, "UnFollow");

    return Status::OK;
  }

  // RPC Login
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {
    std::string username = request->username();
    log(INFO, std::string(is_master ? "MASTER" : "SLAVE") + " Login request from user: " + username);

    // Check if username is valid (not empty)
    if (username.empty()) {
      reply->set_msg("FAILURE_INVALID_USERNAME");
      log(WARNING, "Login request with empty username");
      return Status::OK;
    }

    // Check if user already exists and is connected
    for (Client* client : client_db) {
      if (client->username == username && client->connected) {
        reply->set_msg("FAILURE_ALREADY_EXISTS");
        log(WARNING, "Login request: user " + username + " already connected");
        return Status::OK;
      }
    }

    // Check if user exists but is disconnected
    Client* existing_client = nullptr;
    for (Client* client : client_db) {
      if (client->username == username) {
        existing_client = client;
        break;
      }
    }

    if (existing_client != nullptr) {
      // User exists but was disconnected, reconnect them
      existing_client->connected = true;
      log(INFO, "User " + username + " reconnected");
    } else {
      // Create new user
      Client* new_client = new Client();
      new_client->username = username;
      new_client->connected = true;
      client_db.push_back(new_client);

      // Persist new user to all_users.txt
      add_user_to_all_users_file(username);
      log(INFO, "New user " + username + " created and connected");
    }

    reply->set_msg("SUCCESS");
    log(INFO, std::string(is_master ? "MASTER" : "SLAVE") + " Login successful for user: " + username);

    // Mirror to Slave if Master
    mirrorToSlave(*request, reply, "Login");

    return Status::OK;
  }

  Status Timeline(ServerContext* context,
		ServerReaderWriter<Message, Message>* stream) override {

    Message message;
    Client* user_client = nullptr;

    // Read the first message to identify the user
    if (stream->Read(&message)) {
      std::string username = message.username();
      log(INFO, "Timeline request from user: " + username);

      user_client = find_user(username);
      if (user_client == nullptr) {
        log(ERROR, "Timeline request from non-existent user: " + username);
        return Status::OK;
      }

      // Set the stream for this client
      user_client->stream = stream;

      // Send last 20 posts from users this client is following
      std::vector<Message> timeline_messages;

      for (Client* following : user_client->client_following) {
        // Get the follow time for this user
        std::time_t follow_time = 0;
        auto follow_time_it = user_client->follow_times.find(following->username);
        if (follow_time_it != user_client->follow_times.end()) {
          follow_time = follow_time_it->second;
        }

        // Only get messages posted after we started following this user
        std::vector<Message> user_messages = read_last_messages(following->username, 20, follow_time);
        timeline_messages.insert(timeline_messages.end(), user_messages.begin(), user_messages.end());

        log(INFO, "Retrieved " + std::to_string(user_messages.size()) + " messages from " +
                  following->username + " after follow time " + std::to_string(follow_time));
      }

      // Sort messages by timestamp (newest first)
      std::sort(timeline_messages.begin(), timeline_messages.end(),
                [](const Message& a, const Message& b) {
                  return a.timestamp().seconds() > b.timestamp().seconds();
                });

      // Send last 20 messages
      int count = 0;
      for (const Message& msg : timeline_messages) {
        if (count >= 20) break;
        stream->Write(msg);
        count++;
      }

      log(INFO, "Sent " + std::to_string(count) + " timeline messages to user: " + username);

      // Track last modification times of followed users' timeline files
      std::map<std::string, std::time_t> last_mtime;
      for (Client* following : user_client->client_following) {
        std::string filename = server_directory + "/" + following->username + ".txt";
        last_mtime[following->username] = get_file_mtime(filename);
      }

      // Flag to control monitoring thread
      bool timeline_active = true;

      // Start file monitoring thread
      std::thread monitor_thread([&]() {
        while (timeline_active && user_client->stream != nullptr) {
          sleep(5); // Check every 5 seconds

          if (!timeline_active || user_client->stream == nullptr) break;

          std::time_t current_time = std::time(nullptr);

          for (Client* following : user_client->client_following) {
            std::string filename = server_directory + "/" + following->username + ".txt";
            std::time_t current_mtime = get_file_mtime(filename);

            // Check if file was modified since last check
            if (current_mtime > last_mtime[following->username]) {
              // Check if file was modified in the last 30 seconds
              if (difftime(current_time, current_mtime) <= 30) {
                log(INFO, "Timeline file for " + following->username + " modified recently, re-sending posts to " + username);

                // Re-send latest 20 posts from all followed users
                std::vector<Message> updated_timeline;

                for (Client* f : user_client->client_following) {
                  std::time_t follow_time = 0;
                  auto follow_time_it = user_client->follow_times.find(f->username);
                  if (follow_time_it != user_client->follow_times.end()) {
                    follow_time = follow_time_it->second;
                  }

                  std::vector<Message> user_messages = read_last_messages(f->username, 20, follow_time);
                  updated_timeline.insert(updated_timeline.end(), user_messages.begin(), user_messages.end());
                }

                // Sort by timestamp (newest first)
                std::sort(updated_timeline.begin(), updated_timeline.end(),
                          [](const Message& a, const Message& b) {
                            return a.timestamp().seconds() > b.timestamp().seconds();
                          });

                // Send latest 20 messages
                int msg_count = 0;
                for (const Message& msg : updated_timeline) {
                  if (msg_count >= 20) break;
                  if (user_client->stream != nullptr) {
                    user_client->stream->Write(msg);
                    msg_count++;
                  }
                }

                log(INFO, "Re-sent " + std::to_string(msg_count) + " updated timeline messages to " + username);
              }

              // Update last modification time
              last_mtime[following->username] = current_mtime;
            }
          }
        }
      });

      // Handle real-time message posting
      while (stream->Read(&message)) {
        if (message.username() == username) {
          log(INFO, "New message from " + username + ": " + message.msg());

          // Write message to user's own file
          write_message_to_file(username, message);

          // Broadcast to all followers who are in timeline mode
          for (Client* follower : user_client->client_followers) {
            if (follower->stream != nullptr) {
              follower->stream->Write(message);
              log(INFO, "Broadcasted message from " + username + " to follower: " + follower->username);
            }
          }
        }
      }

      // Client disconnected from timeline - stop monitoring
      timeline_active = false;
      user_client->stream = nullptr;
      log(INFO, "User " + username + " disconnected from timeline");

      // Wait for monitoring thread to finish
      if (monitor_thread.joinable()) {
        monitor_thread.join();
      }
    }

    return Status::OK;
  }

};

void RunServer(std::string port_no) {
  // Server role is initially false (Slave), will be set by Coordinator on first heartbeat
  is_master = false;

  log(INFO, "Server starting (Cluster " + cluster_id_str + ", Server " + server_id_str +
            "), waiting to learn role from Coordinator...");


  // Start heartbeat thread
  std::thread hb_thread(heartbeatThread);
  hb_thread.detach();
  log(INFO, "Heartbeat thread launched");

  std::string server_address = "0.0.0.0:"+port_no;
  // SNSServiceImpl sns_service;
  SNSServiceImpl ms_service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // builder.RegisterService(&sns_service);
  builder.RegisterService(&ms_service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "SNS Server listening on " + server_address +
            " (Cluster " + cluster_id_str + ", Server " + server_id_str + ")");

  server->Wait();
}

int main(int argc, char** argv) {

  std::string port = "3010";
  std::string cluster_id = "1";
  std::string server_id = "1";
  std::string coord_hostname = "localhost";
  std::string coord_port = "9090";

  int opt = 0;
  while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1){
    switch(opt) {
      case 'c':
          cluster_id = optarg;
          break;
      case 's':
          server_id = optarg;
          break;
      case 'h':
          coord_hostname = optarg;
          break;
      case 'k':
          coord_port = optarg;
          break;
      case 'p':
          port = optarg;
          break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }

  // Set global variables
  cluster_id_str = cluster_id;
  server_id_str = server_id;
  coordinator_hostname = coord_hostname;
  coordinator_port = coord_port;
  server_port = port;

  std::string log_file_name = std::string("server-") + cluster_id + "-" + server_id;
  FLAGS_log_prefix = false;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  log(INFO, "Configuration: Cluster=" + cluster_id + ", Server=" + server_id +
            ", Coordinator=" + coord_hostname + ":" + coord_port +
            ", Port=" + port + ", Directory=" + server_directory);

  RunServer(port);

  return 0;
}
