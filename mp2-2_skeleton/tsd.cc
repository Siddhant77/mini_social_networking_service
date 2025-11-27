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
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <atomic>
#include <thread>
#include <sys/stat.h>
#include <sys/types.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include "cluster_files.h"


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

struct FollowRecord {
  std::string username;
  std::time_t timestamp = 0;
  std::string raw_line;
};

std::mutex active_users_mutex;
std::unordered_set<std::string> active_users;
std::mutex stream_mutex;
std::unordered_map<std::string, ServerReaderWriter<Message, Message>*> active_streams;

std::vector<Message> read_last_messages(const std::string& username, int count, std::time_t after_time);

cluster_files::ClusterFilesContext CurrentClusterContext() {
  int cluster_numeric = 1;
  if (!cluster_id_str.empty()) {
    cluster_numeric = std::stoi(cluster_id_str);
  }
  if (clusterSubdirectory.empty()) {
    clusterSubdirectory = is_master ? "1" : "2";
  }
  return cluster_files::MakeContext(cluster_numeric, clusterSubdirectory);
}

void RefreshServerDirectory() {
  if (cluster_id_str.empty()) {
    cluster_id_str = "1";
  }
  clusterSubdirectory = is_master ? "1" : "2";
  cluster_files::ClusterFilesContext ctx = CurrentClusterContext();
  server_directory = cluster_files::GetBaseDirectory(ctx);
}

void EnsureServerDirectory() {
  if (server_directory.empty()) {
    RefreshServerDirectory();
  }
}

bool user_exists(const std::string& username) {
  if (username.empty()) return false;
  cluster_files::ClusterFilesContext ctx = CurrentClusterContext();
  return cluster_files::FileContainsEntry(ctx, cluster_files::UserFileType::kAllUsers, "INVALID", username);
}

std::vector<std::string> load_all_users_from_file() {
  cluster_files::ClusterFilesContext ctx = CurrentClusterContext();
  return cluster_files::ReadUserFile(ctx, cluster_files::UserFileType::kAllUsers, "INVALID");
}

FollowRecord parse_follow_record(const std::string& line) {
  FollowRecord rec;
  rec.raw_line = line;
  size_t delim = line.find('|');
  if (delim == std::string::npos) {
    rec.username = line;
    rec.timestamp = 0;
  } else {
    rec.username = line.substr(0, delim);
    std::string ts = line.substr(delim + 1);
    try {
      rec.timestamp = ts.empty() ? 0 : static_cast<std::time_t>(std::stoll(ts));
    } catch (...) {
      rec.timestamp = 0;
    }
  }
  return rec;
}

std::vector<FollowRecord> get_follow_records(cluster_files::UserFileType type, const std::string& owner) {
  std::vector<FollowRecord> records;
  cluster_files::ClusterFilesContext ctx = CurrentClusterContext();
  std::vector<std::string> lines = cluster_files::ReadUserFile(ctx, type, owner);
  for (const auto& line : lines) {
    if (!line.empty()) {
      records.push_back(parse_follow_record(line));
    }
  }
  return records;
}

std::vector<FollowRecord> get_following_records(const std::string& username) {
  return get_follow_records(cluster_files::UserFileType::kFollowing, username);
}

std::vector<FollowRecord> get_follower_records(const std::string& username) {
  return get_follow_records(cluster_files::UserFileType::kFollowers, username);
}

bool append_follow_entry(cluster_files::UserFileType type,
                         const std::string& owner,
                         const std::string& other_user,
                         std::time_t timestamp) {
  cluster_files::ClusterFilesContext ctx = CurrentClusterContext();
  std::vector<FollowRecord> existing = get_follow_records(type, owner);
  for (const auto& rec : existing) {
    if (rec.username == other_user) {
      return false;
    }
  }
  std::string value = other_user + "|" + std::to_string(static_cast<long long>(timestamp));
  return cluster_files::AppendUniqueEntry(ctx, type, owner, value);
}

bool remove_follow_entry(cluster_files::UserFileType type,
                         const std::string& owner,
                         const std::string& other_user) {
  cluster_files::ClusterFilesContext ctx = CurrentClusterContext();
  std::vector<FollowRecord> existing = get_follow_records(type, owner);
  for (const auto& rec : existing) {
    if (rec.username == other_user) {
      return cluster_files::RemoveEntry(ctx, type, owner, rec.raw_line);
    }
  }
  return false;
}

std::vector<Message> collect_timeline_messages(const std::vector<FollowRecord>& following, std::time_t last_sent_time = 0) {
  std::vector<Message> timeline_messages;
  for (const auto& record : following) {
    // Use the maximum of follow time and last sent time to avoid duplicates
    std::time_t cutoff_time = std::max(record.timestamp, last_sent_time);
    std::vector<Message> user_messages = read_last_messages(record.username, 20, cutoff_time);
    timeline_messages.insert(timeline_messages.end(), user_messages.begin(), user_messages.end());
  }
  std::sort(timeline_messages.begin(), timeline_messages.end(),
            [](const Message& a, const Message& b) {
              return a.timestamp().seconds() > b.timestamp().seconds();
            });
  return timeline_messages;
}

void broadcast_to_followers(const std::string& username, const Message& message) {
  std::vector<FollowRecord> followers = get_follower_records(username);
  for (const auto& follower : followers) {
    ServerReaderWriter<Message, Message>* follower_stream = nullptr;
    {
      std::lock_guard<std::mutex> lock(stream_mutex);
      auto it = active_streams.find(follower.username);
      if (it != active_streams.end()) {
        follower_stream = it->second;
      }
    }
    if (follower_stream != nullptr) {
      follower_stream->Write(message);
      log(INFO, "Broadcasted message from " + username + " to follower: " + follower.username);
    }
  }
}



// Helper function to write message to file in the required format
void write_message_to_file(const std::string& username, const Message& message) {
  EnsureServerDirectory();
  std::string filename = server_directory + "/" + username + "_timeline.txt";
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

void add_user_to_all_users_file(const std::string& username) {
  if (username.empty()) {
    return;
  }
  cluster_files::ClusterFilesContext ctx = CurrentClusterContext();
  cluster_files::AppendUniqueEntry(ctx, cluster_files::UserFileType::kAllUsers, "", username);
}

bool add_follower_to_file(const std::string& target_username, const std::string& follower_username, std::time_t timestamp) {
  if (target_username.empty() || follower_username.empty()) {
    return false;
  }
  return append_follow_entry(cluster_files::UserFileType::kFollowers, target_username, follower_username, timestamp);
}

bool add_following_to_file(const std::string& username, const std::string& target_username, std::time_t timestamp) {
  if (username.empty() || target_username.empty()) {
    return false;
  }
  return append_follow_entry(cluster_files::UserFileType::kFollowing, username, target_username, timestamp);
}

bool remove_follower_from_file(const std::string& target_username, const std::string& follower_username) {
  return remove_follow_entry(cluster_files::UserFileType::kFollowers, target_username, follower_username);
}

bool remove_following_from_file(const std::string& username, const std::string& target_username) {
  return remove_follow_entry(cluster_files::UserFileType::kFollowing, username, target_username);
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
std::vector<Message> read_last_messages(
  const std::string& username, 
  int count, 
  std::time_t after_time = 0) 
  {
  std::vector<Message> messages;
  EnsureServerDirectory();
  std::string filename = server_directory + "/" + username + "_timeline.txt";
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
    RefreshServerDirectory();
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

  if (!is_master) return true;
  log(INFO, "I AM " + std::string(is_master ? "MASTER" : "SLAVE") + " calling mirrorToSlave ");

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

    if (!user_exists(username)) {
      log(WARNING, "List request from non-existent user: " + username);
      return Status::OK;
    }

    std::vector<std::string> all_users = load_all_users_from_file();
    for (const auto& user : all_users) {
      list_reply->add_all_users(user);
    }

    std::vector<FollowRecord> followers = get_follower_records(username);
    for (const auto& follower : followers) {
      list_reply->add_followers(follower.username);
    }

    log(INFO, std::string(is_master ? "MASTER" : "SLAVE") + " List request completed for user: " + username +
              ", total users: " + std::to_string(all_users.size()) +
              ", followers: " + std::to_string(followers.size()));

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

    if (!user_exists(follower_username) || !user_exists(target_username)) {
      reply->set_msg("FAILURE_INVALID_USERNAME");
      log(WARNING, "Follow request: invalid user(s) follower=" + follower_username + " target=" + target_username);
      return Status::OK;
    }

    std::time_t follow_time = std::time(nullptr);

    if (!add_following_to_file(follower_username, target_username, follow_time)) {
      reply->set_msg("FAILURE_ALREADY_EXISTS");
      log(INFO, "Follow request: " + follower_username + " already follows " + target_username);
      return Status::OK;
    }

    add_follower_to_file(target_username, follower_username, follow_time);

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
    
    if (!user_exists(follower_username) || !user_exists(target_username)) {
      reply->set_msg("FAILURE_INVALID_USERNAME");
      log(WARNING, "UnFollow request: invalid user(s) follower=" + follower_username + " target=" + target_username);
      return Status::OK;
    }

    if (!remove_following_from_file(follower_username, target_username)) {
      reply->set_msg("FAILURE_NOT_A_FOLLOWER");
      log(INFO, "UnFollow request: " + follower_username + " was not following " + target_username);
      return Status::OK;
    }

    remove_follower_from_file(target_username, follower_username);

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

    {
      std::lock_guard<std::mutex> lock(active_users_mutex);
      if (active_users.find(username) != active_users.end()) {
        reply->set_msg("FAILURE_ALREADY_EXISTS");
        log(WARNING, "Login request: user " + username + " already connected");
        return Status::OK;
      }
    }

    if (!user_exists(username)) {
      add_user_to_all_users_file(username);
      log(INFO, "New user " + username + " created");
    } else {
      log(INFO, "User " + username + " reconnected");
    }

    {
      std::lock_guard<std::mutex> lock(active_users_mutex);
      active_users.insert(username);
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

    if (!stream->Read(&message)) {
      return Status::OK;
    }

    std::string username = message.username();
    log(INFO, "Timeline request from user: " + username);

    if (!user_exists(username)) {
      log(ERROR, "Timeline request from non-existent user: " + username);
      return Status::OK;
    }

    EnsureServerDirectory();

    {
      std::lock_guard<std::mutex> lock(stream_mutex);
      active_streams[username] = stream;
    }

    auto following_records = get_following_records(username);
    std::vector<Message> timeline_messages = collect_timeline_messages(following_records);
    int count = 0;
    for (const Message& msg : timeline_messages) {
      if (count >= 20) break;
      stream->Write(msg);
      count++;
    }
    log(INFO, "Sent " + std::to_string(count) + " timeline messages to user: " + username);

    while (stream->Read(&message)) {
      if (message.username() == username) {
        log(INFO, "New message from " + username + ": " + message.msg());
        write_message_to_file(username, message);
        broadcast_to_followers(username, message);
      }
    }

    {
      std::lock_guard<std::mutex> lock(stream_mutex);
      active_streams.erase(username);
    }

    log(INFO, "User " + username + " disconnected from timeline");

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
  RefreshServerDirectory();

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
