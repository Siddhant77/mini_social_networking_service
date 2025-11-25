// NOTE: This starter code contains a primitive implementation using the default RabbitMQ protocol.
// You are recommended to look into how to make the communication more efficient,
// for example, modifying the type of exchange that publishes to one or more queues, or
// throttling how often a process consumes messages from a queue so other consumers are not starved for messages
// All the functions in this implementation are just suggestions and you can make reasonable changes as long as
// you continue to use the communication methods that the assignment requires between different processes

#include <bits/fs_fwd.h>
#include <ctime>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <semaphore.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unordered_map>
#include <vector>
#include <unordered_set>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <stdio.h>
#include <cstdlib>
#include <unistd.h>
#include <algorithm>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include "sns.grpc.pb.h"
#include "sns.pb.h"
#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <jsoncpp/json/json.h>

#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

namespace fs = std::filesystem;

using csce438::AllUsers;
using csce438::Confirmation;
using csce438::CoordService;
using csce438::Empty;
using csce438::ID;
using csce438::ServerInfo;
using csce438::ServerList;
using csce438::SynchronizerListReply;
using csce438::SynchService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
// tl = timeline, fl = follow list
using csce438::TLFL;

// Global variables for synchronizer configuration
// std::string user_queue;
// std::string client_relations_queue;
// std::string timeline_queue;

int synchID = 1;
int clusterID = 1;
bool isMaster = false;
int total_number_of_registered_synchronizers = 6; // update this by asking coordinator
std::string cluster_id_str;
std::string coordAddr;
std::string clusterSubdirectory;
std::vector<std::string> otherHosts;
std::unordered_map<std::string, int> timelineLengths;
std::unordered_map<std::string, time_t> lastTimelineModification; // Track last modification time for each timeline

std::vector<std::string> get_lines_from_file(std::string, std::string, std::string);
std::vector<std::string> get_all_users_func(int);
std::vector<std::string> get_tl_or_fl(int, int, bool);
std::vector<std::string> getFollowersOfUser(int);
bool file_contains_user(std::string filename, std::string user, std::string file_type, std::string client);

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo, int syncID);

std::unique_ptr<csce438::CoordService::Stub> coordinator_stub_;

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
  // cluster_subdirectory is "1" (Master) or "2" (Slave) based on is_master flag

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

class SynchronizerRabbitMQ
{
private:
    amqp_connection_state_t conn;
    amqp_channel_t channel;
    std::string hostname;
    int port;
    int synchID;
    std::unique_ptr<CoordService::Stub> coord_stub;

    void setupRabbitMQ()
    {
        log(INFO, "setupRabbitMQ connection to " + hostname + ":" + std::to_string(port));
        conn = amqp_new_connection();
        amqp_socket_t *socket = amqp_tcp_socket_new(conn);
        amqp_socket_open(socket, hostname.c_str(), port);
        amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
        amqp_channel_open(conn, channel);
    }

    void declareQueue(const std::string &queueName)
    {
        log(INFO, "declareQueue RabbitMQ queue: " + std::string(queueName));
        amqp_queue_declare(conn, channel, amqp_cstring_bytes(queueName.c_str()),
                           0, 0, 0, 0, amqp_empty_table);
    }

    void publishMessage(const std::string &queueName, const std::string &message)
    {
        // log(INFO, "publishMessage " + std::string(message) + " to RabbitMQ queue: " + std::string(queueName));
        amqp_basic_publish(conn, channel, amqp_empty_bytes, amqp_cstring_bytes(queueName.c_str()),
                           0, 0, NULL, amqp_cstring_bytes(message.c_str()));
    }

    std::string consumeMessage(const std::string &queueName, int timeout_ms = 5000)
    {
        // log(INFO, "consumeMessage from RabbitMQ queue: " + queueName);
        amqp_basic_consume(conn, channel, amqp_cstring_bytes(queueName.c_str()),
                           amqp_empty_bytes, 0, 1, 0, amqp_empty_table);

        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn);

        struct timeval timeout;
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_usec = (timeout_ms % 1000) * 1000;

        amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, &timeout, 0);

        if (res.reply_type != AMQP_RESPONSE_NORMAL)
        {
            return "";
        }

        std::string message(static_cast<char *>(envelope.message.body.bytes), envelope.message.body.len);
        amqp_destroy_envelope(&envelope);
        return message;
    }

public:
    // SynchronizerRabbitMQ(const std::string &host, int p, int id) : hostname(host), port(p), channel(1), synchID(id)
    SynchronizerRabbitMQ(
        const std::string &host, 
        int p, 
        int id, 
        const std::string &coordAddr)
        : hostname("rabbitmq"), 
        port(p), 
        channel(1), 
        synchID(id),
        coord_stub(std::unique_ptr<CoordService::Stub>(
            CoordService::NewStub(
                grpc::CreateChannel(coordAddr, grpc::InsecureChannelCredentials()))))
    {
        setupRabbitMQ();
        std::string user_queue = "s_" + std::to_string(synchID) + "_users_Q";
        std::string client_relations_queue = "s_" + std::to_string(synchID) + "_clients_Q";
        std::string timeline_queue = "s_" + std::to_string(synchID) + "_tl_Q";
        declareQueue(user_queue);
        declareQueue(client_relations_queue);
        declareQueue(timeline_queue);
        // TODO: add or modify what kind of queues exist in your clusters based on your needs
    }

    void publishUserList()
    {
        std::vector<std::string> users = get_all_users_func(synchID);
        std::sort(users.begin(), users.end());
        Json::Value userList;
        for (const auto &user : users)
        {
            userList["users"].append(user);
        }
        Json::FastWriter writer;
        std::string message = writer.write(userList);

        ServerList allSynchronizers = getAllSynchronizers();
        log(INFO, "S_" + std::to_string(synchID) + " publishUserList ...");

        // Publish to all synchronizers except ourselves
        for (int i = 0; i < allSynchronizers.serverid_size(); i++) {
            int serverId = allSynchronizers.serverid(i);
            if (serverId != synchID) {
                std::string queueName = "s_" + std::to_string(serverId) + "_users_Q";
                log(INFO, "S_" + std::to_string(synchID) + " publish " + std::to_string(users.size()) + " users : " + std::string(message) + " to " + queueName);
                publishMessage(queueName, message);
                // log(INFO, "S_" + std::to_string(synchID) + " message published");
            }
        }
        // log(INFO, "S_" + std::to_string(synchID) + " exiting publishUserList");        
    }

    void consumeUserLists()
    {
        std::vector<std::string> allUsers;
        std::unordered_set<std::string> uniqueUsers; // Track unique users to avoid duplicates


        // Consume user list from our own queue (other synchronizers publish to our queue)
        std::string user_queue = "s_" + std::to_string(synchID) + "_users_Q";
        std::string message = consumeMessage(user_queue, 1000); // 1 second timeout
        log(INFO, "S_" + std::to_string(synchID) + " consumeUserLists : " + std::string(message) + " from queue: " + user_queue);

        if (!message.empty())
        {
            Json::Value root;
            Json::Reader reader;
            if (reader.parse(message, root))
            {
                for (const auto &user : root["users"])
                {
                    std::string userName = user.asString();
                    // Only add unique users
                    if (uniqueUsers.find(userName) == uniqueUsers.end())
                    {
                        allUsers.push_back(userName);
                        uniqueUsers.insert(userName);
                    }
                }
            }
        }
        if (!allUsers.empty()) {

            updateAllUsersFile(allUsers);
        }
        // log(INFO, "S_" + std::to_string(synchID) + " exiting consumeUserLists");        
    }

    void publishClientRelations()
    {
        Json::Value relations;
        std::vector<std::string> users = get_all_users_func(synchID);
        // log(INFO, "publishClientRelations Synchronizer " + std::to_string(synchID) + " publishing client relations");
        for (const auto &client : users)
        {
            int clientId = std::stoi(client);
            std::vector<std::string> followers = getFollowersOfUser(clientId);

            Json::Value followerList(Json::arrayValue);
            for (const auto &follower : followers)
            {
                followerList.append(follower);
            }

            if (!followerList.empty())
            {
                relations[client] = followerList;
            }
        }

        Json::FastWriter writer;
        std::string message = writer.write(relations);
        // log(INFO, "S_ " + std::to_string(synchID) + " publishing client relations with " + std::to_string(relations.size()) + " users with followers");
        std::string queueName = "s_" + std::to_string(synchID) + "client_Q";
        publishMessage(queueName, message);
    }

    void consumeClientRelations()
    {
        std::vector<std::string> allUsers = get_all_users_func(synchID);
        int relationsConsumed = 0;

        // log(INFO, "consumeClientRelations Synchronizer " + std::to_string(synchID) + " consuming client relations");

        // Consume client relations from all registered synchronizers
        for (int i = 1; i <= total_number_of_registered_synchronizers; i++)
        {   
            std::string queueName = "s_" + std::to_string(i) + "_clients_Q";
            std::string message = consumeMessage(queueName, 1000); // 1 second timeout

            if (!message.empty())
            {
                Json::Value root;
                Json::Reader reader;
                if (reader.parse(message, root))
                {
                    relationsConsumed += root.size();
                    for (const auto &client : allUsers)
                    {
                        std::string followerFile = get_filepath("followers", client);
                        std::string semName = get_sem_name("followers", client);
                        sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

                        // log(INFO, "S_ " + std::to_string(synchID) + " acquiring lock (semaphore: " + semName + ")");
                        // Wait for lock before writing
                        sem_wait(fileSem);

                        // log(INFO, "S_ " + std::to_string(synchID) + " writing to file: " + followerFile + " (semaphore: " + semName + ")");
                        std::ofstream followerStream(followerFile, std::ios::app | std::ios::out | std::ios::in);
                        if (root.isMember(client))
                        {
                            for (const auto &follower : root[client])
                            {
                                if (!file_contains_user(followerFile, follower.asString(), "followers", client))
                                {
                                    followerStream << follower.asString() << std::endl;
                                }
                            }
                        }
                        followerStream.close();

                        // Release lock
                        sem_post(fileSem);
                        sem_close(fileSem);
                    }
                }
            }
        }
        // log(INFO, "S_ " + std::to_string(synchID) + " consuming client relations: " + std::to_string(relationsConsumed) + " relations consumed");
    }

    // for every client in your cluster, update all their followers' timeline files
    // by publishing your user's timeline file (or just the new updates in them)
    //  periodically to the message queue of the synchronizer responsible for that client
    void publishTimelines()
    {
        std::vector<std::string> users = get_all_users_func(synchID);
        time_t currentTime = time(nullptr);
        int timelinesPublished = 0;
        // log(INFO, "publishTimelines Synchronizer " + std::to_string(synchID) + " for " + std::to_string(users.size()) + " users");

        for (const auto &client : users)
        {
            int clientId = std::stoi(client);
            int client_cluster = ((clientId - 1) % 3) + 1;
            // only do this for clients in your own cluster
            if (client_cluster != clusterID)
            {
                continue;
            }

            // Check if timeline file was modified in the last 30 seconds
            std::string timelineFile = get_filepath("timeline", client);
            bool hasChangedRecently = false;

            // if (stat(timelineFile.c_str(), &fileStat) == 0)
            // {
            //     time_t fileModTime = fileStat.st_mtime;

            //     // Check if this is the first time checking this file or if it was modified in last 30 seconds
            //     if (lastTimelineModification.find(client) == lastTimelineModification.end())
            //     {
            //         // First time checking this file, initialize tracking
            //         lastTimelineModification[client] = fileModTime;
            //         hasChangedRecently = true;
            //     }
            //     else if (fileModTime > lastTimelineModification[client])
            //     {
            //         // File was modified since last check
            //         hasChangedRecently = true;
            //         lastTimelineModification[client] = fileModTime;
            //     }
            // }

            // Only proceed if timeline changed recently
            if (!hasChangedRecently)
            {
                continue;
            }

            // Read the followers.txt file for this client
            std::string followersFile = get_filepath("followers", client);
            std::vector<std::string> followers = get_lines_from_file(followersFile, "followers", client);
            // log(INFO, "publishTimelines num followers " + std::to_string(followers.size()) + " for user " + client.c_str());

            // Only send timeline if there are followers
            if (followers.empty())
            {
                continue;
            }

            std::vector<std::string> timeline = get_tl_or_fl(synchID, clientId, true);

            for (const auto &follower : followers)
            {
                // send the timeline updates of your current user to all its followers
                int followerId = std::stoi(follower);

                // Query coordinator to find which synchronizer manages this follower
                ClientContext context;
                ID followerId_msg;
                followerId_msg.set_id(followerId);
                ServerInfo followerServerInfo;

                Status status = coord_stub->GetFollowerServer(&context, followerId_msg, &followerServerInfo);
                if (!status.ok())
                {
                    log(WARNING, "Failed to get follower server for client " + std::to_string(followerId) +
                                 " from coordinator: " + status.error_message());
                    continue;
                }

                // Determine target synchronizer based on follower's cluster and Master/Slave status
                int follower_cluster = ((followerId - 1) % 3) + 1;
                int target_synch;
                if (followerServerInfo.is_master())
                {
                    // Follower is on Master machine, use synchronizer 1-3 based on cluster
                    target_synch = follower_cluster;
                }
                else
                {
                    // Follower is on Slave machine, use synchronizer 4-6 based on cluster
                    target_synch = follower_cluster + 3;
                }

                // Create JSON message with timeline data
                Json::Value timelineMessage;
                timelineMessage["user"] = client;
                timelineMessage["timeline_posts"] = Json::arrayValue;
                for (const auto &post : timeline)
                {
                    timelineMessage["timeline_posts"].append(post);
                }

                Json::FastWriter writer;
                std::string message = writer.write(timelineMessage);

                // Publish to the follower's synchronizer's timeline queue
                std::string queueName = "s_" + std::to_string(target_synch) + "_tl_Q";
                publishMessage(queueName, message);
                timelinesPublished++;
                // log(INFO, "S_ " + std::to_string(synchID) + " published timeline for user " + client +
                //            " to synch " + std::to_string(target_synch) + " (follower " + follower + ")");
            }
        }
        if (timelinesPublished > 0) {
            log(INFO, "S_ " + std::to_string(synchID) + " published " + std::to_string(timelinesPublished) + " timelines total");
        }
    }

    // For each client in your cluster, consume messages from your timeline queue and modify your client's timeline files based on what the users they follow posted to their timeline
    void consumeTimelines()
    {
        std::string timeline_queue = "s_" + std::to_string(synchID) + "_tl_Q";
        std::string message = consumeMessage(timeline_queue, 1000); // 1 second timeout

        // log(INFO, "consumeTimelines Synchronizer " + std::to_string(synchID) + " queue: " + timeline_queue + " message size: " + std::to_string(message.size()));

        if (!message.empty())
        {
            // consume the message from the queue and update the timeline file of the appropriate client with
            // the new updates to the timeline of the user it follows
            Json::Value root;
            Json::Reader reader;
            if (reader.parse(message, root))
            {
                std::string posted_by_user = root["user"].asString();
                std::vector<std::string> timeline_posts;

                for (const auto &post : root["timeline_posts"])
                {
                    timeline_posts.push_back(post.asString());
                }

                int timelineUpdates = 0;
                // For each client in our cluster, check if they follow the user who posted
                std::vector<std::string> allUsers = get_all_users_func(synchID);
                for (const auto &client : allUsers)
                {
                    std::string followingFile = get_filepath("following", client);
                    if (file_contains_user(followingFile, posted_by_user, "following", client))
                    {
                        // This client follows the user who posted, update their timeline
                        std::string timelineFile = get_filepath("timeline", client);
                        std::string semName = get_sem_name("timeline", client);
                        sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

                        // log(INFO, "S_ " + std::to_string(synchID) + " acquiring lock (semaphore: " + semName + ")");
                        // Wait for lock before writing
                        sem_wait(fileSem);

                        // log(INFO, "S_ " + std::to_string(synchID) + " writing to file: " + timelineFile + " (semaphore: " + semName + ")");
                        std::ofstream timelineStream(timelineFile, std::ios::app | std::ios::out | std::ios::in);
                        for (const auto &post : timeline_posts)
                        {
                            timelineStream << post << std::endl;
                        }
                        timelineStream.close();

                        // Release lock
                        sem_post(fileSem);
                        sem_close(fileSem);
                        timelineUpdates++;
                        // log(INFO, "S_ " + std::to_string(synchID) + " updated " + timelineFile +
                        //            " with posts from user " + posted_by_user + " (semaphore: " + semName + ")");
                    }
                }
                // if (timelineUpdates > 0) {
                //     log(INFO, "S_ " + std::to_string(synchID) + " consumed timeline from user " + posted_by_user +
                //                ", updated " + std::to_string(timelineUpdates) + " clients");
                // }
            }
        }
    }

    ServerList getAllSynchronizers()
    {
        grpc::ClientContext context;
        ServerList allSynchronizers;
        Empty empty;

        // Query coordinator for all registered synchronizers
        Status status = coord_stub->GetAllSynchronizers(&context, empty, &allSynchronizers);

        if (status.ok()) {
            return allSynchronizers;
        } else {
            log(WARNING, "Failed to get synchronizers from coordinator: " + status.error_message());
            return ServerList(); // Return empty ServerList on failure
        }
    }

private:
    void updateAllUsersFile(const std::vector<std::string> &users)
    {

        std::string usersFile = get_filepath("users", "INVALID");
        std::string semName = get_sem_name("users", "INVALID");

        sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

        sem_wait(fileSem);

        std::unordered_set<std::string> uniqueUsers;
        for (const auto &existing : get_lines_from_file(usersFile, "users", "INVALID"))
        {
            uniqueUsers.insert(existing);
        }
        size_t before = uniqueUsers.size();
        for (const auto &user : users)
        {
            uniqueUsers.insert(user);
        }

        std::vector<std::string> merged(uniqueUsers.begin(), uniqueUsers.end());
        std::sort(merged.begin(), merged.end());

        std::ofstream userStream(usersFile, std::ios::out | std::ios::trunc);
        for (const auto &user : merged)
        {
            userStream << user << std::endl;
        }
        userStream.close();

        sem_post(fileSem);
        sem_close(fileSem);

        log(INFO, "S_" + std::to_string(synchID) + " updated " + usersFile + ", added " + std::to_string(uniqueUsers.size() - before) + " new users (semaphore: " + semName + ")");
    }
};

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID, SynchronizerRabbitMQ &rabbitMQ);

class SynchServiceImpl final : public SynchService::Service
{
    // You do not need to modify this in any way
};

void RunServer(std::string coordIP, std::string coordPort, std::string port_no, int synchID)
{
    // localhost = 127.0.0.1
    std::string server_address("127.0.0.1:" + port_no);
    log(INFO, "Starting synchronizer server at " + server_address);
    SynchServiceImpl service;
    // grpc::EnableDefaultHealthCheckService(true);
    // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Initialize RabbitMQ connection with coordinator address
    // SynchronizerRabbitMQ rabbitMQ("localhost", 5672, synchID, coordAddr);
    SynchronizerRabbitMQ rabbitMQ("rabbitmq", 5672, synchID, coordAddr);

    std::thread t1(run_synchronizer, coordIP, coordPort, port_no, synchID, std::ref(rabbitMQ));

    // Create a consumer thread
    std::thread consumerThread([&rabbitMQ]()
                               {
        while (true) {
            log(INFO, "consumer thread ...");
            rabbitMQ.consumeUserLists();
            // rabbitMQ.consumeClientRelations();
            // rabbitMQ.consumeTimelines();
            std::this_thread::sleep_for(std::chrono::seconds(5));
            // you can modify this sleep period as per your choice
        } });

    server->Wait();

    //   t1.join();
    //   consumerThread.join();
}

int main(int argc, char **argv)
{
    int opt = 0;
    std::string coordIP;
    std::string coordPort;
    std::string port = "3029";

    while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1)
    {
        switch (opt)
        {
        case 'h':
            coordIP = optarg;
            break;
        case 'k':
            coordPort = optarg;
            break;
        case 'p':
            port = optarg;
            break;
        case 'i':
            synchID = std::stoi(optarg);
            break;
        default:
            std::cerr << "Invalid Command Line Argument\n";
        }
    }

    std::string log_file_name = std::string("synchronizer-") + port;
    FLAGS_log_prefix = false;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting...");

    coordAddr = coordIP + ":" + coordPort;
    clusterID = ((synchID - 1) % 3) + 1;

    // Synchronizers 1,2,3 are on Master machines; 4,5,6 are on Slave machines
    clusterSubdirectory = (synchID <= 3) ? "1" : "2";

    ServerInfo serverInfo;
    serverInfo.set_hostname("localhost");
    serverInfo.set_port(port);
    serverInfo.set_type("synchronizer");
    serverInfo.set_serverid(synchID);
    serverInfo.set_clusterid(clusterID);
    cluster_id_str = std::to_string(clusterID);

    // log(INFO, "S_ " + std::to_string(synchID) + " initialized: Cluster=" + std::to_string(clusterID) +
    //           ", Directory=./cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory +
    //           " (role will be determined by coordinator handshake)");

    Heartbeat(coordIP, coordPort, serverInfo, synchID);

    RunServer(coordIP, coordPort, port, synchID);
    return 0;
}

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID, SynchronizerRabbitMQ &rabbitMQ)
{
    // log(INFO, "run_synchronizer Synchronizer runner thread started for synch " + std::to_string(synchID));

    // TODO: begin synchronization process
    while (true)
    {
        // the synchronizers sync files every 5 seconds
        sleep(5);
        log(INFO, "run_synchronizer.. ");

        ServerList allSynchronizers = rabbitMQ.getAllSynchronizers();

        if (allSynchronizers.serverid_size() > 0) {
            total_number_of_registered_synchronizers = allSynchronizers.serverid_size();
            // log(INFO, "Retrieved " + std::to_string(total_number_of_registered_synchronizers) + " synchronizers from coordinator");

            std::vector<int> server_ids;
            std::vector<std::string> hosts, ports;
            for (std::string host : allSynchronizers.hostname()) {
                hosts.push_back(host);
            }
            for (std::string p : allSynchronizers.port()) {
                ports.push_back(p);
            }
            for (int serverid : allSynchronizers.serverid()) {
                server_ids.push_back(serverid);
            }

            // Store info about other synchronizers (excluding self)
            otherHosts.clear();
            for (size_t i = 0; i < server_ids.size(); i++) {
                if (server_ids[i] != synchID) {
                    otherHosts.push_back(hosts[i] + ":" + ports[i]);
                }
            }

            // log(INFO, "Known other synchronizers: " + std::to_string(otherHosts.size()));
        }

        // Only publish if this is a Master synchronizer
        if (isMaster) {
            // Publish user list
            rabbitMQ.publishUserList();

            // Publish client relations
            // rabbitMQ.publishClientRelations();

            // Publish timelines
            // rabbitMQ.publishTimelines();
        }

        // All synchronizers consume messages
        rabbitMQ.consumeUserLists();
        // rabbitMQ.consumeClientRelations();
        // rabbitMQ.consumeTimelines();
    }
    return;
}

std::vector<std::string> get_lines_from_file(
    std::string filename, 
    std::string filetype, 
    std::string client)
{
    std::vector<std::string> users;
    std::string user;
    std::ifstream file;
    
    std::string semName = get_sem_name(filetype, client);
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
    file.open(filename);
    if (file.peek() == std::ifstream::traits_type::eof())
    {
        // return empty vector if empty file
        // std::cout<<"returned empty vector bc empty file"<<std::endl;
        file.close();
        sem_close(fileSem);
        return users;
    }
    while (file)
    {
        getline(file, user);

        if (!user.empty())
            users.push_back(user);
    }

    file.close();
    sem_close(fileSem);

    return users;
}

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo, int syncID)
{
    // For the synchronizer, a single initial heartbeat RPC acts as an initialization method which
    // registers the synchronizer with the coordinator and determines whether it is a master

    log(INFO, "Sending initial heartbeat to coordinator");
    std::string coordinatorInfo = coordinatorIp + ":" + coordinatorPort;
    std::unique_ptr<CoordService::Stub> stub = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(coordinatorInfo, grpc::InsecureChannelCredentials())));

    ClientContext context;
    context.AddMetadata("clusterid", std::to_string(clusterID));

    Confirmation confirmation;
    Status status = stub->Heartbeat(&context, serverInfo, &confirmation);

    if (status.ok() && confirmation.status()) {
        isMaster = confirmation.is_master();
        log(INFO, "Synchronizer registered with coordinator, role: " +
                  std::string(isMaster ? "MASTER" : "SLAVE") +
                  " (Cluster " + std::to_string(clusterID) + ", Sync " + std::to_string(syncID) + ")");

        clusterSubdirectory = isMaster ? "1" : "2";
    } else {
        log(ERROR, "Failed to send heartbeat to coordinator: " + status.error_message());
    }
}

bool file_contains_user(
    std::string filename, 
    std::string user,
    std::string filetype,
    std::string client)
{
    std::vector<std::string> users;
    // check username is valid
    std::string semName = get_sem_name(filetype, client);
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
    users = get_lines_from_file(filename, "users", "INVALID");
    for (int i = 0; i < users.size(); i++)
    {
        // std::cout<<"Checking if "<<user<<" = "<<users[i]<<std::endl;
        if (user == users[i])
        {
            // std::cout<<"found"<<std::endl;
            sem_close(fileSem);
            return true;
        }
    }
    // std::cout<<"not found"<<std::endl;
    sem_close(fileSem);
    return false;
}

std::vector<std::string> get_all_users_func(int synchID)
{
    // read all_users file master and client for correct serverID
    // std::string master_users_file = "./master"+std::to_string(synchID)+"/all_users";
    // std::string slave_users_file = "./slave"+std::to_string(synchID)+"/all_users";

    std::string clusterID = std::to_string(((synchID - 1) % 3) + 1);
    std::string master_users_file = get_filepath("users", "INVALID"); //"./cluster_" + clusterID + "/1/all_users.txt";
    // std::string slave_users_file = "./cluster_" + clusterID + "/2/all_users.txt";
    // take longest list and package into AllUsers message
    std::vector<std::string> master_user_list = get_lines_from_file(master_users_file, "users", "INVALID");
    // std::vector<std::string> slave_user_list = get_lines_from_file(slave_users_file);

    return master_user_list;
    // if (master_user_list.size() >= slave_user_list.size())
    //     return master_user_list;
    // else
    //     return slave_user_list;
}

std::vector<std::string> get_tl_or_fl(int synchID, int clientID, bool tl)
{
    // std::string master_fn = "./master"+std::to_string(synchID)+"/"+std::to_string(clientID);
    // std::string slave_fn = "./slave"+std::to_string(synchID)+"/" + std::to_string(clientID);
    std::string master_fn; //= "cluster_" + std::to_string(clusterID) + "/1/" + std::to_string(clientID);
    // std::string slave_fn = "cluster_" + std::to_string(clusterID) + "/2/" + std::to_string(clientID);
    if (tl)
    {
        // master_fn.append("_timeline.txt");
        // slave_fn.append("_timeline.txt");
    }
    else
    {
        // master_fn.append("_followers.txt");
        // slave_fn.append("_followers.txt");
    }

    master_fn = get_filepath(tl ? "timeline" : "followers", std::to_string(clientID));
    std::vector<std::string> m = get_lines_from_file(master_fn, tl ? "timeline" : "followers", std::to_string(clientID));
    // std::vector<std::string> s = get_lines_from_file(slave_fn);

    return m;
    // if (m.size() >= s.size())
    // {
    //     return m;
    // }
    // else
    // {
    //     return s;
    // }
}

std::vector<std::string> getFollowersOfUser(int ID)
{
    std::vector<std::string> followers;
    std::string clientID = std::to_string(ID);
    std::vector<std::string> usersInCluster = get_all_users_func(synchID);

    for (auto userID : usersInCluster)
    { // Examine each user's following file
        std::string file = get_filepath("following", userID); 
        std::string semName = get_sem_name("following", userID); 
        sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
        // std::cout << "Reading file " << file << std::endl;
        if (file_contains_user(file, clientID, "following", userID))
        {
            followers.push_back(userID);
        }
        sem_close(fileSem);
    }

    return followers;
}
