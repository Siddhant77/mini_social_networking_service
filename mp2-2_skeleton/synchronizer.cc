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
#include <queue>
#include <vector>
#include <unordered_set>
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
#include "cluster_files.h"

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <jsoncpp/json/json.h>
#include <condition_variable>

#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

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

std::string user_queue; 
std::string client_follower_queue; 
std::string client_following_queue; 
std::string timeline_queue;

std::string get_queue_name(int ID, const std::string &type){
    return "s_" + std::to_string(ID) + "_" + type + "_Q";
}

std::string get_user_queue(int ID){
    return get_queue_name(ID, "users");
}

std::string get_following_queue(int ID){
    return get_queue_name(ID, "following");
}

std::string get_follower_queue(int ID){
    return get_queue_name(ID, "follower");
}

std::string get_timeline_queue(int ID){
    return get_queue_name(ID, "tl");
}



std::vector<std::string> get_lines_from_file(std::string, std::string, std::string);
std::vector<std::string> get_all_users_func(int);
std::vector<std::string> get_tl_or_fl(int, int, bool);
std::vector<std::string> getFollowersOfUser(int);
bool file_contains_user(std::string filename, std::string user, std::string file_type, std::string client);

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo, int syncID);

std::unique_ptr<csce438::CoordService::Stub> coordinator_stub_;

cluster_files::ClusterFilesContext CurrentClusterContext() {
    if (clusterSubdirectory.empty()) {
        clusterSubdirectory = (synchID <= 3) ? "1" : "2";
    }
    return cluster_files::MakeContext(clusterID, clusterSubdirectory);
}

cluster_files::UserFileType ToFileType(const std::string &file_type) {
    if (file_type == "followers") {
        return cluster_files::UserFileType::kFollowers;
    } else if (file_type == "following") {
        return cluster_files::UserFileType::kFollowing;
    } else if (file_type == "timeline") {
        return cluster_files::UserFileType::kTimeline;
    }
    return cluster_files::UserFileType::kAllUsers;
}

std::string get_sem_name(std::string file_type, std::string client) {
  return cluster_files::GetSemaphoreName(CurrentClusterContext(), ToFileType(file_type), client);
}

std::string get_filepath(std::string file_type, std::string client) {
  return cluster_files::GetFilePath(CurrentClusterContext(), ToFileType(file_type), client);
}

class SynchronizerRabbitMQ
{
private:
    amqp_connection_state_t conn;
    // Publish channels (one per message type)
    amqp_channel_t publish_users_channel;
    amqp_channel_t publish_followers_channel;
    amqp_channel_t publish_following_channel;
    amqp_channel_t publish_timeline_channel;
    // Consume channels (one per message type)
    amqp_channel_t consume_users_channel;
    amqp_channel_t consume_followers_channel;
    amqp_channel_t consume_following_channel;
    amqp_channel_t consume_timeline_channel;
    std::mutex dispatch_mutex;
    std::condition_variable dispatch_cv;
    std::unordered_map<amqp_channel_t, std::queue<std::string>> dispatch_queues;
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
        // Open 4 publish channels (one per message type)
        amqp_channel_open(conn, publish_users_channel);
        amqp_channel_open(conn, publish_followers_channel);
        amqp_channel_open(conn, publish_following_channel);
        amqp_channel_open(conn, publish_timeline_channel);
        // Open 4 consume channels (one per message type)
        amqp_channel_open(conn, consume_users_channel);
        amqp_channel_open(conn, consume_followers_channel);
        amqp_channel_open(conn, consume_following_channel);
        amqp_channel_open(conn, consume_timeline_channel);
    }

    void declareQueue(const std::string &queueName, const std::string &messageType = "users")
    {
        log(INFO, "declareQueue RabbitMQ queue: " + std::string(queueName));
        // Determine which consume channel based on queue name
        amqp_channel_t channel = consume_users_channel;
        if (messageType == "follower") {
            channel = consume_followers_channel;
        } else if (messageType == "following") {
            channel = consume_following_channel;
        } else if (messageType == "timeline") {
            channel = consume_timeline_channel;
        }
        amqp_queue_declare(conn, channel, amqp_cstring_bytes(queueName.c_str()),
                           0, 0, 0, 0, amqp_empty_table);
    }

    void publishMessage(const std::string &queueName, const std::string &message, const std::string &messageType)
    {
        // Determine which publish channel based on message type
        amqp_channel_t channel = publish_users_channel;
        if (messageType == "follower") {
            channel = publish_followers_channel;
        } else if (messageType == "following") {
            channel = publish_following_channel;
        } else if (messageType == "timeline") {
            channel = publish_timeline_channel;
        }
        if (channel == publish_timeline_channel){
            log(INFO, "publishMessage " + std::string(message) + " to RabbitMQ queue: " + std::string(queueName) + " on channel " + std::to_string(channel) + " (" + channelName(channel) + ")");
        }

        amqp_basic_publish(conn, channel, amqp_empty_bytes, amqp_cstring_bytes(queueName.c_str()),
                           0, 0, NULL, amqp_cstring_bytes(message.c_str()));
    }

    std::string channelName(amqp_channel_t channel) const
    {
        if (channel == consume_users_channel) return "consume_users";
        if (channel == consume_followers_channel) return "consume_followers";
        if (channel == consume_following_channel) return "consume_following";
        if (channel == consume_timeline_channel) return "consume_timeline";
        if (channel == publish_users_channel) return "publish_users";
        if (channel == publish_followers_channel) return "publish_followers";
        if (channel == publish_following_channel) return "publish_following";
        if (channel == publish_timeline_channel) return "publish_timeline";
        return "unknown";
    }

    std::string consumeMessage(amqp_channel_t channel)
    {
        std::unique_lock<std::mutex> lock(dispatch_mutex);
        dispatch_cv.wait(lock, [&]() {
            auto it = dispatch_queues.find(channel);
            return it != dispatch_queues.end() && !it->second.empty();
        });

        std::string message = dispatch_queues[channel].front();
        if (channel == consume_timeline_channel){
            log(INFO, "consumeMessage " + message + " from channel: " + std::to_string(channel) + " (" + channelName(channel) + ")");        
        }

        dispatch_queues[channel].pop();
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
        publish_users_channel(1),
        publish_followers_channel(2),
        publish_following_channel(3),
        publish_timeline_channel(4),
        consume_users_channel(5),
        consume_followers_channel(6),
        consume_following_channel(7),
        consume_timeline_channel(8),
        synchID(id),
        coord_stub(std::unique_ptr<CoordService::Stub>(
            CoordService::NewStub(
                grpc::CreateChannel(coordAddr, grpc::InsecureChannelCredentials()))))
    {
        setupRabbitMQ();
        user_queue = get_user_queue(synchID);
        client_follower_queue = get_follower_queue(synchID);
        client_following_queue = get_following_queue(synchID);
        timeline_queue = get_timeline_queue(synchID);
        declareQueue(user_queue, "users");
        declareQueue(client_follower_queue, "follower");
        declareQueue(client_following_queue, "following");
        declareQueue(timeline_queue, "timeline");

        // Initialize consumers - register each queue on its dedicated consume channel
        amqp_basic_consume(conn, consume_users_channel, amqp_cstring_bytes(user_queue.c_str()),
                           amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
        amqp_basic_consume(conn, consume_followers_channel, amqp_cstring_bytes(client_follower_queue.c_str()),
                           amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
        amqp_basic_consume(conn, consume_following_channel, amqp_cstring_bytes(client_following_queue.c_str()),
                           amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
        amqp_basic_consume(conn, consume_timeline_channel, amqp_cstring_bytes(timeline_queue.c_str()),
                           amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
        log(INFO, "S_" + std::to_string(synchID) + " initialized all consumers");
    }

    void dispatcherLoop()
    {
        while (true)
        {
            amqp_envelope_t envelope;
            amqp_maybe_release_buffers(conn);
            amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, nullptr, 0);

            if (res.reply_type != AMQP_RESPONSE_NORMAL)
            {
                amqp_destroy_envelope(&envelope);
                continue;
            }

            std::string payload(static_cast<char *>(envelope.message.body.bytes), envelope.message.body.len);
            amqp_channel_t delivery_channel = envelope.channel;
            amqp_destroy_envelope(&envelope);

            {
                std::lock_guard<std::mutex> lock(dispatch_mutex);
                dispatch_queues[delivery_channel].push(payload);
            }
            dispatch_cv.notify_all();
        }
    }

    void publishUserList()
    {
        // log(INFO, "S_" + std::to_string(synchID) + " publishUserList called");
        std::vector<std::string> users = get_all_users_func(synchID);

        // Early return if no users
        if (users.empty()) {
            return;
        }

        // Early return if all_users file hasn't changed recently
        // if (!NeedToSynch(
        //         CurrentClusterContext(),
        //         cluster_files::UserFileType::kAllUsers,
        //         "INVALID",
        //         5)) {
        //     log(INFO, "S_" + std::to_string(synchID) + " publishUserList: No recent changes to all_users file, skipping publish.");
        //     return;
        // }

        std::sort(users.begin(), users.end());
        Json::Value userList;
        for (const auto &user : users)
        {
            userList["users"].append(user);
        }
        Json::FastWriter writer;
        std::string message = writer.write(userList);

        if (message.empty() || message == "null"){
            return;
        }



        ServerList allSynchronizers = getAllSynchronizers();

        // Publish to all synchronizers except ourselves
        for (int i = 0; i < allSynchronizers.serverid_size(); i++) {
            int serverId = allSynchronizers.serverid(i);
            if (serverId != synchID) {
                // log(INFO, "S_" + std::to_string(synchID) + " publish " + message + " to " + get_user_queue(serverId));
                publishMessage(get_user_queue(serverId), message, "users");
            }
        }
    }

    void handleUserListMessage(const std::string &message)
    {
        if (message.empty() || message == "null")
        {
            return;
        }

        // Parse incoming data
        Json::Value root;
        Json::Reader reader;
        if (!reader.parse(message, root))
        {
            return;
        }

        // Extract incoming users
        std::vector<std::string> incomingUsers;
        for (const auto &user : root["users"])
        {
            incomingUsers.push_back(user.asString());
        }

        // Get current data
        std::vector<std::string> currentUsers = get_all_users_func(synchID);

        // Find new users that aren't in current list
        std::vector<std::string> newUsers;
        for (const auto &incomingUser : incomingUsers)
        {
            if (std::find(currentUsers.begin(), currentUsers.end(), incomingUser) == currentUsers.end())
            {
                newUsers.push_back(incomingUser);
            }
        }

        // Only update if there are new users to add
        if (!newUsers.empty())
        {
            // Merge: combine current users with new users
            std::vector<std::string> mergedUsers = currentUsers;
            mergedUsers.insert(mergedUsers.end(), newUsers.begin(), newUsers.end());
            updateAllUsersFile(mergedUsers);
            // log(INFO, "S_" + std::to_string(synchID) + " consumeUserLists : added " +
            //           std::to_string(newUsers.size()) + " new users from queue: " + user_queue);
        }
        // If incoming is subset of or equal to current, do nothing - break the cycle
    }

    void consumeUserListsLoop()
    {
        while (true)
        {
            std::string message = consumeMessage(consume_users_channel);
            handleUserListMessage(message);
        }
    }

    void publishClientFollower()
    {
        // log(INFO, "S_" + std::to_string(synchID) + " publishClientFollower called");
        // Early return if followers file hasn't changed recently
        // if (!NeedToSynch(
        //         CurrentClusterContext(),
        //         cluster_files::UserFileType::kFollowers,
        //         std::to_string(synchID),
        //         5)) {
        //     return;
        // }

        Json::Value relations;
        std::vector<std::string> users = get_all_users_func(synchID);

        for (const auto &client : users)
        {
            std::vector<std::string> followerEntries = cluster_files::ReadUserFile(
                CurrentClusterContext(),
                cluster_files::UserFileType::kFollowers,
                client
            );

            if (!followerEntries.empty())
            {
                Json::Value followerList(Json::arrayValue);
                for (const auto &entry : followerEntries)
                {
                    followerList.append(entry);
                }
                relations[client] = followerList;
            }
        }

        Json::FastWriter writer;
        std::string message = writer.write(relations);

        if (message.empty()){
            return;
        }

        // log(INFO, "S_ " + std::to_string(synchID) + " publishing client follower with " + message);

        // Publish to all synchronizers except ourselves
        ServerList allSynchronizers = getAllSynchronizers();
        for (int i = 0; i < allSynchronizers.serverid_size(); i++) {
            int serverId = allSynchronizers.serverid(i);
            if (serverId != synchID) {
                publishMessage(get_follower_queue(serverId), message, "follower");
            }
        }
    }

    void handleFollowerMessage(const std::string &message)
    {
        std::vector<std::string> allUsers = get_all_users_func(synchID);

        if (!message.empty())
        {
            Json::Value root;
            Json::Reader reader;
            if (reader.parse(message, root))
            {
                for (const auto &client : allUsers)
                {
                    if (root.isMember(client))
                    {
                        for (const auto &follower : root[client])
                        {
                            cluster_files::AppendUniqueEntry(
                                CurrentClusterContext(),
                                cluster_files::UserFileType::kFollowers,
                                client,
                                follower.asString()
                            );
                        }
                    }
                }
            }
        }
    }

    void consumeClientFollowerLoop()
    {
        while (true)
        {
            std::string message = consumeMessage(consume_followers_channel);
            // log(INFO, "S_" + std::to_string(synchID) + " consuming client follower with " + message);
            handleFollowerMessage(message);
        }
    }

    void publishClientFollowing()
    {
        // log(INFO, "S_" + std::to_string(synchID) + " publishClientFollowing called");
        // Early return if following file hasn't changed recently
        // if (!NeedToSynch(
        //         CurrentClusterContext(),
        //         cluster_files::UserFileType::kFollowing,
        //         std::to_string(synchID),
        //         5)) {
        //     return;
        // }

        Json::Value following;
        std::vector<std::string> users = get_all_users_func(synchID);

        for (const auto &client : users)
        {
            // Read this user's following file
            std::vector<std::string> followingList = cluster_files::ReadUserFile(
                CurrentClusterContext(),
                cluster_files::UserFileType::kFollowing,
                client
            );

            if (!followingList.empty())
            {
                Json::Value followingArray(Json::arrayValue);
                for (const auto &followedUser : followingList)
                {
                    followingArray.append(followedUser);
                }
                following[client] = followingArray;
            }
        }

        Json::FastWriter writer;
        std::string message = writer.write(following);

        if (message.empty()){
            return;
        }

        // log(INFO, "S_ " + std::to_string(synchID) + " publishing client following with " + message);

        // Publish to all synchronizers except ourselves
        ServerList allSynchronizers = getAllSynchronizers();
        for (int i = 0; i < allSynchronizers.serverid_size(); i++) {
            int serverId = allSynchronizers.serverid(i);
            if (serverId != synchID) {
                publishMessage(get_following_queue(serverId), message, "following");

            }
        }

    }

    void handleFollowingMessage(const std::string &message)
    {
        std::vector<std::string> allUsers = get_all_users_func(synchID);
        if (!message.empty())
        {
            Json::Value root;
            Json::Reader reader;
            if (reader.parse(message, root))
            {
                for (const auto &client : allUsers)
                {
                    if (root.isMember(client))
                    {
                        for (const auto &followedUser : root[client])
                        {
                            cluster_files::AppendUniqueEntry(
                                CurrentClusterContext(),
                                cluster_files::UserFileType::kFollowing,
                                client,
                                followedUser.asString()
                            );
                        }
                    }
                }
            }
        }
    }

    void consumeClientFollowingLoop()
    {
        while (true)
        {
            std::string message = consumeMessage(consume_following_channel);
            // log(INFO, "S_" + std::to_string(synchID) + " consuming client following : " + message);
            handleFollowingMessage(message);
        }
    }

    // Brute-force approach: publish ALL user timelines to ALL synchronizers
    void publishTimelines()
    {
        std::vector<std::string> users = get_all_users_func(synchID);

        if (users.empty()) {
            return;
        }

        // Collect all timelines from all users in this synchronizer's cluster
        Json::Value allTimelines;
        for (const auto &user : users)
        {
            int userId = std::stoi(user);
            // Use ReadTimelineFileWithBlanks to preserve blank lines that mark post boundaries
            std::vector<std::string> timeline = cluster_files::ReadTimelineFileWithBlanks(
                CurrentClusterContext(),
                user
            );
            // log(INFO, "S_" + std::to_string(synchID) + " publishTimelines: User " + user + " has " + std::to_string(timeline.size()) + " timeline entries.");

            if (!timeline.empty())
            {
                Json::Value userTimeline(Json::arrayValue);
                for (const auto &post : timeline)
                {
                    userTimeline.append(post);
                }
                allTimelines[user] = userTimeline;
            }
        }

        Json::FastWriter writer;
        std::string message = writer.write(allTimelines);

        if (message.empty() || message == "null") {
            return;
        }

        // log(INFO, "S_" + std::to_string(synchID) + " publishing timelines to all synchronizers");

        // Broadcast to all OTHER synchronizers
        ServerList allSynchronizers = getAllSynchronizers();
        for (int i = 0; i < allSynchronizers.serverid_size(); i++) {
            int serverId = allSynchronizers.serverid(i);
            if (serverId != synchID) {
                std::string queueName = get_timeline_queue(serverId);
                publishMessage(queueName, message, "timeline");
                // log(INFO, "S_" + std::to_string(synchID) + " published timelines to S_" + std::to_string(serverId));
            }
        }
    }

    // Brute-force approach: consume ALL timelines from all synchronizers and merge with local timelines
    void consumeTimelines()
    {
        std::string message = consumeMessage(consume_timeline_channel);

        // log(INFO, "S_" + std::to_string(synchID) + " consuming timelines from timeline queue: " + timeline_queue);

        if (message.empty())
        {
            return;
        }

        // Parse incoming timeline data from other synchronizer
        Json::Value root;
        Json::Reader reader;
        if (!reader.parse(message, root))
        {
            log(WARNING, "S_" + std::to_string(synchID) + " failed to parse timeline message");
            return;
        }

        log(INFO, "S_" + std::to_string(synchID) + " consuming timeline " + message);

        // For each user in the incoming message
        for (const auto &user_id : root.getMemberNames())
        {
            const Json::Value &incoming_posts = root[user_id];

            // Convert JSON array to vector of strings, preserving blank lines (they mark post boundaries)
            std::vector<std::string> incomingEntries;
            for (const auto &post : incoming_posts)
            {
                std::string post_str = post.asString();
                // Preserve both non-empty and empty strings - empty strings mark post boundaries
                incomingEntries.push_back(post_str);
            }
            // log(INFO, "S_" + std::to_string(synchID) + " user " + user_id + " with " + incomingEntries.size());
            // Merge incoming timeline with local file in a thread-safe manner
            cluster_files::mergeToTimeline(CurrentClusterContext(), user_id, incomingEntries);

        }
    }

    void consumeTimelinesLoop()
    {
        while (true)
        {
            consumeTimelines();
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
        std::size_t added = cluster_files::WriteAllUsers(CurrentClusterContext(), users);
        if (!added){
            return;
        }
        std::string usersFile = cluster_files::GetFilePath(CurrentClusterContext(), cluster_files::UserFileType::kAllUsers);
        std::string semName = cluster_files::GetSemaphoreName(CurrentClusterContext(), cluster_files::UserFileType::kAllUsers, "INVALID");
        log(INFO, "S_" + std::to_string(synchID) + " updated " + usersFile + ", added " + std::to_string(added) + " new users (semaphore: " + semName + ")");
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

    // Create dispatcher and consumer threads
    std::thread dispatcher([&rabbitMQ]() {
        rabbitMQ.dispatcherLoop();
    });

    std::thread userConsumer([&rabbitMQ]() {
        rabbitMQ.consumeUserListsLoop();
    });

    std::thread followerConsumer([&rabbitMQ]() {
        rabbitMQ.consumeClientFollowerLoop();
    });

    std::thread followingConsumer([&rabbitMQ]() {
        rabbitMQ.consumeClientFollowingLoop();
    });

    std::thread timelineConsumer([&rabbitMQ]() {
        rabbitMQ.consumeTimelinesLoop();
    });

    dispatcher.detach();
    userConsumer.detach();
    followerConsumer.detach();
    followingConsumer.detach();
    timelineConsumer.detach();

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

    Heartbeat(coordIP, coordPort, serverInfo, synchID);

    RunServer(coordIP, coordPort, port, synchID);
    return 0;
}

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID, SynchronizerRabbitMQ &rabbitMQ)
{

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
    }

    log(INFO, "run_synchronizer Synchronizer " + std::to_string(synchID) + " starting main loop");

    while (true)
    {
        // log(INFO, "run_synchronizer Synchronizer runner thread started for synch " + std::to_string(synchID));

        // the synchronizers sync files every 5 seconds
        sleep(10);

        // Only publish if this is a Master synchronizer
        if (isMaster) {
            // Publish user list
            rabbitMQ.publishUserList();

            // Publish client relations (followers)
            rabbitMQ.publishClientFollower();

            // Publish client following
            rabbitMQ.publishClientFollowing();

            // Publish timelines
            rabbitMQ.publishTimelines();
        }
    }
    return;
}

std::vector<std::string> get_lines_from_file(
    std::string filename, 
    std::string filetype, 
    std::string client)
{
    (void)filename;
    return cluster_files::ReadUserFile(CurrentClusterContext(), ToFileType(filetype), client);
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
    (void)filename;
    return cluster_files::FileContainsEntry(CurrentClusterContext(), ToFileType(filetype), client, user);
}

std::vector<std::string> get_all_users_func(int synchID)
{
    (void)synchID;
    return cluster_files::ReadUserFile(CurrentClusterContext(), cluster_files::UserFileType::kAllUsers, "INVALID");
}

std::vector<std::string> get_tl_or_fl(int synchID, int clientID, bool tl)
{
    (void)synchID;
    std::string client = std::to_string(clientID);
    cluster_files::UserFileType type = tl ? cluster_files::UserFileType::kTimeline
                                          : cluster_files::UserFileType::kFollowers;
    return cluster_files::ReadUserFile(CurrentClusterContext(), type, client);
}

std::vector<std::string> getFollowersOfUser(int ID)
{
    std::vector<std::string> followers;
    std::string clientID = std::to_string(ID);
    std::vector<std::string> usersInCluster = get_all_users_func(synchID);

    for (auto userID : usersInCluster)
    { // Examine each user's following file
        if (cluster_files::FileContainsEntry(CurrentClusterContext(), cluster_files::UserFileType::kFollowing, userID, clientID))
        {
            followers.push_back(userID);
        }
    }

    return followers;
}
