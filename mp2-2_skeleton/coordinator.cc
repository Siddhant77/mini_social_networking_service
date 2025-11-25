#include <algorithm>
#include <cstdio>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>
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
#include <glog/logging.h>

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity);

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
using csce438::ServerList;
using csce438::Empty;
using csce438::SynchService;

struct zNode{
    int serverID;
    int clusterID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    int consecutive_missed_heartbeats;
    bool is_master;
    bool isActive();
};

struct SyncNode{
    int syncID;
    int clusterID;
    std::string hostname;
    std::string port;
    bool is_master; // true if sync is on master machine of cluster
    std::time_t last_heartbeat;
};

//potentially thread safe
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;

// creating a vector of pointers to cluster vectors for easy iteration
std::vector<std::vector<zNode*>*> clusters = {&cluster1, &cluster2, &cluster3};

// Synchronizer tracking
std::vector<SyncNode*> synchronizers;


//func declarations
int findServer(std::vector<zNode*> v, int id); 
std::time_t getTimeNow();
void checkHeartbeat();


bool zNode::isActive(){
    bool status = false;
    if(!missed_heartbeat){
        status = true;
    }else if(difftime(getTimeNow(),last_heartbeat) < 60){
        status = true;
    }
    return status;
}


class CoordServiceImpl final : public CoordService::Service {

    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        // Extract server information
        int server_id = serverinfo->serverid();
        std::string hostname = serverinfo->hostname();
        std::string port = serverinfo->port();
        std::string type = serverinfo->type();

        // Extract cluster ID from metadata
        int cluster_id = -1;
        const auto& metadata = context->client_metadata();
        for (const auto& entry : metadata) {
            std::string key(entry.first.data(), entry.first.size());
            std::string value(entry.second.data(), entry.second.size());

            if (key == "clusterid") {
                cluster_id = std::stoi(value);
                break;
            }
        }

        if (cluster_id < 1 || cluster_id > 3) {
            log(ERROR, "Heartbeat received with invalid cluster ID: " + std::to_string(cluster_id));
            confirmation->set_status(false);
            return Status::OK;
        }

        // log(INFO, "Heartbeat received from " + type + " " + std::to_string(server_id) +
        //           " in cluster " + std::to_string(cluster_id) +
        //           " at " + hostname + ":" + port);

        // Lock mutex for thread safety
        v_mutex.lock();

        bool server_is_master = false;

        // Handle synchronizer registration
        if (type == "synchronizer" || type == "follower") {
            // Synchronizer heartbeat
            SyncNode* existing_sync = nullptr;
            for (SyncNode* sync : synchronizers) {
                if (sync->syncID == server_id) {
                    existing_sync = sync;
                    break;
                }
            }

            if (existing_sync == nullptr) {
                // New synchronizer registration
                SyncNode* new_sync = new SyncNode();
                new_sync->syncID = server_id;
                new_sync->clusterID = cluster_id;
                new_sync->hostname = hostname;
                new_sync->port = port;
                new_sync->last_heartbeat = getTimeNow();

                // Determine master/slave based on handshake order within the cluster
                // First synchronizer to handshake for this cluster becomes Master
                int sync_count_in_cluster = 0;
                for (SyncNode* sync : synchronizers) {
                    if (sync->clusterID == cluster_id) {
                        sync_count_in_cluster++;
                    }
                }
                new_sync->is_master = (sync_count_in_cluster == 0); // True if first in cluster
                server_is_master = new_sync->is_master;

                synchronizers.push_back(new_sync);

                log(INFO, "SYNCHRONIZER REGISTERED: Cluster " + std::to_string(cluster_id) +
                          ", Synchronizer " + std::to_string(server_id) +
                          " (" + std::string(new_sync->is_master ? "MASTER" : "SLAVE") + ")" +
                          " at " + hostname + ":" + port);
            } else {
                // Update existing synchronizer heartbeat
                existing_sync->last_heartbeat = getTimeNow();
                server_is_master = existing_sync->is_master;

                log(INFO, "Synchronizer heartbeat updated: " + std::to_string(server_id));
            }
        } else {
            // TSD server heartbeat
            std::vector<zNode*>* target_cluster = clusters[cluster_id - 1];

            // Check if server already exists in the cluster
            zNode* existing_server = nullptr;
            for (zNode* node : *target_cluster) {
                if (node->serverID == server_id) {
                    existing_server = node;
                    break;
                }
            }

            if (existing_server == nullptr) {
                // Registration: First heartbeat from this server
                zNode* new_server = new zNode();
                new_server->serverID = server_id;
                new_server->clusterID = cluster_id;
                new_server->hostname = hostname;
                new_server->port = port;
                new_server->type = type;
                new_server->last_heartbeat = getTimeNow();
                new_server->missed_heartbeat = false;
                new_server->consecutive_missed_heartbeats = 0;
                // First server to register in cluster becomes Master
                new_server->is_master = target_cluster->empty();
                server_is_master = new_server->is_master;

                target_cluster->push_back(new_server);

                log(INFO, "SERVER REGISTERED: Cluster " + std::to_string(cluster_id) +
                          ", Server " + std::to_string(server_id) +
                          " (" + std::string(new_server->is_master ? "MASTER" : "SLAVE") + ")" +
                          " at " + hostname + ":" + port);
            } else {
                // Regular heartbeat: Update existing server
                existing_server->last_heartbeat = getTimeNow();
                existing_server->missed_heartbeat = false;
                existing_server->consecutive_missed_heartbeats = 0;
                server_is_master = existing_server->is_master;

                log(INFO, "Heartbeat updated for server " + std::to_string(server_id) +
                          " in cluster " + std::to_string(cluster_id) + " : " + std::string(existing_server->is_master ? "MASTER" : "SLAVE"));
            }
        }

        v_mutex.unlock();

        // Send confirmation with is_master info to the server's response
        confirmation->set_status(true);
        confirmation->set_is_master(server_is_master);
        return Status::OK;
    }

    //function returns the server information for requested client id
    //this function assumes there are always 3 clusters and has math
    //hardcoded to represent this.
    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        int client_id = id->id();

        // Calculate cluster ID using the formula: (ClientID - 1) % 3 + 1
        int cluster_id = ((client_id - 1) % 3) + 1;

        log(INFO, "GetServer request for client " + std::to_string(client_id) +
                  ", assigning to cluster " + std::to_string(cluster_id));

        // Lock mutex for thread safety
        v_mutex.lock();

        // Get the appropriate cluster (cluster_id is 1-indexed, vector is 0-indexed)
        std::vector<zNode*>* target_cluster = clusters[cluster_id - 1];

        // Find an active server in the cluster - prefer Master (is_master=true)
        // This includes newly-promoted Slaves
        zNode* selected_server = nullptr;
        for (zNode* node : *target_cluster) {
            if (node->isActive() && node->is_master) {
                selected_server = node;
                break; // Return Master if active
            }
        }
        // If Master not active, find any active non-Master server
        if (selected_server == nullptr) {
            for (zNode* node : *target_cluster) {
                if (node->isActive()) {
                    selected_server = node;
                    break;
                }
            }
        }

        v_mutex.unlock();

        if (selected_server == nullptr) {
            log(ERROR, "No active server found in cluster " + std::to_string(cluster_id) +
                      " for client " + std::to_string(client_id));
            return Status(grpc::StatusCode::UNAVAILABLE,
                         "No active server available in cluster " + std::to_string(cluster_id));
        }

        // Fill in the server information
        serverinfo->set_serverid(selected_server->serverID);
        serverinfo->set_hostname(selected_server->hostname);
        serverinfo->set_port(selected_server->port);
        serverinfo->set_type(selected_server->type);
        serverinfo->set_is_master(selected_server->is_master);

        log(INFO, "Client " + std::to_string(client_id) +
                  " assigned to server " + std::to_string(selected_server->serverID) +
                  " in cluster " + std::to_string(cluster_id) +
                  " at " + selected_server->hostname + ":" + selected_server->port);

        return Status::OK;
    }

    Status GetAllSynchronizers(ServerContext* context, const Empty* empty, ServerList* serverlist) override {
        v_mutex.lock();

        for (SyncNode* sync : synchronizers) {
            serverlist->add_serverid(sync->syncID);
            serverlist->add_hostname(sync->hostname);
            serverlist->add_port(sync->port);
            serverlist->add_type(sync->is_master ? "synchronizer_master" : "synchronizer_slave");
        }

        v_mutex.unlock();

        log(INFO, "GetAllSynchronizers: returning " + std::to_string(synchronizers.size()) + " synchronizers");
        return Status::OK;
    }

    Status GetSlave(ServerContext* context, const ID* id, ServerInfo* slaveinfo) override {
        int master_server_id = id->id();

        // Extract cluster ID from metadata
        int cluster_id = -1;
        const auto& metadata = context->client_metadata();
        for (const auto& entry : metadata) {
            std::string key(entry.first.data(), entry.first.size());
            std::string value(entry.second.data(), entry.second.size());

            if (key == "clusterid") {
                cluster_id = std::stoi(value);
                break;
            }
        }

        if (cluster_id < 1 || cluster_id > 3) {
            log(ERROR, "GetSlave: Invalid cluster ID: " + std::to_string(cluster_id));
            return Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid cluster ID");
        }

        v_mutex.lock();

        std::vector<zNode*>* target_cluster = clusters[cluster_id - 1];
        zNode* slave_server = nullptr;

        // Find the Slave in this cluster (any server that is not the Master)
        for (zNode* node : *target_cluster) {
            if (!node->is_master) {
                slave_server = node;
                break;
            }
        }

        v_mutex.unlock();

        if (slave_server == nullptr) {
            log(WARNING, "GetSlave: No Slave found in cluster " + std::to_string(cluster_id));
            return Status(grpc::StatusCode::NOT_FOUND, "No Slave available in cluster");
        }

        // Fill in the slave server information
        slaveinfo->set_serverid(slave_server->serverID);
        slaveinfo->set_hostname(slave_server->hostname);
        slaveinfo->set_port(slave_server->port);
        slaveinfo->set_type(slave_server->type);
        slaveinfo->set_is_master(false);

        log(INFO, "GetSlave: Master " + std::to_string(master_server_id) + " in cluster " +
                  std::to_string(cluster_id) + " requesting Slave info: " +
                  slave_server->hostname + ":" + slave_server->port);

        return Status::OK;
    }

    Status GetAllFollowerServers(ServerContext* context, const ID* id, ServerList* serverlist) override {
        int client_id = id->id();
        int cluster_id = ((client_id - 1) % 3) + 1;

        log(INFO, "GetAllFollowerServers request for client " + std::to_string(client_id) +
                  " in cluster " + std::to_string(cluster_id));

        v_mutex.lock();

        std::vector<zNode*>* target_cluster = clusters[cluster_id - 1];

        // Return all servers in the cluster (both Master and Slave)
        for (zNode* node : *target_cluster) {
            serverlist->add_serverid(node->serverID);
            serverlist->add_hostname(node->hostname);
            serverlist->add_port(node->port);
            serverlist->add_type(node->type);
        }

        v_mutex.unlock();

        log(INFO, "GetAllFollowerServers: returning " + std::to_string(serverlist->serverid_size()) +
                  " servers for client " + std::to_string(client_id));
        return Status::OK;
    }

    Status GetFollowerServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        int client_id = id->id();
        int cluster_id = ((client_id - 1) % 3) + 1;

        log(INFO, "GetFollowerServer request for client " + std::to_string(client_id) +
                  " in cluster " + std::to_string(cluster_id));

        v_mutex.lock();

        std::vector<zNode*>* target_cluster = clusters[cluster_id - 1];

        // Return the Master server in the cluster
        zNode* master_server = nullptr;
        for (zNode* node : *target_cluster) {
            if (node->is_master) {
                master_server = node;
                break;
            }
        }

        v_mutex.unlock();

        if (master_server == nullptr) {
            log(WARNING, "GetFollowerServer: No Master found in cluster " + std::to_string(cluster_id) +
                         " for client " + std::to_string(client_id));
            return Status(grpc::StatusCode::NOT_FOUND, "No Master available in cluster");
        }

        // Fill in the server information
        serverinfo->set_serverid(master_server->serverID);
        serverinfo->set_hostname(master_server->hostname);
        serverinfo->set_port(master_server->port);
        serverinfo->set_type(master_server->type);
        serverinfo->set_clusterid(master_server->clusterID);
        serverinfo->set_is_master(true);

        log(INFO, "GetFollowerServer: returning Master " + std::to_string(master_server->serverID) +
                  " for client " + std::to_string(client_id));
        return Status::OK;
    }

};

void RunServer(std::string port_no){
    //start thread to check heartbeats
    std::thread hb(checkHeartbeat);
    hb.detach(); // Let the heartbeat checker run in the background

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
    std::cout << "Coordinator listening on " << server_address << std::endl;
    log(INFO, "Coordinator server started on " + server_address);

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {

    std::string port = "3010";
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

    // Initialize Google Logging
    std::string log_file_name = std::string("coordinator-") + port;
    FLAGS_log_prefix = false;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Coordinator logging initialized on port " + port);

    RunServer(port);
    return 0;
}



void checkHeartbeat(){
    while(true){
        v_mutex.lock();

        // iterating through the clusters vector of pointers to vectors of znodes
        for (auto c : clusters){
            // iterate through servers in the cluster
            for(auto it = c->begin(); it != c->end(); ){
                auto& s = *it;
                if(difftime(getTimeNow(),s->last_heartbeat)>60){
                    if(!s->missed_heartbeat){
                        s->missed_heartbeat = true;
                        s->consecutive_missed_heartbeats = 1;
                        log(WARNING, "Missed heartbeat 1 from server " + std::to_string(s->serverID) +
                                     " in cluster " + std::to_string(s->clusterID));
                        ++it;
                    } else {
                        s->consecutive_missed_heartbeats++;
                        log(WARNING, "Missed heartbeat " + std::to_string(s->consecutive_missed_heartbeats) +
                                     " from server " + std::to_string(s->serverID) +
                                     " in cluster " + std::to_string(s->clusterID));

                        // If Master missed 2 consecutive heartbeats, promote Slave
                        if(s->consecutive_missed_heartbeats >= 2 && s->is_master){
                            log(ERROR, "Master server " + std::to_string(s->serverID) +
                                       " in cluster " + std::to_string(s->clusterID) +
                                       " failed. Promoting Slave.");

                            int failed_cluster_id = s->clusterID;

                            // Find and promote the Slave in this cluster
                            for(auto& slave : *c){
                                if(s->clusterID == slave->clusterID && slave->serverID != s->serverID && !slave->is_master){
                                    slave->is_master = true;
                                    log(INFO, "Slave server " + std::to_string(slave->serverID) +
                                              " in cluster " + std::to_string(slave->clusterID) +
                                              " promoted to MASTER");
                                    break;
                                }
                            }

                            // Promote corresponding Slave synchronizer to Master synchronizer
                            for(auto& sync : synchronizers){
                                if(sync->clusterID == failed_cluster_id && !sync->is_master){
                                    sync->is_master = true;
                                    log(INFO, "Slave synchronizer " + std::to_string(sync->syncID) +
                                              " in cluster " + std::to_string(sync->clusterID) +
                                              " promoted to MASTER");
                                    break;
                                }
                            }

                            // Delete and remove the failed Master from cluster
                            delete s;
                            it = c->erase(it);
                        } else {
                            ++it;
                        }
                    }
                }
                else {
                    ++it;
                }
            }
        }

        v_mutex.unlock();

        sleep(3);
    }
}


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

