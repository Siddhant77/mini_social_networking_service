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
using csce438::SynchService;

struct zNode{
    int serverID;
    int clusterID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();

};

//potentially thread safe
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;

// creating a vector of pointers to cluster vectors for easy iteration
std::vector<std::vector<zNode*>*> clusters = {&cluster1, &cluster2, &cluster3};


//func declarations
int findServer(std::vector<zNode*> v, int id); 
std::time_t getTimeNow();
void checkHeartbeat();


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

        log(INFO, "Heartbeat received from server " + std::to_string(server_id) +
                  " in cluster " + std::to_string(cluster_id) +
                  " at " + hostname + ":" + port);

        // Lock mutex for thread safety
        v_mutex.lock();

        // Get the appropriate cluster (cluster_id is 1-indexed, vector is 0-indexed)
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

            target_cluster->push_back(new_server);

            log(INFO, "SERVER REGISTERED: Cluster " + std::to_string(cluster_id) +
                      ", Server " + std::to_string(server_id) +
                      " at " + hostname + ":" + port);
        } else {
            // Regular heartbeat: Update existing server
            existing_server->last_heartbeat = getTimeNow();
            existing_server->missed_heartbeat = false;

            log(INFO, "Heartbeat updated for server " + std::to_string(server_id) +
                      " in cluster " + std::to_string(cluster_id));
        }

        v_mutex.unlock();

        // Send confirmation
        confirmation->set_status(true);
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

        // Find an active server in the cluster
        zNode* selected_server = nullptr;
        for (zNode* node : *target_cluster) {
            if (node->isActive()) {
                selected_server = node;
                break; // Return the first active server
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

        log(INFO, "Client " + std::to_string(client_id) +
                  " assigned to server " + std::to_string(selected_server->serverID) +
                  " in cluster " + std::to_string(cluster_id) +
                  " at " + selected_server->hostname + ":" + selected_server->port);

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
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Coordinator logging initialized on port " + port);

    RunServer(port);
    return 0;
}



void checkHeartbeat(){
    while(true){
        //check servers for heartbeat > 10
        //if true turn missed heartbeat = true

        v_mutex.lock();

        // iterating through the clusters vector of pointers to vectors of znodes
        for (auto c : clusters){
            for(auto& s : *c){
                if(difftime(getTimeNow(),s->last_heartbeat)>10){
                    log(WARNING, "Missed heartbeat from server " + std::to_string(s->serverID) +
                                 " in cluster " + std::to_string(s->clusterID) + ", marking as unavailable");
                    if(!s->missed_heartbeat){
                        s->missed_heartbeat = true;
                        s->last_heartbeat = getTimeNow();
                    }
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

