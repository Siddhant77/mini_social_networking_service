#include "cluster_files.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <semaphore.h>
#include <string>
#include <unordered_set>
#include <vector>
#include <fcntl.h>
#include <ctime>
#include <iostream>
#include <chrono>

namespace fs = std::filesystem;

namespace cluster_files {
namespace {

fs::path EnsureBase(const ClusterFilesContext &ctx) {
    fs::path cluster_dir = fs::path("cluster_" + std::to_string(ctx.cluster_id));
    fs::create_directories(cluster_dir);
    fs::path server_dir = cluster_dir / ctx.subdirectory;
    fs::create_directories(server_dir);
    return server_dir;
}

std::string FileSuffix(UserFileType type) {
    switch (type) {
    case UserFileType::kFollowers:
        return "_followers.txt";
    case UserFileType::kFollowing:
        return "_following.txt";
    case UserFileType::kTimeline:
        return "_timeline.txt";
    case UserFileType::kAllUsers:
    default:
        return "all_users.txt";
    }
}

class FileSemaphoreLock {
  public:
    explicit FileSemaphoreLock(const std::string &name) : sem_name_(name), sem_(nullptr) {
        sem_ = sem_open(sem_name_.c_str(), O_CREAT, 0644, 1);
        if (sem_) {
            sem_wait(sem_);
        }
    }

    ~FileSemaphoreLock() {
        if (sem_) {
            sem_post(sem_);
            sem_close(sem_);
        }
    }

    FileSemaphoreLock(const FileSemaphoreLock &) = delete;
    FileSemaphoreLock &operator=(const FileSemaphoreLock &) = delete;

  private:
    std::string sem_name_;
    sem_t *sem_;
};

} // namespace

ClusterFilesContext MakeContext(int cluster_id, const std::string &subdirectory) {
    return ClusterFilesContext{cluster_id, subdirectory};
}

std::string GetBaseDirectory(const ClusterFilesContext &ctx) {
    return EnsureBase(ctx).string();
}

std::string GetFilePath(const ClusterFilesContext &ctx, UserFileType type, const std::string &username) {
    fs::path base = EnsureBase(ctx);
    switch (type) {
    case UserFileType::kAllUsers:
        return (base / "all_users.txt").string();
    case UserFileType::kFollowers:
    case UserFileType::kFollowing:
    case UserFileType::kTimeline: {
        std::string suffix = FileSuffix(type);
        return (base / (username + suffix)).string();
    }
    default:
        return (base / "all_users.txt").string();
    }
}

std::string GetSemaphoreName(const ClusterFilesContext &ctx, UserFileType type, const std::string &username) {
    std::string base = "/" + std::to_string(ctx.cluster_id) + "_" + ctx.subdirectory + "_";
    if (type == UserFileType::kAllUsers) {
        return base + "all_users";
    }
    std::string suffix;
    switch (type) {
    case UserFileType::kFollowers:
        suffix = "_followers";
        break;
    case UserFileType::kFollowing:
        suffix = "_following";
        break;
    case UserFileType::kTimeline:
        suffix = "_timeline";
        break;
    default:
        suffix = "_file";
        break;
    }
    return base + username + suffix;
}

std::vector<std::string> ReadUserFile(const ClusterFilesContext &ctx, UserFileType type, const std::string &username) {
    std::vector<std::string> entries;
    std::string path = GetFilePath(ctx, type, username);
    std::string sem_name = GetSemaphoreName(ctx, type, username);
    FileSemaphoreLock lock(sem_name);

    std::ifstream file(path);
    if (!file.is_open()) {
        return entries;
    }

    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty()) {
            entries.push_back(line);
        }
    }

    return entries;
}

std::vector<std::string> ReadTimelineFileWithBlanks(const ClusterFilesContext &ctx, const std::string &username) {
    std::vector<std::string> entries;
    std::string path = GetFilePath(ctx, UserFileType::kTimeline, username);
    std::string sem_name = GetSemaphoreName(ctx, UserFileType::kTimeline, username);
    FileSemaphoreLock lock(sem_name);

    std::ifstream file(path);
    if (!file.is_open()) {
        return entries;
    }

    std::string line;
    while (std::getline(file, line)) {
        // Preserve blank lines for timeline files (they mark post boundaries)
        entries.push_back(line);
    }

    return entries;
}

std::vector<TimelinePost> ReadTimelineAsStructuredPosts(const ClusterFilesContext &ctx, const std::string &username) {
    std::vector<TimelinePost> posts;
    std::string path = GetFilePath(ctx, UserFileType::kTimeline, username);
    std::string sem_name = GetSemaphoreName(ctx, UserFileType::kTimeline, username);
    FileSemaphoreLock lock(sem_name);

    std::ifstream file(path);
    if (!file.is_open()) {
        return posts;
    }

    std::vector<std::string> lines;
    std::string line;
    while (std::getline(file, line)) {
        lines.push_back(line);
    }

    // Parse posts: each post is T line, U line, W line, blank line
    // Process backwards like tsd.cc does, but we'll process forward for simplicity
    for (std::size_t i = 0; i + 3 < lines.size(); i += 4) {
        // Check if we have a valid post structure: T, U, W, blank
        if (lines[i].size() >= 2 && lines[i].substr(0, 2) == "T " &&
            lines[i+1].size() >= 2 && lines[i+1].substr(0, 2) == "U " &&
            lines[i+2].size() >= 2 && lines[i+2].substr(0, 2) == "W " &&
            lines[i+3].empty()) {

            TimelinePost post;
            post.timestamp = lines[i];
            post.user = lines[i+1];
            post.message = lines[i+2];
            posts.push_back(post);
        }
    }

    return posts;
}

void WriteTimelinePost(const ClusterFilesContext &ctx, const std::string &username, const TimelinePost &post) {
    std::string path = GetFilePath(ctx, UserFileType::kTimeline, username);
    std::string sem_name = GetSemaphoreName(ctx, UserFileType::kTimeline, username);
    FileSemaphoreLock lock(sem_name);

    std::ofstream file(path, std::ios::app);
    if (file.is_open()) {
        file << post.timestamp << std::endl;
        file << post.user << std::endl;
        file << post.message << std::endl;
        file << std::endl;  // Blank line
    }
}

std::time_t GetTimelineFileModTime(const ClusterFilesContext &ctx, const std::string &username) {
    std::string path = GetFilePath(ctx, UserFileType::kTimeline, username);
    try {
        if (fs::exists(path)) {
            auto last_write = fs::last_write_time(path);
            // Convert filesystem time to system_clock time, then to time_t
            auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
                last_write - fs::file_time_type::clock::now() + std::chrono::system_clock::now()
            );
            auto tt = std::chrono::system_clock::to_time_t(sctp);
            // std::cout << "[GetTimelineFileModTime] File " << path << " mtime=" << tt << std::endl;
            return tt;
        }
    } catch (const std::exception &e) {
        // std::cout << "[GetTimelineFileModTime] Error: " << e.what() << " for " << path << std::endl;
    }
    // std::cout << "[GetTimelineFileModTime] File not found: " << path << std::endl;
    return 0;
}

std::vector<TimelinePost> ReadNewTimelinePosts(
    const ClusterFilesContext &ctx,
    const std::string &username,
    std::time_t follow_time,
    const std::unordered_set<std::string> &sent_post_keys) {

    std::vector<TimelinePost> new_posts;
    std::string path = GetFilePath(ctx, UserFileType::kTimeline, username);
    std::string sem_name = GetSemaphoreName(ctx, UserFileType::kTimeline, username);
    FileSemaphoreLock lock(sem_name);

    std::ifstream file(path);
    if (!file.is_open()) {
        // std::cout << "[ReadNewTimelinePosts] File not found: " << path << std::endl;
        return new_posts;
    }

    std::vector<std::string> lines;
    std::string line;
    while (std::getline(file, line)) {
        lines.push_back(line);
    }
    // std::cout << "[ReadNewTimelinePosts] Read " << lines.size() << " lines from " << username << std::endl;

    // Parse posts: each post is T line, U line, W line, blank line
    int posts_parsed = 0;
    int posts_filtered_by_time = 0;
    int posts_filtered_by_dedup = 0;

    for (std::size_t i = 0; i + 3 < lines.size(); i += 4) {
        // Check if we have a valid post structure: T, U, W, blank
        if (lines[i].size() >= 2 && lines[i].substr(0, 2) == "T " &&
            lines[i+1].size() >= 2 && lines[i+1].substr(0, 2) == "U " &&
            lines[i+2].size() >= 2 && lines[i+2].substr(0, 2) == "W " &&
            lines[i+3].empty()) {

            posts_parsed++;

            // Parse timestamp
            std::string time_str = lines[i].substr(2);
            struct tm tm = {};
            strptime(time_str.c_str(), "%a %b %d %H:%M:%S %Y", &tm);
            std::time_t msg_time = mktime(&tm);

            // std::cout << "[ReadNewTimelinePosts] Post " << posts_parsed << " from " << username
            //           << " - msg_time=" << msg_time << " follow_time=" << follow_time << std::endl;

            // Sanity check 1: Post made after following time
            if (msg_time <= follow_time) {
                // std::cout << "[ReadNewTimelinePosts] Filtered by time: " << lines[i] << std::endl;
                posts_filtered_by_time++;
                continue;
            }

            // Create post key for deduplication
            std::string post_key = lines[i] + "|" + lines[i+1] + "|" + lines[i+2];

            // Sanity check 2: Post not already sent
            if (sent_post_keys.find(post_key) != sent_post_keys.end()) {
                // std::cout << "[ReadNewTimelinePosts] Filtered by dedup: " << lines[i] << std::endl;
                posts_filtered_by_dedup++;
                continue;
            }

            // Create TimelinePost
            TimelinePost post;
            post.timestamp = lines[i];
            post.user = lines[i+1];
            post.message = lines[i+2];
            new_posts.push_back(post);
            // std::cout << "[ReadNewTimelinePosts] Added new post: " << lines[i] << std::endl;
        }
    }

    // std::cout << "[ReadNewTimelinePosts] Summary for " << username << ": parsed=" << posts_parsed
    //           << " filtered_by_time=" << posts_filtered_by_time << " filtered_by_dedup=" << posts_filtered_by_dedup
    //           << " new=" << new_posts.size() << std::endl;

    return new_posts;
}

bool FileContainsEntry(const ClusterFilesContext &ctx, UserFileType type, const std::string &username, const std::string &value) {
    std::vector<std::string> entries = ReadUserFile(ctx, type, username);
    for (const auto &entry : entries) {
        if (entry == value) {
            return true;
        }
    }
    return false;
}

bool AppendUniqueEntry(const ClusterFilesContext &ctx, UserFileType type, const std::string &username, const std::string &value) {
    std::string path = GetFilePath(ctx, type, username);
    std::string sem_name = GetSemaphoreName(ctx, type, username);
    FileSemaphoreLock lock(sem_name);

    std::ifstream infile(path);
    std::string line;
    while (std::getline(infile, line)) {
        if (line == value) {
            return false;
        }
    }

    std::ofstream outfile(path, std::ios::app);
    if (!outfile.is_open()) {
        return false;
    }
    outfile << value << std::endl;
    return true;
}

bool RemoveEntry(const ClusterFilesContext &ctx, UserFileType type, const std::string &username, const std::string &value) {
    std::string path = GetFilePath(ctx, type, username);
    std::string sem_name = GetSemaphoreName(ctx, type, username);
    FileSemaphoreLock lock(sem_name);

    std::ifstream infile(path);
    if (!infile.is_open()) {
        return false;
    }

    std::vector<std::string> remaining;
    std::string line;
    bool removed = false;
    while (std::getline(infile, line)) {
        if (line == value) {
            removed = true;
            continue;
        }
        if (!line.empty()) {
            remaining.push_back(line);
        }
    }
    infile.close();

    if (!removed) {
        return false;
    }

    std::ofstream outfile(path, std::ios::trunc);
    for (const auto &entry : remaining) {
        outfile << entry << std::endl;
    }
    return true;
}

std::size_t WriteAllUsers(const ClusterFilesContext &ctx, const std::vector<std::string> &users) {
    std::string path = GetFilePath(ctx, UserFileType::kAllUsers);
    std::string sem_name = GetSemaphoreName(ctx, UserFileType::kAllUsers);
    FileSemaphoreLock lock(sem_name);

    std::unordered_set<std::string> unique_entries;
    {
        std::ifstream infile(path);
        std::string line;
        while (std::getline(infile, line)) {
            if (!line.empty()) {
                unique_entries.insert(line);
            }
        }
    }

    std::size_t before = unique_entries.size();
    for (const auto &user : users) {
        if (!user.empty()) {
            unique_entries.insert(user);
        }
    }

    std::vector<std::string> merged(unique_entries.begin(), unique_entries.end());
    std::sort(merged.begin(), merged.end());

    std::ofstream outfile(path, std::ios::out | std::ios::trunc);
    for (const auto &user : merged) {
        outfile << user << std::endl;
    }

    return unique_entries.size() - before;
}

bool NeedToSynch(const ClusterFilesContext &ctx, UserFileType type, const std::string &username, int seconds = 5) {
    std::string path = GetFilePath(ctx, type, username);

    if (!fs::exists(path)) {
        return false;
    }
    // temp hacky fix
    return true;

    auto last_write_time = fs::last_write_time(path);
    auto current_time = fs::file_time_type::clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(current_time - last_write_time);

    return duration.count() < seconds;
}

void mergeToTimeline(const ClusterFilesContext &ctx, const std::string &username, const std::vector<std::string> &incomingEntries) {
    // Read existing posts from file
    std::vector<TimelinePost> existingPosts = ReadTimelineAsStructuredPosts(ctx, username);

    // Track unique posts by their full content
    std::unordered_set<std::string> seenPostKeys;
    for (const auto &post : existingPosts) {
        seenPostKeys.insert(post.getUniqueKey());
    }

    // Parse incoming entries into posts (T-U-W blocks with blank lines)
    std::vector<TimelinePost> incomingPosts;
    std::vector<std::string> currentPostLines;

    for (const auto &entry : incomingEntries) {
        if (entry.empty()) {
            // Blank line marks end of post
            if (currentPostLines.size() == 3 &&
                currentPostLines[0].size() >= 2 && currentPostLines[0].substr(0, 2) == "T " &&
                currentPostLines[1].size() >= 2 && currentPostLines[1].substr(0, 2) == "U " &&
                currentPostLines[2].size() >= 2 && currentPostLines[2].substr(0, 2) == "W ") {

                TimelinePost post;
                post.timestamp = currentPostLines[0];
                post.user = currentPostLines[1];
                post.message = currentPostLines[2];
                incomingPosts.push_back(post);
            }
            currentPostLines.clear();
        } else {
            currentPostLines.push_back(entry);
        }
    }
    // Handle last post if it doesn't end with blank line
    if (currentPostLines.size() == 3 &&
        currentPostLines[0].size() >= 2 && currentPostLines[0].substr(0, 2) == "T " &&
        currentPostLines[1].size() >= 2 && currentPostLines[1].substr(0, 2) == "U " &&
        currentPostLines[2].size() >= 2 && currentPostLines[2].substr(0, 2) == "W ") {

        TimelinePost post;
        post.timestamp = currentPostLines[0];
        post.user = currentPostLines[1];
        post.message = currentPostLines[2];
        incomingPosts.push_back(post);
    }

    // Find new posts to add
    std::vector<TimelinePost> newPosts;
    for (const auto &post : incomingPosts) {
        if (seenPostKeys.find(post.getUniqueKey()) == seenPostKeys.end()) {
            newPosts.push_back(post);
            seenPostKeys.insert(post.getUniqueKey());
        }
    }

    // If no new posts were added, data is stale - return without writing
    if (newPosts.empty()) {
        return;
    }

    // Append new posts to timeline file
    std::string path = GetFilePath(ctx, UserFileType::kTimeline, username);
    std::string sem_name = GetSemaphoreName(ctx, UserFileType::kTimeline, username);
    FileSemaphoreLock lock(sem_name);

    std::ofstream file(path, std::ios::app);
    if (file.is_open()) {
        for (const auto &post : newPosts) {
            file << post.timestamp << std::endl;
            file << post.user << std::endl;
            file << post.message << std::endl;
            file << std::endl;  // Blank line between posts
        }
    }
}

}  // namespace cluster_files
