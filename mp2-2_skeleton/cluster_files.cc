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

    auto last_write_time = fs::last_write_time(path);
    auto current_time = fs::file_time_type::clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(current_time - last_write_time);

    return duration.count() < seconds;
}

} // namespace cluster_files
