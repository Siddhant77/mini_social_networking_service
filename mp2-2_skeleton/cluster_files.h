#ifndef CLUSTER_FILES_H_
#define CLUSTER_FILES_H_

#include <string>
#include <vector>
#include <unordered_set>
#include <ctime>


namespace cluster_files {

enum class UserFileType {
    kAllUsers,
    kFollowers,
    kFollowing,
    kTimeline
};

struct ClusterFilesContext {
    int cluster_id;
    std::string subdirectory;
};

ClusterFilesContext MakeContext(int cluster_id, const std::string &subdirectory);

std::string GetBaseDirectory(const ClusterFilesContext &ctx);

std::string GetFilePath(const ClusterFilesContext &ctx, UserFileType type, const std::string &username = "");

std::string GetSemaphoreName(const ClusterFilesContext &ctx, UserFileType type, const std::string &username = "");

std::vector<std::string> ReadUserFile(const ClusterFilesContext &ctx, UserFileType type, const std::string &username = "");

std::vector<std::string> ReadTimelineFileWithBlanks(const ClusterFilesContext &ctx, const std::string &username);

// Represents a single structured timeline post: T line, U line, W line
struct TimelinePost {
    std::string timestamp;  // Full T line (e.g., "T Thu Nov 27 00:52:44 2025")
    std::string user;       // Full U line (e.g., "U 1")
    std::string message;    // Full W line (e.g., "W p11")

    // For deduplication - identify unique posts by combining all three lines
    std::string getUniqueKey() const {
        return timestamp + "|" + user + "|" + message;
    }
};

std::vector<TimelinePost> ReadTimelineAsStructuredPosts(const ClusterFilesContext &ctx, const std::string &username);

void WriteTimelinePost(const ClusterFilesContext &ctx, const std::string &username, const TimelinePost &post);

// Get the last modification time of a timeline file
std::time_t GetTimelineFileModTime(const ClusterFilesContext &ctx, const std::string &username);

// Read new timeline messages since a given follow time, excluding already sent posts
std::vector<TimelinePost> ReadNewTimelinePosts(
    const ClusterFilesContext &ctx,
    const std::string &username,
    std::time_t follow_time,
    const std::unordered_set<std::string> &sent_post_keys);

bool FileContainsEntry(const ClusterFilesContext &ctx, UserFileType type, const std::string &username, const std::string &value);

bool AppendUniqueEntry(const ClusterFilesContext &ctx, UserFileType type, const std::string &username, const std::string &value);

bool RemoveEntry(const ClusterFilesContext &ctx, UserFileType type, const std::string &username, const std::string &value);

std::size_t WriteAllUsers(const ClusterFilesContext &ctx, const std::vector<std::string> &users);

bool NeedToSynch(const ClusterFilesContext &ctx, UserFileType type, const std::string &username, int seconds);

void mergeToTimeline(const ClusterFilesContext &ctx, const std::string &username, const std::vector<std::string> &incomingEntries);

} // namespace cluster_files

#endif // CLUSTER_FILES_H_
