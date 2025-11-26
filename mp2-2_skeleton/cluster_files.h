#ifndef CLUSTER_FILES_H_
#define CLUSTER_FILES_H_

#include <string>
#include <vector>

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

bool FileContainsEntry(const ClusterFilesContext &ctx, UserFileType type, const std::string &username, const std::string &value);

bool AppendUniqueEntry(const ClusterFilesContext &ctx, UserFileType type, const std::string &username, const std::string &value);

bool RemoveEntry(const ClusterFilesContext &ctx, UserFileType type, const std::string &username, const std::string &value);

std::size_t WriteAllUsers(const ClusterFilesContext &ctx, const std::vector<std::string> &users);

bool NeedToSynch(const ClusterFilesContext &ctx, UserFileType type, const std::string &username, int seconds);

} // namespace cluster_files

#endif // CLUSTER_FILES_H_
