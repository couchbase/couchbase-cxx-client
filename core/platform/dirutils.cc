/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dirutils.h"

#include <cstring>
#ifdef _MSC_VER
#include <filesystem>

#include <windows.h>
#else
#include <dirent.h>
#endif

namespace couchbase::core::platform
{
static std::string
split(const std::string& input, bool directory)
{
    std::string::size_type path = input.find_last_of("\\/");
    std::string file;
    std::string dir;

    if (path == std::string::npos) {
        dir = ".";
        file = input;
    } else {
        dir = input.substr(0, path);
        if (dir.length() == 0) {
            dir = input.substr(0, 1);
        }

        while (dir.length() > 1 && dir.find_last_of("\\/") == (dir.length() - 1)) {
            dir.resize(dir.length() - 1);
        }

        file = input.substr(path + 1);
    }

    if (directory) {
        return dir;
    }
    return file;
}

std::string
dirname(const std::string& dir)
{
    return split(dir, true);
}

std::string
basename(const std::string& name)
{
    return split(name, false);
}

#ifdef _MSC_VER
std::vector<std::string>
find_files_with_prefix(const std::string& dir, const std::string& name)
{
    std::vector<std::string> files;
    auto match = std::filesystem::path(dir) / (name + "*");
    WIN32_FIND_DATAW FindFileData;

    HANDLE hFind = FindFirstFileExW(match.c_str(), FindExInfoStandard, &FindFileData, FindExSearchNameMatch, NULL, 0);

    if (hFind != INVALID_HANDLE_VALUE) {
        do {
            std::wstring fn = FindFileData.cFileName;
            std::string fnm(fn.length(), 0);
            std::transform(fn.begin(), fn.end(), fnm.begin(), [](wchar_t c) { return static_cast<char>(c); });
            if (fnm != "." && fnm != "..") {
                std::string entry = dir;
                entry.append("\\");
                entry.append(fnm);
                files.push_back(entry);
            }
        } while (FindNextFileW(hFind, &FindFileData));

        FindClose(hFind);
    }
    return files;
}
#else
std::vector<std::string>
find_files_with_prefix(const std::string& dir, const std::string& name)
{
    std::vector<std::string> files;
    if (DIR* dp = opendir(dir.c_str()); dp != nullptr) {
        const struct dirent* de;
        while ((de = readdir(dp)) != nullptr) {
            if (std::string fnm(de->d_name); fnm == "." || fnm == "..") {
                continue;
            }
            if (strncmp(de->d_name, name.c_str(), name.length()) == 0) {
                std::string entry = dir;
                entry.append("/");
                entry.append(de->d_name);
                files.push_back(entry);
            }
        }

        closedir(dp);
    }
    return files;
}
#endif

std::vector<std::string>
find_files_with_prefix(const std::string& name)
{
    return find_files_with_prefix(dirname(name), basename(name));
}

} // namespace couchbase::core::platform
