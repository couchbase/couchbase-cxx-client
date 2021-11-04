/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <couchbase/platform/dirutils.h>

#include <dirent.h>
#include <cstring>
#include <system_error>

namespace couchbase::platform
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

std::vector<std::string>
find_files_with_prefix(const std::string& name)
{
    return find_files_with_prefix(dirname(name), basename(name));
}

} // namespace couchbase::platform
