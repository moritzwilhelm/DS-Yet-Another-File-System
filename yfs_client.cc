// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <fuse/fuse_lowlevel.h>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst) {
    ec = new extent_client(extent_dst);

}

yfs_client::inum yfs_client::n2i(std::string n) {
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string yfs_client::filename(inum inum) {
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool yfs_client::isfile(inum inum) {
    if (inum & 0x80000000)
        return true;
    return false;
}

bool yfs_client::isdir(inum inum) {
    return !isfile(inum);
}

int yfs_client::getfile(inum inum, fileinfo &fin) {
    int r = OK;


    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

    release:

    return r;
}

int yfs_client::getdir(inum inum, dirinfo &din) {
    int r = OK;


    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

    release:
    return r;
}

yfs_client::status yfs_client::create(inum parent, const std::string &name, unsigned long &new_id) {
    // check if parent exists
    std::string dir_content;
    if (this->ec->get(parent, dir_content) != extent_protocol::OK)
        return yfs_client::NOENT;

    // create new empty file
    new_id = lookup(parent, name);
    if (new_id == 0) {
        this->ec->get_next_id(0, new_id);
        // set file bit
        new_id |= 0x80000000;
        this->ec->put(new_id, "");

        // add file to parent directory
        dir_content += name + ',' + std::to_string(new_id) + ';';
        this->ec->put(parent, dir_content);
    }

    return yfs_client::OK;
}

yfs_client::inum yfs_client::lookup(inum di, std::string name) {
    if (!isdir(di))
        return 0;

    std::string dir_content;
    if (this->ec->get(di, dir_content) != extent_protocol::OK)
        return 0;

    char *token = std::strtok(const_cast<char *>(dir_content.c_str()), ";");
    while (token) {
        std::string entry = std::string(token);
        size_t delimiter = entry.find(',');
        if (name == entry.substr(0, delimiter)) {
            return strtoul(entry.substr(delimiter + 1).c_str(), nullptr, 10);
        }
        token = std::strtok(nullptr, ";");
    }
    return 0;
}

std::vector<yfs_client::dirent> yfs_client::readdir(inum dir) {
    std::vector<dirent> res;

    std::string content;
    ec->get(dir, content);

    char *token = std::strtok(const_cast<char *>(content.c_str()), ";");
    while (token) {
        dirent new_entry;
        std::string entry = std::string(token);
        size_t delimiter = entry.find(',');

        new_entry.name = entry.substr(0, delimiter);
        new_entry.inum = strtoul(entry.substr(delimiter + 1).c_str(), nullptr, 10);
        res.push_back(new_entry);

        token = std::strtok(nullptr, ";");
    }

    return res;
}
