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
    lc = new lock_client(lock_dst);
}

yfs_client::~yfs_client() {
    delete ec;
    delete lc;
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
    ScopedExtentLock scopedExtentLock(inum, lc);

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        return IOERR;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

    return OK;
}

int yfs_client::getdir(inum inum, dirinfo &din) {
    ScopedExtentLock scopedExtentLock(inum, lc);

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        return IOERR;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

    return OK;
}

yfs_client::status yfs_client::setattr(inum id, fileinfo &info) {
    ScopedExtentLock scopedExtentLock(id, lc);

    extent_protocol::attr a;
    a.size = info.size;
    a.atime = info.atime;
    a.mtime = info.mtime;
    a.ctime = info.ctime;

    if (this->ec->setattr(id, a) != extent_protocol::OK) {
        return NOENT;
    }

    return OK;
}

yfs_client::status yfs_client::create(inum parent, const std::string &name, unsigned long &new_id) {
    ScopedExtentLock scopedExtentLock(parent, lc);

    // check if parent exists
    std::string dir_content;
    if (this->ec->get(parent, dir_content) != extent_protocol::OK) {
        return NOENT;
    }

    // create new empty file
    new_id = find_inum(parent, name);
    if (new_id == 0) {
        this->ec->get_next_id(0, new_id);
        // set file bit
        new_id |= 0x80000000;
        this->ec->put(new_id, "");

        // add file to parent directory
        dir_content += name + ',' + std::to_string(new_id) + ';';
        this->ec->put(parent, dir_content);
    }

    return OK;
}

yfs_client::status yfs_client::mkdir(inum parent, const std::string &name, unsigned long &new_id) {
    ScopedExtentLock scopedExtentLock(parent, lc);

    // check if parent exists
    std::string dir_content;
    if (this->ec->get(parent, dir_content) != extent_protocol::OK) {
        return NOENT;
    }

    // create new empty directory
    new_id = find_inum(parent, name);
    if (!new_id) {
        this->ec->get_next_id(0, new_id);
        // set least significant bit to zero for directory
        new_id &= ~0x80000000;
        this->ec->put(new_id, "");

        // add directory to parent directory
        dir_content += name + ',' + std::to_string(new_id) + ';';
        this->ec->put(parent, dir_content);
    }

    return OK;
}

yfs_client::status yfs_client::unlink(inum parent, std::string name) {
    ScopedExtentLock scopedDirLock(parent, lc);

    inum id = this->find_inum(parent, name);
    if (!id || !isfile(id)) {
        return NOENT;
    }

    ScopedExtentLock scopedFileLock(id, lc);

    // remove file from storage
    if (this->ec->remove(id) != extent_protocol::OK) {
        return NOENT;
    }

    // remove file from parent directory
    std::string dir_content{};
    if (this->ec->get(parent, dir_content) != extent_protocol::OK) {
        return NOENT;
    }

    std::string new_content;
    char *token = std::strtok(const_cast<char *>(dir_content.c_str()), ";");
    while (token) {
        std::string entry = std::string(token);
        size_t delimiter = entry.find(',');
        if (name != entry.substr(0, delimiter)) {
            new_content += entry + ';';
        }
        token = std::strtok(nullptr, ";");
    }

    this->ec->put(parent, new_content);

    return OK;
}

// precondition: caller holds lock of di
yfs_client::inum yfs_client::find_inum(inum di, const std::string &name) {
    if (!isdir(di)) {
        return 0;
    }

    std::string dir_content;
    if (this->ec->get(di, dir_content) != extent_protocol::OK) {
        return 0;
    }

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

yfs_client::inum yfs_client::lookup(inum di, std::string name) {
    ScopedExtentLock scopedExtentLock(di, lc);
    inum r = find_inum(di, name);
    return r;
}

std::vector<yfs_client::dirent> yfs_client::readdir(inum dir) {
    ScopedExtentLock scopedExtentLock(dir, lc);

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

yfs_client::status yfs_client::read(inum fi, size_t size, off_t offset, std::string &data) {
    ScopedExtentLock scopedExtentLock(fi, lc);

    if (size == 0) {
        return yfs_client::OK;
    }

    std::string content;
    if (this->ec->get(fi, content) != yfs_client::OK) {
        return yfs_client::NOENT;
    }

    if (static_cast<size_t>(offset) < content.size()) {
        data = content.substr(offset, size);
    }

    if (data.size() < size) {
        data.resize(size);
    }

    return yfs_client::OK;
}

yfs_client::status yfs_client::write(inum fi, std::string data, off_t offset) {
    ScopedExtentLock scopedExtentLock(fi, lc);

    std::string old_content;
    if (this->ec->get(fi, old_content) != extent_protocol::OK) {
        return NOENT;
    }

    std::string new_content = old_content;
    // fill potential hole with '\0'
    if (static_cast<size_t>(offset) > old_content.size()) {
        new_content.resize(offset);
    }
    // write actual data
    new_content.replace(offset, data.size(), data);

    this->ec->put(fi, new_content);

    return OK;
}
