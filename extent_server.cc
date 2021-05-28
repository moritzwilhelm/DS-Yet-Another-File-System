// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "rpc/slock.h"


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &) {
    ScopedLock scoped_sl(&this->storage_lock);

    Extent extent;
    if (this->storage.find(id) != this->storage.end()) {
        extent = this->storage.at(id);
        if (extent.content != buf) {
            unsigned int current_time = std::time(nullptr);
            extent.metadata.mtime = current_time;
            extent.metadata.ctime = current_time;
        }
    }
    extent.content = buf;
    extent.metadata.size = buf.size();
    this->storage[id] = extent;
    return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf) {
    ScopedLock scoped_sl(&this->storage_lock);

    if (this->storage.find(id) != this->storage.end()) {
        Extent extent = this->storage.at(id);

        // update access time
        extent.metadata.atime = std::time(nullptr);

        buf = extent.content;
        return extent_protocol::OK;
    }
    return extent_protocol::NOENT;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a) {
    ScopedLock scoped_sl(&this->storage_lock);

    if (this->storage.find(id) != this->storage.end()) {
        const extent_protocol::attr &data = this->storage.at(id).metadata;
        a.size = data.size;
        a.atime = data.atime;
        a.mtime = data.mtime;
        a.ctime = data.ctime;
        return extent_protocol::OK;
    }
    return extent_protocol::NOENT;
}

int extent_server::remove(extent_protocol::extentid_t id, int &) {
    ScopedLock scoped_sl(&this->storage_lock);

    if (this->storage.find(id) != this->storage.end()) {
        this->storage.erase(id);
        return extent_protocol::OK;
    }
    return extent_protocol::NOENT;
}

int extent_server::get_next_id(extent_protocol::extentid_t id, unsigned long &res) {
    ScopedLock scoped_sl(&this->storage_lock);

    // prevent reuse of id 0 or 1 after overflow
    if (this->next_id < 2) {
        this->next_id = 2;
    }
    res = this->next_id++;
    return extent_protocol::OK;
}
