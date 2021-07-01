// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <utility>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst) {
    sockaddr_in dstsock;
    make_sockaddr(dst.c_str(), &dstsock);
    cl = new rpcc(dstsock);
    if (cl->bind() != 0) {
        printf("extent_client: bind failed\n");
    }

    assert(pthread_mutex_init(&this->storage_lock, nullptr) == 0);
}

extent_client::~extent_client() {
    assert(pthread_mutex_destroy(&this->storage_lock) == 0);
}

extent_protocol::status extent_client::put(extent_protocol::extentid_t eid, std::string buf) {
    ScopedLock scopedLock(&this->storage_lock);

    Extent extent;
    if (this->storage.find(eid) != this->storage.end() && this->storage.at(eid).present) {
        extent = this->storage.at(eid);

        // update metadata
        time_t current_time = std::time(nullptr);
        extent.metadata.mtime = current_time;
        extent.metadata.ctime = current_time;
    } else {
        extent.is_new = true;
    }
    extent.content = buf;
    extent.metadata.size = buf.size();
    extent.dirty = true;
    this->storage[eid] = extent;
    return extent_protocol::OK;
}

extent_protocol::status extent_client::get(extent_protocol::extentid_t eid, std::string &buf) {
    ScopedLock sl(&this->storage_lock);
    extent_protocol::status ret = extent_protocol::OK;

    if (this->storage.find(eid) != this->storage.end() && this->storage.at(eid).present) {
        // update access time
        Extent &extent = this->storage.at(eid);
        extent.metadata.atime = std::time(nullptr);
        extent.dirty = true;

        buf = extent.content;
    } else {
        Extent extent;
        ret = cl->call(extent_protocol::get, eid, extent.content);
        if (ret != extent_protocol::OK) {
            return extent_protocol::NOENT;
        }
        ret = cl->call(extent_protocol::getattr, eid, extent.metadata);
        if (ret != extent_protocol::OK) {
            return extent_protocol::NOENT;
        }
        this->storage.emplace(eid, extent);
    }
    return ret;
}

extent_protocol::status extent_client::getattr(extent_protocol::extentid_t eid, extent_protocol::attr &attr) {
    ScopedLock scopedLock(&this->storage_lock);
    extent_protocol::status ret = extent_protocol::OK;

    if (this->storage.find(eid) == this->storage.end() || !this->storage.at(eid).present) {
        Extent extent;
        ret = cl->call(extent_protocol::getattr, eid, extent.metadata);
        if (ret != extent_protocol::OK) {
            return extent_protocol::NOENT;
        }
        ret = cl->call(extent_protocol::get, eid, extent.content);
        if (ret != extent_protocol::OK) {
            return extent_protocol::NOENT;
        }
        this->storage.emplace(eid, extent);
    }

    const extent_protocol::attr &data = this->storage.at(eid).metadata;
    attr = data;
    return ret;
}

extent_protocol::status extent_client::setattr(extent_protocol::extentid_t eid, extent_protocol::attr attr) {
    ScopedLock scopedLock(&this->storage_lock);

    if (this->storage.find(eid) != this->storage.end() && this->storage.at(eid).present) {
        Extent &extent = this->storage.at(eid);
        extent.content.resize(attr.size);
        extent.metadata = attr;
        extent.dirty = true;
        return extent_protocol::OK;
    }
    return extent_protocol::NOENT;
}

extent_protocol::status extent_client::remove(extent_protocol::extentid_t eid) {
    ScopedLock scopedLock(&this->storage_lock);

    //TODO: on synchronization, only sync remove if extent was not new
    //TODO: on synchronization, remove from storage if not present

    if (this->storage.find(eid) != this->storage.end() && this->storage.at(eid).present) {
        Extent &extent = this->storage.at(eid);
        extent.present = false;
    }
    return extent_protocol::OK;
}

extent_protocol::status extent_client::get_next_id(extent_protocol::extentid_t id, unsigned long &res) {
    return cl->call(extent_protocol::get_next_id, id, res);
}
