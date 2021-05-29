#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>


class yfs_client {
    extent_client *ec;
public:

    typedef unsigned long long inum;
    enum xxstatus {
        OK, RPCERR, NOENT, IOERR, FBIG
    };
    typedef int status;

    struct fileinfo {
        unsigned long long size;
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    };
    struct dirinfo {
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    };
    struct dirent {
        std::string name;
        unsigned long long inum;
    };

private:
    static std::string filename(inum);

    static inum n2i(std::string);

public:

    yfs_client(std::string, std::string);

    bool isfile(inum);

    bool isdir(inum);

    yfs_client::status create(inum parent, const std::string &name, unsigned long &);

    inum lookup(inum di, std::string name);

    std::vector<yfs_client::dirent> readdir(inum dir);

    int getfile(inum, fileinfo &);

    int getdir(inum, dirinfo &);

    yfs_client::status setattr(inum id, unsigned long value, char which);

    yfs_client::status read(inum, size_t, off_t, std::string &);

    yfs_client::status write(inum, std::string, off_t);
};

#endif 
