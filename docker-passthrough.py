#!/usr/bin/env python

from __future__ import with_statement

import os, errno
import sys
import errno

from fuse import FUSE, FuseOSError, Operations

import docker

ignore_files = ['busybox-x86_64', '.dockerenv', 'etc', 'dev', 'proc', 'sys', 'run']

import os
# we use this for journal files.
def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)

class Cont():
    def __init__(self, container, partial, path):
        self.container = container
        self.partial = partial
        self.path = path

class Passthrough(Operations):
    def __init__(self, root):
        self.containers = dict()
        self.client = docker.from_env(version='auto')
        self.apiclient = docker.APIClient(base_url='unix://var/run/docker.sock', version='auto')
        self.container = self.client.containers.run("jeidtest/testfile", "/busybox-x86_64 sleep 100000", detach=True)
        inspect_dict = self.apiclient.inspect_container(self.container.id)
        print (inspect_dict)
        dev_name =  inspect_dict['GraphDriver']['Data']['DeviceName']
        print (dev_name)
        self.loc = dev_name[22:]
        path = "/var/lib/docker/devicemapper/mnt/"+self.loc+"/rootfs/"
        self.root = path


    def start_container(self, partial):
        print("start_container")
        try:
            container = self.client.containers.run("jeidtest/testfile/" + partial, "/busybox-x86_64 sleep 100000", detach=True)
            inspect_dict = self.apiclient.inspect_container(container.id)
            print (inspect_dict)
            dev_name =  inspect_dict['GraphDriver']['Data']['DeviceName']
            print (dev_name)
            loc = dev_name[22:]
            path = "/var/lib/docker/devicemapper/mnt/"+loc+"/rootfs/"
            cont = Cont(container, partial, path)
            self.containers[partial] = cont
            return path
        except docker.errors.NotFound:
            print("404!")
            return -errno.ENOENT

    # Helpers
    # =======

    def _full_path(self, partial):
        print("full path")
        print (partial)
        if partial == "/":
            path = os.path.join(self.root, partial)
            print("we're at root")
            print(self.root)
            print(path)
            print ("done")
            return self.root
        if partial.startswith("/"):
            partial = partial[1:]
        if partial in self.containers:
            cont = self.containers[partial]
            path = os.path.join(cont.path, partial)
            print(path)
            return path
        else:
            if os.path.exists(self.root + partial):
                if os.path.isfile(self.root + partial):
                    root = self.start_container(partial)
                    if root == -errno.ENOENT:
                        print("enoent")
                        return root
                    path = os.path.join(root, partial)
                    print(path)
                    return path
                else:
                    #we're a folder
                    print("Folder! " + self.root + partial)
                    path = os.path.join(self.root, partial)
                    return path
            else:
                print("not in journal so gonna enoent")
                return -errno.ENOENT

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        print("getattr " + path)
        full_path = self._full_path(path)
        if full_path == -errno.ENOENT:
            print("enoent in getattr")
            st = os.lstat("/aekjneqwlkjnqfwe")
            print("got to the return")
            return full_path
        try:
            st = os.lstat(full_path)
            ret = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                         'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
            print (ret)
            return ret
        except FileNotFoundError:
            print("we broke on devicemapper lookup")
            print(full_path)
            print(path)
            #partial = path[1:]
            #self.containers.pop(partial, None)
            print("popped, trying full_path again")
            #full_path = self._full_path(path)
            #self.getattr(path)

    def readdir(self, path, fh):
        print ("readdir " + path)
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            files = os.listdir(full_path)
            for dirent in files:
                if dirent in ignore_files:
                    print ("ignored " + dirent)
                else:
                    print ("didn't ignore" + dirent)
                    dirents.append(dirent)
            #dirents.extend(os.listdir(full_path))
        print ("dirents")
        print (dirents)
        for r in dirents:
            yield r

    def readlink(self, path):
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        print ("mknod")
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        print("Rmdir")
        #full_path = self._full_path(path)
        #return os.rmdir(full_path)
        print("deleting from journal")
        try:
            print(self.root + path)
            os.rmdir(self.root + path)
            self.container.commit("jeidtest/testfile")
            print("gonna push")
            self.client.images.push("jeidtest/testfile")
        except:
            print("failed to rmdir from journal")

    def mkdir(self, path, mode):
        print("Mkdir")
        print("mkdir path is " + path)
        os.mkdir(self.root + path, mode)
        self.container.commit("jeidtest/testfile")
        self.client.images.push("jeidtest/testfile")

    def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        print("Unlink")
        print("deleting from journal")
        try:
            os.unlink(self.root + path)
        except:
            print("failed to unlink from journal")
        self.container.commit("jeidtest/testfile")
        self.client.images.push("jeidtest/testfile")
        partial = path[1:]
        self.containers.pop(partial, None)
        print("popped from inmem")
        #return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        return os.symlink(target, self._full_path(name))

    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        return os.link(self._full_path(name), self._full_path(target))

    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        print ("open")
        full_path = self._full_path(path)
        #read_file = "/var/lib/docker/devicemapper/mnt/"+self.loc+"/rootfs/" + "testfile"
        return os.open(full_path, flags)
        #container = self.client.containers.run("jeidtest/testfile", "sleep 10000", detach=True)
        #self.client.copy(

    def create(self, path, mode, fi=None):
        print ("Create")
        print("create path is " + path)
        self.container.commit("jeidtest/testfile" + path)
        print("gonna push")
        self.client.images.push("jeidtest/testfile" + path)
        touch(self.root + path)
        self.container.commit("jeidtest/testfile")
        self.client.images.push("jeidtest/testfile")
        print("created a fake inode")
        full_path = self._full_path(path)
        print (full_path)
        #print (os.path.isfile("/var/lib/docker/devicemapper/mnt/"+self.loc+"/rootfs/" + "testfile"))
        #read_file = "/var/lib/docker/devicemapper/mnt/"+self.loc+"/rootfs/" + "testfile-inside"
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        print("truncate")
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        print ("Release")
        print(path)
        if path.startswith("/"):
            path = path[1:]
        if path in self.containers:
            cont = self.containers[path]
            ret = os.close(fh)
            cont.container.commit("jeidtest/testfile/"+path)
            self.client.images.push("jeidtest/testfile" + path)
            print("path was in self.containers")
            return ret
        else:
            print("uhoh")
            ret = os.close(fh)
            self.container.commit("jeidtest/testfile")
            self.client.images.push("jeidtest/testfile")
            print("probably errored")
            return ret

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)


def main(mountpoint, root):
    FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
