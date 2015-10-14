#!/usr/bin/python
# needs shoutout to a couple of gits...
import os
import time
import argparse
from errno import EFAULT, ENOENT, EPERM
from bioblend import galaxy
from bioblend.galaxy import objects
from bioblend.galaxy.client import ConnectionError
from stat import S_IFDIR, S_IFREG
from fuse import Operations
from fuse import FUSE, FuseOSError

import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger()


class GFSObject(Operations):

    def __init__(self, gfs):
        self.gfs = gfs

    def _id_from_path(self, path_component):
        return path_component[path_component.rfind('[')+1:path_component.rfind(']')]


class GFSManager(Operations):
    context = None  # should be abstract to force sub-class implementation

    def delegate(self, op, *args):

        boundObj = self._path_bound(args[0])
        print '{}.{} [{}]'.format(boundObj.__class__.__name__, op, args)

        if not hasattr(boundObj, op):
            raise FuseOSError(EFAULT)

        return getattr(boundObj, op)(*args)


class GalaxyFS(Operations):

    def __init__(self, galaxy_url, api_key):
        self.gi = objects.GalaxyInstance(url=galaxy_url, api_key=api_key)
        self.pgi = galaxy.GalaxyInstance(url=galaxy_url, key=api_key)
        self.root = RootDirectory()

        # TODO: for all subclasses of GFSObject register path, module grab
        # classes inspect inheritance, b00m
        self.path_bindings = {
            RootDirectory.context: self.root,
            HistoryManager.context: HistoryManager(self),
            LibraryManager.context: LibraryManager(self),
        }

    def _path_bound(self, path):
        pbits = path.split(os.path.sep)

        if len(pbits) == 1:  # ['']/
            bind = self.path_bindings.get(path)
        elif len(pbits) > 1:  # e.g. ['']/['histories']/['Unnamed History [8997977]']
            bind = self.path_bindings.get(pbits[1], False)
            if not bind:  # i.e. top level directory couldn't match
                bind = self.root  # delegate handling to root directory

        return bind

    def __call__(self, op, *args):
        print '{} -> {} ::'.format(op, args[0]),
        boundObj = self._path_bound(args[0])
        return boundObj.delegate(op, *args)


class Directory():
    # should subclass file? or fusepy object?

    def readdir(self, path=None, fh=None):
        return ['.', '..']

    def getattr(self, path=None, fh=None):

        st = dict(st_mode=(S_IFDIR | 0700), st_nlink=2)  # TODO: nlinks
        # do we want actual create times?
        st['st_ctime'] = st['st_mtime'] = st['st_atime'] = time.time()
        st['st_uid'] = os.geteuid()
        st['st_gid'] = os.getegid()
        return st

    def _format_path(self, iterable):
        return ["{0.name} [{0.id}]".format(x) for x in iterable]

    def _format_path_plain(self, iterable):
        return ["{name} [{id}]".format(**x) for x in iterable if not x['deleted']]


class File():

    def getattr(self, path=None, fh=None):
        st = dict(st_mode=(S_IFREG | 0400), st_nlink=2)  # TODO: nlinks
        # do we want actual create times?
        st['st_ctime'] = st['st_mtime'] = st['st_atime'] = time.time()
        st['st_uid'] = os.geteuid()
        st['st_gid'] = os.getegid()
        return st


class RootDirectory(Directory, GFSManager):
    context = '/'
    tlds = ['histories', 'libraries']  # , 'tools', 'workflows']

    def _path_bound(self, path):
        return self

    def getattr(self, path=None, fh=None):

        if path == '/':
            st = dict(st_mode=(S_IFDIR | 0500), st_nlink=2)  # TODO: nlinks
            # do we want actual create times?
            st['st_ctime'] = st['st_mtime'] = st['st_atime'] = time.time()
            st['st_uid'] = os.geteuid()
            st['st_gid'] = os.getegid()
            return st
        else:
            raise FuseOSError(ENOENT)

    def readdir(self, path=None, fh=None):
        return RootDirectory.tlds + super(RootDirectory, self).readdir()


class HistoryManager(GFSObject, GFSManager):
    context = 'histories'

    def __init__(self, gfs):
        super(HistoryManager, self).__init__(gfs)
        self.transactionMap = {}

    def _path_bound(self, path):
        if path in self.transactionMap:
            wd = self.transactionMap.get(path).split(os.path.sep)
            del self.transactionMap[path]
        else:
            wd = path.split(os.path.sep)
        if len(wd) == 2:  # == /histories -> Histories
            return Histories(self)
        elif len(wd) == 3:  # == /histories/Unnamed History [8997977]/ -> History
            hist_id = self._id_from_path(wd[2])
            try:
                return History(hist_id, self.gfs)
            except ConnectionError:
                return Histories(self)
        elif len(wd) == 4:  # == /histories/Unnamed History [8997977]/Pasted Entry [5969b1f7201f12ae] -> HistoryDataset
            hist_id = self._id_from_path(wd[2])
            dataset_id = self._id_from_path(wd[3])
            return History(hist_id, self.gfs).getDataset(dataset_id)


class Histories(Directory, GFSObject):

    def __init__(self, manager):
        self.manager = manager
        super(Histories, self).__init__(manager.gfs)

    def getattr(self, path=None, fh=None):
        if path == '/histories':
            return super(Histories, self).getattr(path, fh)
        else:
            raise FuseOSError(ENOENT)

    def readdir(self, path=None, fh=None):
        return [x.name+' ['+x.id+']' for x in self.gfs.gi.histories.list()]+super(Histories, self).readdir()

    def mkdir(self, path=None, mode=None):
        new_hist = self.gfs.gi.histories.create(path[path.rfind(os.path.sep)+1:])
        self.manager.transactionMap[path] = '/histories/{} [{}]'.format(new_hist.name, new_hist.id)


class History(Directory, GFSObject):

    def __init__(self, hist_id, gfs):
        super(History, self).__init__(gfs)
        self.hist = self.gfs.gi.histories.get(hist_id)

    def rmdir(self, path):
        self.gfs.gi.histories.delete(self.hist.id)

    def rename(self, old, new):
        path = new[:new.rfind(os.path.sep)]

        if path != '/histories':
            raise FuseOSError(EPERM)
        name = new[new.rfind(os.path.sep)+1:]
        self.hist.update(name=name)

    def readdir(self, path=None, fh=None):
        return [
            '{}. {} [{}]'.format(x.wrapped['hid'], x.name, x.id)
            for x in self.hist.content_infos if not x.deleted
        ] + super(History, self).readdir()

    def getDataset(self, dataset_id):
        return HistoryDataset(self.hist, dataset_id, self.gfs)


class HistoryDataset(File, GFSObject):

    def __init__(self, hist, dataset_id, gfs):
        super(HistoryDataset, self).__init__(gfs)
        self.dataset = hist.get_dataset(dataset_id)

    def unlink(self, path):
        self.dataset.delete()


class LibraryManager(GFSObject, GFSManager):
    context = 'libraries'

    def __init__(self, gfs):
        super(LibraryManager, self).__init__(gfs)
        self.transactionMap = {}

    def _path_bound(self, path):
        if path in self.transactionMap:
            wd = self.transactionMap.get(path).split(os.path.sep)
            del self.transactionMap[path]
        else:
            wd = path.split(os.path.sep)

        if len(wd) == 2:  # == /libraries -> Libraries
            return Libraries(self)
        elif len(wd) >= 3:  # == /libraries/Unnamed Library [8997977]/ -> Library,
                            # /libraries/Unnamed Library [8997977]/* -> Folder, Datasets
            lib_id = self._id_from_path(wd[2])
            try:
                return Library(lib_id, self.gfs)
            except ConnectionError:
                return Libraries(self)


class Libraries(Directory, GFSObject):

    def __init__(self, manager):
        self.manager = manager
        super(Libraries, self).__init__(manager.gfs)

    def getattr(self, path=None, fh=None):
        if path == '/libraries':
            return super(Libraries, self).getattr(path, fh)
        else:
            raise FuseOSError(ENOENT)

    def readdir(self, path=None, fh=None):
        # perms? Maybe this is what .list() provides
        libs = self.gfs.pgi.libraries.get_libraries()
        return ['.', '..'] + self._format_path_plain(libs)
        # return ['.', '..'] + self._format_path(self.gfs.gi.libraries.list())

    def mkdir(self, path=None, mode=None):
        new_lib = self.gfs.gi.libraries.create(path[path.rfind(os.path.sep)+1:])
        self.manager.transactionMap[path] = '/libraries/{0.name} [{0.id}]'.format(new_lib)


class Library(Directory, GFSObject):

    def __init__(self, lib_id, gfs, folder_path=None):
        super(Library, self).__init__(gfs)
        self.lib = self.gfs.gi.libraries.get(lib_id)
        self.folder_path = folder_path

    def rmdir(self, path):
        """rmdir called on parent library object"""
        self.gfs.gi.libraries.delete(self.lib.id)

    def rename(self, old, new):
        path = new[:new.rfind(os.path.sep)]

        if path != '/libraries':
            raise FuseOSError(EPERM)
        name = new[new.rfind(os.path.sep)+1:]
        self.lib.update(name=name)

    def readdir(self, path=None, fh=None):
        # Definitely cache this one.
        library_contents = self.gfs.pgi.libraries.show_library(self.lib.id, contents=True)
        wd = path.split(os.path.sep)
        fp = wd[3:]

        print 'FP: ', fp
        interested_paths = []

        for thing in library_contents[1:]:
            thing_path = thing['name'][1:].split(os.path.sep)

            # If the fp, the user's requested subpath in this library is shared
            # with the given library path item
            if fp == thing_path[0:len(fp)]:
                # If it's one level beyond that, so everything in that folder
                if len(thing_path) == len(fp) + 1:
                    if thing['type'] == 'file':
                        interested_paths.append(LibraryDataset(self.lib, thing['id'], self.gfs))
                    else:
                        interested_paths.append(LibraryFolder(self.lib, thing['id'], self.gfs, self))

        return interested_paths + super(Library, self).readdir()


class LibraryFolder(Directory, GFSObject):

    def __init__(self, lib, folder_id, gfs, library):
        super(LibraryFolder, self).__init__(gfs)
        # self.lib = self.gfs.gi.libraries.get(lib_id)
        self.lib = lib
        self.folder_id = folder_id
        self.library = library

    def rmdir(self, path):
        self.gfs.pgi.folders.delete_folder(self.folder_id)

    def rename(self, old, new):
        return

    def readdir(self, path=None, fh=None):
        results = self.library.readdir(path=path)
        print results
        return results


class LibraryDataset(File, GFSObject):

    def __init__(self, lib, dataset_id, gfs):
        super(LibraryDataset, self).__init__(gfs)
        self.dataset = lib.get_dataset(dataset_id)

    def unlink(self, path):
        self.dataset.delete()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Provides FUSE bindings for a Galaxy instance")
    parser.add_argument('galaxy_url', help='URL:PORT of Galaxy instance')
    parser.add_argument('api_key', help='API key for Galaxy user')
    parser.add_argument('-m', '--mountpoint', default='gfs', help='Mount bound Galaxy instance here')
    args = parser.parse_args()

    if not os.path.exists(args.mountpoint):
        os.makedirs(args.mountpoint)

    fuse = FUSE(GalaxyFS(args.galaxy_url, args.api_key), args.mountpoint, foreground=True, nothreads=True, ro=False)
