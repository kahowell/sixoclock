# Copyright 2017 Kevin Howell
#
# This file is part of sixoclock.
#
# sixoclock is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# sixoclock is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with sixoclock.  If not, see <http://www.gnu.org/licenses/>.

from abc import ABCMeta, abstractmethod
import logging
import os.path

from sixoclock.file import File

class Action(metaclass=ABCMeta):
    def __init__(self, source, file, destination, cache):
        self.source = source
        self.file = file
        self.destination = destination
        self.cache = cache

    @property
    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def execute(self):
        pass

class BackupAction(Action):
    @property
    def name(self):
        return 'backup'

    def execute(self):
        with self.source.stream(self.file.uri) as stream:
            logging.info('Storing {} in {}'.format(self.file, self.destination))
            relative_path = self.file.path
            destination_uri = self.destination.uri
            if not destination_uri.endswith('/'):
                destination_uri += '/'
            new_file = File()
            new_file.uri = self.destination.full_uri(self.destination.uri, relative_path)
            new_file.path = relative_path
            self.destination.put(new_file.uri, stream, mtime=self.file.mtime)
            new_file.mirror_uri = self.destination.uri
            new_file.copy_metadata(self.file)
            self.cache.add(new_file)

class WarnAction(Action):
    @property
    def name(self):
        return 'log'

    def execute(self):
        logging.warning('{} differs between source {} and destination {}!'.format(self.file,self.source, self.destination))

UPDATE_STRATEGIES = {
    'log_warning': WarnAction,
    'update_regardless': BackupAction,
}

class Collection:
    def __init__(self, name, sources, mirrors, minimum_copies=None, update_strategy=None, cache=None):
        self.minimum_copies = minimum_copies or 1
        update_strategy = update_strategy or 'log_warning'
        self.update_strategy = UPDATE_STRATEGIES[update_strategy]
        self.sources = sources
        self.mirrors = mirrors
        self.cache = cache

    def query(self, *args, **kwargs):
        return self.cache.query(*args, **kwargs)

    def refresh_cache(self, mirror=None, reset=False):
        if reset:
            self.cache.reset(self.name)
        all_mirrors = self.sources + self.mirrors
        if mirror:
            mirror_list = [mirror for mirror in all_mirrors if mirror.uri == mirror]
        else:
            mirror_list = all_mirrors
        for mirror in mirror_list:
            for uri in mirror.list():
                cached_file = self.cache.get(uri, mirror.uri)  # TODO kwargs?
                if (cached_file is None) or (mirror.backend.mtime(uri) is not None and mirror.backend.mtime(uri) > cached_file.mtime):
                    file = mirror.get(uri)
                    self.cache.add(file)

    def backup(self, dry_run=False):
        self.refresh_cache()  # TODO be smarter about when to refresh
        backup_actions = []
        for source in self.sources:
            for file in self.query(File.mirror_uri == source.uri):
                copies = self.query(File.mirror_uri != source.uri, File.path == file.path, File.sha1 == file.sha1)
                mirrors_with_copy = set([copy.mirror_uri for copy in copies if copy.mirror_uri in self.mirrors])
                mirrors_without_copy = set(self.mirrors) - mirrors_with_copy
                mirrors_without_copy_list = list(mirrors_without_copy)
                for copy_number in range(self.minimum_copies - len(copies)):
                    # TODO different strategies for mirror selection
                    destination = mirrors_without_copy_list[copy_number]
                    existing = self.query(File.mirror_uri == destination.uri, File.path == file.path)
                    if existing:
                        backup_action = self.update_strategy(source, file, destination, self.cache)
                    else:
                        backup_action = BackupAction(source, file, destination, self.cache)
                    backup_actions.append(backup_action)
                    if not dry_run:
                        backup_action.execute()
        return backup_actions

    def stats(self):
        self.refresh_cache()  # TODO be smarter about when to refresh
        total_source_size = 0
        backed_up_size = 0
        source_file_count = 0
        for mirror in self.sources:  # TODO do this in SQL instead of objects
            for file in self.query(File.mirror_uri == mirror.uri):
                total_source_size += file.size
                copies = self.query(File.mirror_uri != mirror.uri, File.path == file.path, File.sha1 == file.sha1)
                copies = [copy for copy in copies if copy.mirror_uri in self.mirrors]
                if len(copies) > 0:
                    backed_up_size += file.size
                source_file_count += 1
        percentage = 100.0
        if total_source_size > 0:
            percentage = (backed_up_size / total_source_size) * 100.0
        return CollectionStats(size=total_source_size, backed_up_size=backed_up_size, source_file_count=source_file_count)

class CollectionStats:
    def __init__(self, size, backed_up_size, source_file_count):
        self.size = size
        self.backed_up_size = backed_up_size
        self.source_file_count = source_file_count

class Mirror:
    def __init__(self, backend, uri):
        self.backend = backend
        self.uri = uri

    def list(self, recursive=True):
        return self.backend.list(self.uri, recursive)

    def get(self, uri):
        file = self.backend.get(uri)
        file.mirror_uri = self.uri
        full_path = self.backend.determine_path(uri)
        mirror_path = self.backend.determine_path(self.uri)
        file.path = os.path.relpath(full_path, mirror_path)
        return file

    def stream(self, uri):
        return self.backend.stream(uri)

    def full_uri(self, base_uri, path):
        return self.backend.full_uri(base_uri, path)

    def put(self, uri, stream, mtime=None):
        self.backend.put(uri, stream, mtime)

    def __repr__(self):
        return "<Mirror(uri='{}')>".format(self.uri)

    def __str__(self):
        return self.uri
