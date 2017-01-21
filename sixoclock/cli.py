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

import argparse
import humanize
import logging
import os.path
import time

from sixoclock.config import Configuration
from sixoclock.backends.file import FileBackend
from sixoclock.file import File

class Cli:
    def __init__(self):
        parser = argparse.ArgumentParser(description='Simple personal backups.')
        parser.add_argument('--config', help='config file to use')
        parser.add_argument('--no-log', action='store_true', help='log file')
        parser.add_argument('--log-file', help='log file')
        parser.set_defaults(function=self.print_usage, config=os.path.expanduser(os.path.join('~', '.sixoclock.yml')), log_file=None)
        subparsers = parser.add_subparsers()

        backup_parser = subparsers.add_parser('backup', help='perform a backup')
        backup_parser.add_argument('-c', '--collection', help='backup a specific collection')
        backup_parser.add_argument('--dry-run', action='store_true', help='do not backup, show what would happen')
        backup_parser.set_defaults(function=self.backup)

        query_parser = subparsers.add_parser('query', help='find a file in configured sources or mirrors')
        query_parser.add_argument('-c', '--collection', help='look only in a specific collection')
        query_parser.add_argument('-m', '--mirror', help='look only in a specific mirror')
        query_parser.add_argument('--path', help='relative path of the file')
        query_parser.add_argument('--filename', help='base filename (ex. foo.txt)')
        query_parser.add_argument('--file', help='file to use as a basis')
        query_parser.add_argument('--md5', help='md5 hash')
        query_parser.add_argument('--sha1', help='sha1 hash')
        query_parser.add_argument('--sha256', help='sha256 hash')
        query_parser.add_argument('--size', help='file size in bytes')
        query_parser.set_defaults(function=self.query)

        status_parser = subparsers.add_parser('status', help='show backup status')
        status_parser.add_argument('-c', '--collection', help='show status of a specific collection')
        status_parser.set_defaults(function=self.status)

        refresh_parser = subparsers.add_parser('refresh-cache', help='refresh cache')
        refresh_parser.add_argument('-c', '--collection', help='refresh mirror caches for a specific collection')
        refresh_parser.add_argument('-m', '--mirror', help='refresh mirror caches for a specific mirror')
        refresh_parser.add_argument('--rebuild', action='store_true', help='remove entries and rebuild the cache')
        refresh_parser.set_defaults(function=self.refresh_cache)

        self.parser = parser

    def main(self):
        args = self.parser.parse_args()
        log_filename = args.log_file or 'sixoclock.{}.log'.format(int(time.time()))
        if not args.no_log:
            logging.basicConfig(filename=log_filename, level=logging.INFO)
        self.configuration = Configuration(args.config)
        args.function(args)

    def print_usage(self, args):
        self.parser.print_usage()

    def backup(self, args):
        for name, collection in self.configuration.collections.items():
            if args.collection and name != collection:
                continue
            print('Backing up collection: {}'.format(name))
            actions = collection.backup(args.dry_run)
            if args.dry_run:
                for action in actions:
                    print('Would back up {} to {}'.format(action.file, action.destination))

    def query(self, args):
        filters = []
        if args.path:
            filters.append(File.path == args.path)
        if args.file:
            filebackend = FileBackend()
            file = filebackend.get(args.file)
            filters.append(File.sha1 == file.sha1)
            filters.append(File.path.like('%/{}'.format(os.path.basename(args.file))))
        if args.filename:
            filters.append(File.path.like('%/{}'.format(args.filename)))
        if args.md5:
            filters.append(File.md5 == args.md5)
        if args.sha1:
            filters.append(File.sha1 == args.sha1)
        if args.sha256:
            filters.append(File.sha256 == args.sha256)
        if args.size:
            filters.append(File.size == args.size)

        collections = self.configuration.collections.values()
        if args.collection:
            collections = [self.configuration.collections[args.collection]]
        if args.mirror:
            filters.append(File.mirror_uri == args.mirror)

        for collection in collections:
            collection.refresh_cache()
            for match in collection.query(*filters):
                print('Match: {}'.format(match.uri))

    def status(self, args):
        for name, collection in self.configuration.collections.items():
            if args.collection and name != args.collection:
                continue
            print('Collection: {}'.format(name))
            stats = collection.stats()
            print('  # Source files: {}'.format(stats.source_file_count))
            size = humanize.naturalsize(stats.size)
            percentage = 100.0
            if stats.size > 0:
                percentage = stats.backed_up_size / stats.size
            print('  Total size: {}, {}% backed up'.format(size, percentage))

    def refresh_cache(self, args):
        for name, collection in self.configuration.collections.items():
            if args.collection and name != args.collection:
                continue
            collection.refresh_cache(mirror=args.mirror, reset=args.refresh)
