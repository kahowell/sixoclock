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

import os
import os.path

from sixoclock.backend import Backend
from sixoclock.file import File

class FileBackend(Backend):
    @classmethod
    def default_name(cls):
        return 'file'

    def __init__(self, buffer_size = None):
        self.buffer_size = buffer_size or 67108864  # 64K

    def list(self, uri, recurse):
        base_uri = uri
        base_path = self.determine_path(uri)
        all_urls = []
        for dirpath, _, filenames in os.walk(base_path):
            for filename in filenames:
                path = os.path.join(dirpath, filename)
                relative_path = os.path.relpath(path, base_path)
                yield self.full_uri(base_uri, relative_path)
            if not recurse:
                return

    def get(self, uri):
        path = self.determine_path(uri)
        with open(path, 'rb') as stream:
            file = File.from_stream(stream)
        file.uri = uri
        file.mtime = self.mtime(uri)
        return file

    def put(self, uri, stream, mtime=None):
        path = self.determine_path(uri)
        base_dir = os.path.dirname(path)
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
        with open(path, 'wb') as output:
            while True:
                buffer = stream.read(self.buffer_size)
                if len(buffer) == 0:
                    break
                output.write(buffer)
        if mtime:
            os.utime(path, (mtime, mtime))

    def delete(self, uri):
        path = self.determine_path(uri)
        os.remove(path)

    def stream(self, uri):
        path = self.determine_path(uri)
        return open(path, 'rb')

    def mtime(self, uri):
        path = self.determine_path(uri)
        return int(os.path.getmtime(path))
