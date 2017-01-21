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
from urllib.parse import urljoin, urlparse
from urllib.request import pathname2url, url2pathname
import os.path

from sixoclock.file import File

class Backend(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def default_name(cls):
        '''Default name of the backend.'''
        pass

    @property
    @abstractmethod
    def list(self, uri, recurse):
        '''Return a list of URIs of the files contained in a URI'''
        pass

    @abstractmethod
    def get(self, uri):
        '''Get the File instance at this URI.'''
        pass

    @abstractmethod
    def put(self, uri, stream, mtime=None):
        '''Put the File instance into the backend.

        `uri` should be the intended absolute URI of the file.
        Should set mtime if possible and provided.
        '''
        pass

    @abstractmethod
    def delete(self, uri):
        '''Delete the file instance contained at the URI.'''
        pass

    @abstractmethod
    def stream(self, uri):
        '''Return a stream for reading the given URI.'''
        pass

    def mtime(self, uri):
        '''Return the time the file was last modified or None if unsupported.'''
        return None

    def determine_path(self, uri):
        '''Return the absolute path of the given URI.'''
        path = url2pathname(urlparse(uri).path)
        return os.path.abspath(path)

    def full_uri(self, base_uri, relative_path):
        '''Construct the uri for a path relative to the base URI.'''
        if not base_uri.endswith('/'):
            base_uri += '/'
        path = pathname2url(relative_path)
        return urljoin(base_uri, path)
