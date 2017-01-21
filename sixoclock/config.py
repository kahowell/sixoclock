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

from urllib.parse import urlparse
import hashlib
import inspect
import os.path

import yaml

from sixoclock.backend import Backend
from sixoclock.cache import Cache
from sixoclock.collection import Collection
from sixoclock.collection import Mirror
import sixoclock.backends

class Configuration:
    def __init__(self, config_path):
        with open(config_path, 'r') as config_file:
            config = yaml.load(config_file)
        self.cache = Cache()
        self.backends = self.config_backends(config.get('backends') or {})
        self.collections = self.config_collections(config['collections'])

    @staticmethod
    def config_backends(config_data):
        backends = {}
        for name, backend_config in config_data.items():
            backends[key] = self.config_backend(name, backend_config)
        for item in dir(sixoclock.backends):
            if not item.startswith('_'):
                backend = getattr(sixoclock.backends, item)
                if inspect.isclass(backend) and backend.default_name() not in backends:
                    backends[backend.default_name()] = backend()
        return backends

    @staticmethod
    def config_backend(name, config):
        backend_class_name = config.get('backend') or name
        backend_class = getattr(sixoclock.backends, backend_class_name)
        if 'backend' in config:
            del config['backend']
        return backend_class(**config)

    def config_collections(self, collections_data):
        collections = {}
        for name, collection_config in collections_data.items():
            collections[name] = self.config_collection(name, collection_config)
        return collections

    def config_collection(self, name, config):
        sources_config = config['sources']
        mirrors_config = config['mirrors']
        del config['sources']
        del config['mirrors']
        sources = self.config_sources(sources_config)
        mirrors = self.config_mirrors(mirrors_config)
        return Collection(name, sources=sources, mirrors=mirrors, cache=self.cache, **config)

    def config_sources(self, uris):
        return [self.config_mirror(uri) for uri in uris]

    def config_mirrors(self, uris):
        return [self.config_mirror(uri) for uri in uris]

    def config_mirror(self, uri):
        parsed = urlparse(uri)
        backend = self.backends[parsed.scheme]
        return Mirror(backend, uri)
