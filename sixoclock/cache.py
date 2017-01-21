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

import os.path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sixoclock.file import Base, File

class Cache:
    def __init__(self):
        # TODO make db_uri configurable
        # TODO make cache_db_path configurable
        cache_db_path = os.path.expanduser(os.path.join('~', '.cache', 'sixoclockcache.db'))
        db_uri = 'sqlite:///{}'.format(cache_db_path)
        self.engine = create_engine(db_uri)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def add(self, file):
        session = self.Session()
        session.add(file)
        session.commit()
        session.close()

    def reset(self, mirror_uri):
        session = self.Session()
        session.query(File).filter(File.mirror_uri == mirror_uri).delete()
        session.commit()
        session.close()

    def get(self, uri, mirror_uri):
        session = self.Session()
        file = session.query(File).get((uri, mirror_uri))  # TODO kwargs?
        session.close()
        return file

    def query(self, *filters, **kwargs):
        session = self.Session()
        query = session.query(File).filter_by(**kwargs)
        query = query.filter(*filters)
        files = query.all()  # TODO page/limit for performance?
        session.close()
        return files
