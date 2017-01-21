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

from hashlib import md5, sha1, sha256

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime

Base = declarative_base()

class File(Base):
    __tablename__ = 'file'

    uri = Column(String, primary_key=True)
    mirror_uri = Column(String, primary_key=True)
    md5 = Column(String, index=True)
    sha1 = Column(String, index=True)
    sha256 = Column(String, index=True)
    path = Column(String, index=True)
    size = Column(Integer)
    mtime = Column(Integer)

    @classmethod
    def from_stream(cls, stream):
        _md5 = md5()
        _sha1 = sha1()
        _sha256 = sha256()
        block_size = min([_md5.block_size, _sha1.block_size, _sha256.block_size])
        total_size = 0
        while True:
            block = stream.read(block_size)
            total_size += len(block)
            if len(block) == 0:
                break
            _md5.update(block)
            _sha1.update(block)
            _sha256.update(block)
        file = cls()
        file.md5 = _md5.hexdigest()
        file.sha1 = _sha1.hexdigest()
        file.sha256 = _sha256.hexdigest()
        file.size = total_size
        return file

    def copy_metadata(self, other):
        self.md5 = other.md5
        self.sha1 = other.sha1
        self.sha256 = other.sha256
        self.size = other.size
        self.mtime = other.mtime

    def __repr__(self):
        return "<File(uri='{}')>".format(self.uri)

    def __str__(self):
        return self.uri
