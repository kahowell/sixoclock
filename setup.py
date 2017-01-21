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

from setuptools import setup

def readme():
    with open('README.rst') as f:
        return f.read()

setup(
    name='sixoclock',
    version='0.1.0',
    description='Simple personal backup tool',
    long_description=readme(),
    url='https://github.com/kahowell/sixoclock',
    author='Kevin Howell',
    author_email='kevin@kahowell.net',
    license='GPLv3',
    packages=['sixoclock'],
    install_requires=[
        'SQLAlchemy',
        'humanize',
    ],
    scripts=['bin/sixoclock'],
)
