=========
Sixoclock
=========

Sixoclock is a project to create a simple, yet effective personal backup tool.

Sixoclock is implemented in Python 3 and licensed under GPLv3.

The goal of sixoclock is to enable automatic duplication of specific
collections of files to one-or-more mirrors.

Backends have a very simple interface, enabling developers to easily add new
ones.

------------
Installation
------------

Install from pypi:

::

    pip install sixoclock

-----
Usage
-----

In order to use ``sixoclock``, you should first decide how you wish to backup
your files. Then, create a config file to express your plan to ``sixoclock``.

The default config location is ``$HOME/.sixoclock.yml``, but this can be
overriden on a per-command basis by using the ``--config`` option.

``sixoclock`` uses YAML as the configuration language. TODO: example configs

Once configured ``sixoclock backup`` will start the backup process.

``sixoclock`` offers functionality in subcommands:

::

    backup              perform a backup
    query               find a file in configured sources or mirrors
    status              show backup status
    refresh-cache       refresh cache

See ``sixoclock --help`` for more details.

File Backend
============

The file backend uses the very familiar ``file://`` URI protocol. There are
currently no special options for the file backend.

*If you specify paths without a protocol, then sixoclock treats them as file
URIs for convenience.*
