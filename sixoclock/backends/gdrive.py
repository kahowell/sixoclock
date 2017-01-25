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

from datetime import datetime
import argparse
import io
import mimetypes
import os.path
import tempfile

from googleapiclient import discovery
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from oauth2client import client, tools
from oauth2client.file import Storage
import dateutil.parser
import httplib2

from sixoclock.backend import Backend

SCOPES = 'https://www.googleapis.com/auth/drive'
FOLDER_MIMETYPE = 'application/vnd.google-apps.folder'

class GoogleDriveBackend(Backend):
    @classmethod
    def default_name(cls):
        return 'gdrive'

    def __init__(self, **kwargs):
        home = os.path.expanduser('~')
        credentials_path = os.path.join(home, '.sixoclock_gdrive_credentials.json')
        self.store = Storage(credentials_path)
        self.credentials = self.get_credentials()
        self.http = self.credentials.authorize(httplib2.Http())
        self.service = discovery.build('drive', 'v3', http=self.http)
        self.num_retries = 10  # TODO config
        self._locate_cache = {}

    def get_credentials(self):
        credentials = self.store.get()
        if not credentials or credentials.invalid:
            raise EnvironmentError('Need to run setup first!')  # TODO human readable instructions?
        return credentials

    def setup(self, args):
        if self.credentials and not self.credentials.invalid:
            raise EnvironmentError('Already setup')  # TODO add flag to redo
        home = os.path.expanduser('~')
        secrets_path = os.path.join(home, '.sixoclock_gdrive_secrets.json')
        flow = client.flow_from_clientsecrets(secrets_path, SCOPES)
        flow.user_agent = 'sixoclock'
        tools.run_flow(flow, store, args)

    def _create_folder(self, name, parent_id):
        file_metadata = {
            'name': name,
            'mimeType': FOLDER_MIMETYPE,
        }
        if parent_id != 'root':
            file_metadata['parents'] = [parent_id]
        file = self.service.files().create(body=file_metadata, fields='id').execute(num_retries=self.num_retries)
        return file.get('id')

    def _locate(self, path, create_dirs=False):
        if path in self._locate_cache:
            return self._locate_cache[path]
        components = [component for component in path.split('/') if component]
        folder_id = 'root'
        for component in components:
            if folder_id is not None:
                query = "name = '{}' and mimeType = '{}' and '{}' in parents".format(component, FOLDER_MIMETYPE, folder_id)
                response = self.service.files().list(q=query, spaces='drive', fields='files(id)').execute(num_retries=self.num_retries)
                new_folder_id = None
                for file in response.get('files', []):
                    new_folder_id = file.get('id')
                if new_folder_id is None:
                    if create_dirs:  # TODO short-circuit, and essentially just makedirs all the way down.
                        folder_id = self._create_folder(component, parent_id=folder_id)
                    else:
                        return None
                else:
                    folder_id = new_folder_id
        self._locate_cache[path] = folder_id
        return folder_id

    def _collect(self, query, fields):
        page_token = None
        while True:
            response = self.service.files().list(q=query, spaces='drive', fields='nextPageToken, {}'.format(fields), pageToken=page_token).execute(num_retries=self.num_retries)
            for file in response.get('files', []):
                yield file
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

    def _walk(self, path, folder_id=None):
        folder_id = folder_id or self._locate(path)
        if folder_id is None:
            return
        dirs_query = "'{}' in parents".format(folder_id)
        entries = list(self._collect(dirs_query, fields='files(id, name, mimeType)'))
        dirs = [file for file in entries if file.get('mimeType', None) == FOLDER_MIMETYPE]
        files = [file for file in entries if file.get('mimeType', None) != FOLDER_MIMETYPE]
        yield (path, [dir.get('name') for dir in dirs], [file.get('name') for file in files])
        for dir in dirs:
            for result in self._walk(os.path.join(path, dir.get('name')), folder_id=dir.get('id')):
                yield result

    def list(self, uri, recurse):
        base_uri = uri
        base_path = self.determine_path(uri)
        for dirpath, _, filenames in self._walk(base_path):
            for filename in filenames:
                path = os.path.join(dirpath, filename)
                relative_path = os.path.relpath(path, base_path)
                yield self.full_uri(base_uri, relative_path)
            if not recurse:
                return

    def _convert_date(self, date):
        return int(dateutil.parser.parse(date).timestamp())

    def _get(self, uri, fields):
        path = self.determine_path(uri)
        dirpath = os.path.dirname(path)
        filename = os.path.basename(path)
        folder_id = self._locate(dirpath)
        query = "name = '{}' and '{}' in parents".format(filename, folder_id)
        response = self.service.files().list(q=query, spaces='drive', fields=fields).execute(num_retries=self.num_retries)
        for file in response.get('files', []):
            return file

    def get(self, uri):
        new_folder_id = None
        file = self._get(uri, fields=('files(id, modifiedTime)'))
        if file:
            with self.stream(uri, file=file) as stream:
                result = File.from_stream(stream)
                result.uri = uri
                result.mtime = self._convert_date(file.get('modifiedTime'))
                return result

    def _convert_timestamp(self, timestamp):
        return '{}Z'.format(datetime.utcfromtimestamp(timestamp).isoformat('T'))

    def put(self, uri, stream, mtime=None):
        path = self.determine_path(uri)
        filename = os.path.basename(path)
        dirpath = os.path.dirname(path)
        parent_folder_id = self._locate(dirpath, create_dirs=True)
        mimetype = mimetypes.guess_type(uri)[0] or 'application/octet-stream'
        file_metadata = {
            'name': filename,
            'mimeType': mimetype,
            'parents': [parent_folder_id],
        }
        if mtime:
            file_metadata['modifiedTime'] = self._convert_timestamp(mtime)
        media = MediaIoBaseUpload(stream, mimetype=mimetype, chunksize=-1, resumable=True)
        file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute(num_retries=self.num_retries)

    def delete(self, uri):
        file = self._get(uri, fields='files(id)')
        if file:
            self.service.files().delete(fileId=file.id)

    def stream(self, uri, file=None):
        file = file or self._get(uri, fields='files(id)')
        if file:
            output = tempfile.TemporaryFile()
            request = self.service.files().get_media(fileId=file.id)
            downloader = MediaIoBaseDownload(output, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            output.seek(0)
            return output

    def mtime(self, uri):
        file = self._get(uri, fields='files(modifiedTime)')
        if file:
            return self._convert_date(file.get('modifiedTime'))

    def has_subparser(self):
        return True

    def contribute_to_subparser(self, parser):
        parser.set_defaults(function=lambda args: parser.print_usage())
        subparsers = parser.add_subparsers(title='commands')

        setup_parser = subparsers.add_parser('setup', help='setup Google Drive API access through OAuth')
        setup_parser.set_defaults(function=self.setup)
