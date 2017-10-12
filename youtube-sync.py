#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import os
import configparser
import sqlite3
import shlex
import time
from urllib.parse import urlparse
import subprocess

import youtube_dl
import youtube_dl.utils
import youtube_dl.postprocessor
import youtube_dl.postprocessor.common

default_output_dir = '.'


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


# Fix thumbnail image for AtomicPersley (Niconico will returns unsupported jpeg, so convert to png in advance)
class FixThumbnailPP(youtube_dl.postprocessor.common.PostProcessor):
    def __init__(self, downloader):
        super(FixThumbnailPP, self).__init__(downloader)

    def run(self, info):
        if len(info['thumbnails']) == 0:
            return [], info
        convert_from = youtube_dl.utils.encodeFilename(info['thumbnails'][0]['filename'])
        convert_to_raw = '{}.png'.format(info['thumbnails'][0]['filename'])
        convert_to = youtube_dl.utils.encodeFilename(convert_to_raw)
        cmd = "convert '{}' '{}'".format(convert_from, convert_to)
        self._downloader.to_screen('[fix_thumb] %s' % cmd)
        retCode = subprocess.call(youtube_dl.utils.encodeArgument(cmd), shell=True)
        if retCode != 0:
            raise youtube_dl.postprocessor.common.PostProcessingError(
                'Command returned error code %d' % retCode)
        info['thumbnails'][0]['filename'] = convert_to_raw  # overwrite thumbnail for embedding
        return [convert_from], info

setattr(youtube_dl.postprocessor, 'FixThumbnailPP', FixThumbnailPP)


class cd:
    """Context manager for changing the current working directory"""

    def __init__(self, newPath):
        self.newPath = os.path.expanduser(newPath)

    def __enter__(self):
        self.savedPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)


class Sync(object):
    def __init__(self, name, opts, db):
        self.db = db
        self.name = name
        self.opts = opts

    def run(self, ydl_opts):
        out_dir = self.opts.get('output_dir', default_output_dir)
        os.makedirs(out_dir, exist_ok=True)
        with cd(out_dir):
            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                self._proc_(ydl)

    def _proc_(self, ydl):
        ie = ydl.extract_info(self.opts['url'], download=False, process=False)
        if ie['_type'] == 'url':
            ie = ydl.extract_info(ie['url'], download=False, process=False)
        if 'entries' not in ie:
            print('entries not found', ie)
            raise RuntimeError('Unsupported url: %s' % self.opts['url'])
        entries = list(ie['entries'])
        failures = 0
        for entry in entries:
            try:
                tpl = self.db.get_history(entry)
                if tpl and tpl['state'] != 2:  # Found and failed
                    continue
                res = ydl.process_ie_result(entry)
                self.db.insert(self.name, entry, 0)  # Succeed
            except Exception as e:
                print('process_ie_result failed', e)
                failures += 1
                self.db.insert(self.name, entry, 1)  # Failed
        if failures > len(entries) / 4:
            raise Exception('Too many failures')


class DB(object):
    def __init__(self, db_path):
        self.path = db_path
        self.db = sqlite3.connect(self.path)
        self.db.row_factory = dict_factory
        self.db.execute(
            'CREATE TABLE IF NOT EXISTS '
            'history ('
            '  id TEXT, '
            '  grp TEXT'
            '  extractor TEXT,'
            '  caption TEXT, '
            '  state INTEGER, '  # NULL or 0 : default, no problem. 1 : failed  2: requesting retry
            '  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,'
            '  PRIMARY KEY (id, extractor))'
        )

    def _extract_infos(self, entry):
        ie_key = entry['ie_key']
        if ie_key == 'Niconico':
            id = os.path.basename(urlparse(entry['url']).path)  # use 'sm123456' part as
            return id, entry['ie_key'], id

        elif ie_key == 'Youtube':
            return entry['id'], entry['ie_key'], entry['title']

    def get_history(self, entry):
        id, ie_key, _ = self._extract_infos(entry)
        return self.db.execute('SELECT timestamp, state FROM history WHERE id = ? AND extractor = ?',
                               (id, ie_key)).fetchone()

    def insert(self, group, entry, state):
        id, ie_key, title = self._extract_infos(entry)
        self.db.execute('INSERT INTO history(id, grp, extractor, caption, state) VALUES (?, ?, ?, ?, ?)',
                        (id, group, ie_key, title, state))
        self.db.commit()


# youtube-dl --ignore-errors --yes-playlist --no-overwrite -x -f m5a --add-metadata --embed-thumbnail --postprocessor-args '-vn -c:a libfdk_aac -b:a 264k' $1
def main():
    manifests = configparser.ConfigParser()
    manifests.read('config.ini')
    db = DB('database.sqlite3')
    if 'output_dir' in manifests['global']:
        global default_output_dir
        default_output_dir = manifests['global']['output_dir']
    postprocessors = []
    postprocessors.append({
        'key': 'FFmpegExtractAudio',
        'preferredcodec': 'm4a',
    })
    postprocessors.append({'key': 'FFmpegMetadata'})
    postprocessors.append({'key': 'FixThumbnail'})
    postprocessors.append({
        'key': 'EmbedThumbnail',
        'already_have_thumbnail': False,
    })
    ydl_opts = {
        'usenetrc': True,
        'postprocessors': postprocessors,
        'writethumbnail': True,
        # 'postprocessor_args': shlex.split('-vn -c:a libfdk_aac -b:a 264k'),
        'postprocessor_args': shlex.split('-vn -b:a 264k -strict -2'),
    }

    for name, opts in manifests.items():
        if name in {'global', 'DEFAULT'}:
            continue
        s = Sync(name, opts, db)
        s.run(ydl_opts)
        time.sleep(10)


if __name__ == '__main__':
    main()
