#!/usr/bin/python3
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import os
import configparser
import sqlite3
import shlex
import time

import youtube_dl

default_output_dir = '.'


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


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
            print(ie)
            raise RuntimeError('Unsupported url: %s' % self.opts['url'])
        entries = list(ie['entries'])
        failures = 0
        for entry in entries:
            try:
                if self.db.get_history(entry):
                    continue
                ydl.process_ie_result(entry)
                self.db.insert(entry)
            except Exception as e:
                print(e)
                failures += 1
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
            '  extractor TEXT,'
            '  caption TEXT, '
            '  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,'
            '  PRIMARY KEY (id, extractor))'
        )

    def get_history(self, entry):
        return self.db.execute('SELECT timestamp FROM history WHERE id = ? AND extractor = ?',
                               (entry['id'], entry['ie_key'])).fetchone()

    def insert(self, entry):
        self.db.execute('INSERT INTO history(id, extractor, caption) VALUES (?, ?, ?)',
                        (entry['id'], entry['ie_key'], entry['title']))
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
    postprocessors.append({
        'key': 'EmbedThumbnail',
        'already_have_thumbnail': False,
    })
    ydl_opts = {
        'postprocessors': postprocessors,
        'writethumbnail': True,
        # 'postprocessor_args': shlex.split('-vn -c:a libfdk_aac -b:a 264k'),
        'postprocessor_args': shlex.split('-vn -b:a 264k -strict -2'),
    }

    syncs = [
        Sync(name, opts, db)
        for name, opts in manifests.items()
        if name not in {'global', 'DEFAULT'}
    ]

    for name, opts in manifests.items():
        if name in {'global', 'DEFAULT'}:
            continue
        s = Sync(name, opts, db)
        s.run(ydl_opts)
        time.sleep(10)


if __name__ == '__main__':
    main()
