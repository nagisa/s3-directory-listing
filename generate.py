import os
import os.path
import sys
import argparse
import copy
import json
import csv
from collections import defaultdict
from datetime import datetime

import boto.s3
import boto.s3.bucket
from boto.exception import S3ResponseError
import toml

# Default template for each file. Variables used are {name}, {link}, {size}, {humansize}, {mod}
# (mod accepts date formating options, see python reference), {storage} and {etag}.
DEFAULT_ENTRY_TPL = """<tr>
    <td class="filename"><a href="{link}">{name}</a></td>
    <td class="size">{humansize}</td>
    <td class="modification">{mdate:%Y-%m-%d %H:%M}</td>
</tr>"""

# Default template for each directory displayed. Variables used are {name}, {link} and {size} which
# has amount of files inside the directory.
DEFAULT_DIR_TPL = """<tr>
    <td class="directory"><a href="{link}">{name}</a></td>
    <td class="size">{size} files</td>
</tr>"""

# Template which wraps the list of files and goes between <body> tags. Should contain {entries}
# variable.
DEFAULT_BODY_TPL = """<table id="listing">
<tr>
<th class="name">Name</th>
<th>Size</th>
<th>Last modified</th>
</tk>
{entries}
</table>"""

BASE_TPL = """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{title}</title>
    <style>{style}</style>
    <script>{script}</script>
    {extrahead}
</head>
<body>
{listing}
</body>
</html>
"""

CSS = """
body {
    margin: 10px 20px;
    padding: 0;
}

#listing {
    width: 100%;
    border-collapse: collapse;
}

.modification, .size {
    white-space: nowrap;
    text-overflow: ellipsis;
    width: 10%;
}

td, th {
    text-align: left;
    padding: 5px;
}

tr {
    border-bottom: 1px solid #ddd;
}
"""

JAVASCRIPT = """
"""


def panic(msg = None):
    if msg:
        print(msg, file=sys.stderr)
    sys.exit(1)


def emptydir():
    return ([], defaultdict(emptydir))

def collect_files(iterator):
    ret = emptydir()
    pathsep = config.get('bucket', {}).get('path_separator', '/')
    for f in iterator:
        if f.name is None:
            continue
        path = list(reversed(f.name.split(pathsep)))
        currdir = ret
        while True:
            component = path.pop()
            if len(path) == 0:
                # Top-level file
                currdir[0].append({
                    'name': component,
                    'size': f.size,
                    'etag': f.etag,
                    'mdate': datetime.strptime(f.last_modified, '%Y-%m-%dT%H:%M:%S.%fZ'),
                    'storage': f.storage_class
                })
                break
            else:
                # Directory component
                currdir = currdir[1][component]
    return ret


def humansize(size):
    if size < 2 ** 10:
        return "{} B".format(size)
    elif size < 2 ** 20:
        return "{:.1f} KiB".format(size / 2 ** 10)
    elif size < 2 ** 30:
        return "{:.2f} MiB".format(size / 2 ** 20)
    elif size < 2 ** 40:
        return "{:.3f} GiB".format(size / 2 ** 30)
    else:
        return "{:.4f} TiB".format(size / 2 ** 40)

def get_output(directory, fname):
    output_dir = os.path.join(args.output, directory)
    output_file = os.path.join(output_dir, fname)
    try:
        os.makedirs(output_dir)
    except:
        pass
    return (output_dir, output_file)


def file_url(base, directory, filename):
    if directory:
        url = '/'.join([base, directory.replace(os.sep, '/'), filename])
    else:
        url = '/'.join([base, filename])
    # As a precaution if our string concatenation doesn’t cut it
    return url.replace('//', '/')


class HtmlGenerator(object):
    def __init__(self, htmlconfig={}):
        self.base_url = config.get('bucket', {}).get('base_url', '')
        self.rowtpl = htmlconfig.get('entry_tpl', DEFAULT_ENTRY_TPL)
        self.dirtpl = htmlconfig.get('directory_tpl', DEFAULT_DIR_TPL)
        self.bodytpl = htmlconfig.get('body_tpl', DEFAULT_BODY_TPL)
        self.skipzero = not bool(htmlconfig.get('list_zero_sized', False))
        self.file_sort_key = htmlconfig.get('file_sort_key', 'name')
        self.reverse_files = bool(htmlconfig.get('reverse_files', False))

    # Predicate on whether the file should be not listed
    def skip(self, name, size, is_dir=False):
        if size == 0 and self.skipzero:
            return True
        return False

    def run(self, tree, directory=''):
        (output_dir, output_file) = get_output(directory, 'index.html')
        files, directories = tree
        rows = []
        # Generate directory list (as well as listings for deeper directories)
        for dirname, children in sorted(directories.items(), key=lambda x: x[0]):
            path = os.path.join(directory, dirname)
            childcount = self.run(children, path)
            if self.skip(dirname, childcount, True):
                continue
            rows.append(self.dirtpl.format(**{
                'name': dirname + '/',
                'link': file_url(self.base_url, path, 'index.html'),
                'size': childcount
            }))
        # Generate file list, appropriately sorted
        fs = sorted(files, key=lambda x: x[self.file_sort_key])
        fs = fs if not self.reverse_files else reversed(fs)
        for f in fs:
            if self.skip(f['name'], f['size']):
                continue
            rows.append(self.rowtpl.format(**{
                'name': f['name'],
                'link': file_url(self.base_url, directory, f['name']),
                'size': f['size'],
                'humansize': humansize(f['size']),
                'etag': f['etag'],
                'storage': f['storage'],
                'mdate': f['mdate']
            }))
        # Write the output
        extrahead = config.get('output', {}).get('extra_head', '')
        title = 'Directory listing of {}'.format(directory) if directory else 'Directory listing'
        with open(output_file, 'w') as f:
            f.write(BASE_TPL.format(
                listing=self.bodytpl.format(entries="".join(rows)),
                title=title,
                extrahead=extrahead,
                style=CSS,
                script=JAVASCRIPT
            ))
        return len(files)


class JsonGenerator(object):
    def __init__(self, jsonconfig={}):
        self.base_url = config.get('bucket', {}).get('base_url', '')
        self.pretty = jsonconfig.get('pretty', False)

    def run(self, tree):
        (output_dir, output_file) = get_output('', 'index.json')
        with open(output_file, 'w') as f:
            json.dump(self._run(tree), f, indent=4 if self.pretty else 0)

    def _run(self, tree, d=''):
        files, directories = tree
        output = {'fs': [], 'ds': []}
        for dirname, children in directories.items():
            path = os.path.join(d, dirname)
            output['ds'].append({
                'name': dirname,
                'index_link': file_url(self.base_url, path, 'index.html'),
                'children': self._run(children, path)
            })
        for f in files:
            output['fs'].append({
                'name': f['name'],
                'link': file_url(self.base_url, d, f['name']),
                'size': f['size'],
                'etag': f['etag'],
                'storage': f['storage'],
                'mdate': f['mdate'].isoformat()
            })
        return output


class TxtGenerator(object):
    def __init__(self, txtconfig={}):
        self.base_url = config.get('bucket', {}).get('base_url', '')
        self.delimiter = txtconfig.get('delimiter', '\t')
        self.file_fields = txtconfig.get('file_fields', ['path', 'size', 'mdate'])
        self.filename = txtconfig.get('filename', 'index.txt')

    def run(self, tree):
        (output_dir, output_file) = get_output('', self.filename)
        with open(output_file, 'w') as f:
            if self.delimiter == '\t':
                writer = csv.writer(f, dialect='excel-tab')
            else:
                writer = csv.writer(f, delimiter=self.delimiter)
            self._run(tree, writer)

    def _run(self, tree, writer, directory=''):
        files, directories = tree
        for f in files:
            vals = {
                'name': f['name'],
                'path': file_url('', directory, f['name']),
                'link': file_url(self.base_url, directory, f['name']),
                'size': f['size'],
                'etag': f['etag'],
                'storage': f['storage'],
                'mdate': f['mdate'].isoformat()
            }
            writer.writerow([vals[f] for f in self.file_fields])
        for dirname, children in directories.items():
            path = os.path.join(directory, dirname)
            self._run(children, writer, path)


if __name__ == "__main__":
    argp = argparse.ArgumentParser("s3-listing-generator",
            description="Generate a static directory listing for your s3 buckets");
    argp.add_argument("config", nargs='?', type=argparse.FileType('r'),
                      help="Declare config file to use")
    argp.add_argument("--output", type=str, default="out/",
                      help="Directory in which the listings are output (default: out/)")
    args = argp.parse_args();
    currp = os.path.dirname(os.path.abspath(__file__))
    if args.config is None:
        args.config = open(os.path.join(currp, "config.toml"))
    args.output = os.path.join(currp, args.output)

    config = toml.load(args.config)
    args.config.close()

    bucket = config.get("bucket")
    if bucket is None:
        panic("No [bucket] is specified in configuration!")
    name, region = bucket.get("name"), bucket.get("region")
    akey, skey = bucket.get("access_key"), bucket.get("secret_key")
    prefix = bucket.get('prefix', '')
    if name is None:
        panic("Bucket name is not specified!")
    regions = dict((r.name, r) for r in boto.s3.regions())
    region = regions.get(region)
    if region is None:
        panic("Region is not specified or is invalid. Valid regions are {}"
              .format(", ".join(regions.keys())))
    connection = region.connect(aws_access_key_id=akey, aws_secret_access_key=skey)
    bucket = connection.lookup(name)
    if bucket is None:
        panic("Could not open/find bucket \"{}\"! Are your AWS keys/configuration valid?"
              .format(name))

    tree = collect_files(bucket.list(prefix=prefix))
    generate_output(tree)

    # Generate the requested output formats
    outputs = config.get('output', {})
    targets = {
        'html': HtmlGenerator,
        'json': JsonGenerator,
        'txt': TxtGenerator
    }
    for name in outputs.keys():
        tp = config['output'][name].get('type', None)
        klass = targets.get(tp, None)
        if tp is None or klass is None:
            panic('Unknown type for section output.{}'.format(name))
        klass(config['output'][name]).run(tree)
