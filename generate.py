import os
import os.path
import sys
import argparse
import copy
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


def generate_output(tree, directory=''):
    output_dir = os.path.join(args.output, directory)
    output_file = os.path.join(output_dir, 'index.html')
    try:
        os.makedirs(output_dir)
    except:
        pass

    files, directories = tree
    rows = []
    rowtpl = config.get('output', {}).get('entry_tpl', DEFAULT_ENTRY_TPL)
    dirtpl = config.get('output', {}).get('directory_tpl', DEFAULT_DIR_TPL)
    bodytpl = config.get('output', {}).get('body_tpl', DEFAULT_BODY_TPL)
    skipzero = not bool(config.get('output', {}).get('list_zero_sized', False))
    base_url = config.get('bucket', {}).get('base_url', '')
    file_sort_key = config.get('output', {}).get('file_sort_key', 'name')
    reverse_files = bool(config.get('output', {}).get('reverse_files', False))

    # Generate directory list (as well as listings for deeper directories)
    for ndir, tree in sorted(directories.items(), key=lambda x: x[0]):
        path = os.path.join(directory, ndir)
        dirsize = generate_output(tree, path)
        if dirsize == 0 and skipzero:
            continue
        fmt = {
            'name': ndir + '/',
            'link': base_url + '/'.join(['', path.replace(os.sep, '/'), 'index.html']),
            'size': dirsize
        }
        rows.append(dirtpl.format(**fmt))

    # Generate file list
    fs = sorted(files, key=lambda x: x[file_sort_key])
    fs = fs if not reverse_files else reversed(fs)
    for f in fs:
        if f['size'] == 0 and not config.get('output', {}).get('list_zero_sized'):
            continue
        if directory:
            url = base_url + '/'.join(['', directory.replace(os.sep, '/'), f['name']])
        else:
            url = base_url + f['name']

        fmt = {
            'name': f['name'],
            'link': url,
            'size': f['size'],
            'humansize': humansize(f['size']),
            'etag': f['etag'],
            'storage': f['storage'],
            'mdate': f['mdate']
        }
        rows.append(rowtpl.format(**fmt))

    bodyfmt = {
        'title': 'Directory listing for {}'.format(directory) if directory else 'Directory listing',
        'style': CSS,
        'script': JAVASCRIPT,
        'extrahead': config.get('output', {}).get('extra_head', '')
    }
    with open(output_file, 'w') as f:
        f.write(BASE_TPL.format(listing=bodytpl.format(entries="".join(rows)), **bodyfmt))

    return len(files)

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
