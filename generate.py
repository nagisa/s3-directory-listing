import os.path
import sys
import argparse

import boto.s3
import boto.s3.bucket
from boto.exception import S3ResponseError
import toml

def panic(msg = None):
    if msg:
        print(msg, file=sys.stderr)
    sys.exit(1)

if __name__ == "__main__":
    argp = argparse.ArgumentParser("s3-listing-generator",
            description="Generate a static directory listing for your s3 buckets");
    argp.add_argument("config", nargs='?', type=argparse.FileType('r'),
                      help="Declare config file to use")
    args = argp.parse_args();
    if args.config is None:
        currp = os.path.dirname(os.path.abspath(__file__))
        args.config = open(os.path.join(currp, "config.toml"))

    config = toml.load(args.config)
    args.config.close()

    files = {}
    regions = dict((r.name, r) for r in boto.s3.regions())
    files = {}

    for name, bucket in config.get("buckets", {}).items():
        region = bucket.get("region", None)
        if region is None:
            panic("Region is not specified!")
        region = regions.get(region, None)
        if region is None:
            panic("Invalid region is specified! Valid regions are {}".format(list(regions.keys())))
        access_key = bucket.get("access_key", None)
        secret_key = bucket.get("secret_key", None)
        connection = region.connect(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        try:
            bucket = connection.get_bucket(name)
        except S3ResponseError:
            panic("Could not open/find bucket \"{}\"! Are your AWS keys/configuration invalid?".format(name))
        print("Getting a list of files for bucket \"{}\"".format(name))
        files[name] = bucket.list(bucket.get("prefix", ""))
