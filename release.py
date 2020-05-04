#!/usr/bin/env python3

import argparse
import pathlib
import re
import subprocess
import sys
from typing import Tuple


def parse_arguments():
    parser = argparse.ArgumentParser(description="Releases a go module")
    parser.add_argument("-m","--mod", help="Filename of the go mod file you would like to release")
    parser.add_argument("-v","--version", help="Version to use")
    return parser.parse_args()


def parse_semver(version: str) -> Tuple[int, ...]:
    return tuple(int(part) for part in version.split(".", maxsplit=2))


def main():
    args = parse_arguments()

    with open(args.mod, "r") as fp:
        mod = fp.read()

    if re.search("^module ", mod, re.MULTILINE) is None:
        print("It looks like the given file is not a go module.")
        return False

    mod_path = pathlib.Path(args.mod).absolute()

    # look for the repository root
    root = mod_path
    while not (root / ".git").is_dir():
        root = root.parent

    # look for changes in the git repo
    git_status_output = subprocess.check_output(["git", "status", "--porcelain"], cwd=str(root))
    if git_status_output.strip():
        print("Your workspace is dirty, please commit first.")
        return False

    # get the canonical submodule name
    submodule = str(mod_path.relative_to(root).parent)
    if submodule == ".":
        tag_prefix = "v"
    else:
        tag_prefix = submodule + "/v"

    print("Synchronize git tags with remote")
    subprocess.check_call(["git", "fetch"], cwd=str(root))

    print("Lookup version of go module")
    git_tags_output = subprocess.check_output(["git", "tag"], cwd=str(root)).decode("utf8")
    module_versions = re.findall("^" + re.escape(tag_prefix) + r'(\d+.\d+.\d+)$', git_tags_output, re.MULTILINE)
    module_version = max(parse_semver(v) for v in module_versions)

    major, minor, patch = module_version
    release_version = (major, minor, patch + 1)

    release_tag = args.version or (tag_prefix + ".".join(str(v) for v in release_version))

    print("Create release tag {}".format(release_tag))
    subprocess.check_call(["git", "tag", release_tag], cwd=str(root))

    print("Push to go remote")
    subprocess.check_call(["git", "push", "origin", "master", "--tags"], cwd=str(root))

    return True


if __name__ == "__main__":
    sys.exit(0 if main() else 0)
