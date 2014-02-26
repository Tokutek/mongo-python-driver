# Copyright 2009-2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Some tools for running tests based on MongoDB server version."""


import re

def _padded(iter, length, padding=0):
    l = list(iter)
    if len(l) < length:
        for _ in range(length - len(l)):
            l.append(0)
    return l


suffix_re = re.compile(r'([-+])[a-z]+\.([0-9]+)$')

def _parse_version_string(version_string):
    mod = 0
    if version_string.endswith("+"):
        version_string = version_string[0:-1]
        mod = 1
    elif version_string.endswith("-pre-"):
        version_string = version_string[0:-5]
        mod = -1
    elif version_string.endswith("-"):
        version_string = version_string[0:-1]
        mod = -1
    # Deal with '-rcX' substrings
    if version_string.find('-rc') != -1:
        version_string = version_string[0:version_string.find('-rc')]
        mod = -1

    # deal with things like '+hotfix.N'
    m = suffix_re.search(version_string)
    if m:
        version_string = version_string[0:m.start()]
        if m.group(1) == '-':
            mod = -1
        else:
            mod = int(m.group(2)) + 1

    version = [int(part) for part in version_string.split(".")]
    version = _padded(version, 3)
    version.append(mod)

    return tuple(version)


# Note this is probably broken for very old versions of the database...
def version(client):
    return _parse_version_string(client.server_info()["version"])


def tokumx_version(client):
    return _parse_version_string(client.server_info()["tokumxVersion"])


def at_least(client, min_version):
    return version(client) >= tuple(_padded(min_version, 4))


def tokumx_at_least(client, min_version):
    return tokumx_version(client) >= tuple(_padded(min_version, 4))
