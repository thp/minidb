#!/usr/bin/env python3
# Setup script for
'minidb'
# by Thomas Perl <thp.io>

import os
import re

dirname = os.path.dirname(os.path.abspath(__file__))
src = open(os.path.join(dirname, '{}.py'.format(__doc__))).read()
docstrings = re.findall('"""(.*)"""', src)
m = dict(re.findall("__([a-z_]+)__\s*=\s*'([^']+)'", src))
m['name'] = __doc__
m['author'], m['author_email'] = re.match(r'(.*) <(.*)>', m['author']).groups()
m['description'] = docstrings[0]
m['py_modules'] = (m['name'],)
m['download_url'] = '{m[url]}{m[name]}-{m[version]}.tar.gz'.format(m=m)

from distutils.core import setup
setup(**m)
