#!/usr/bin/env python
# Setup script for python-minidb
# by Thomas Perl <thp.io>

from distutils.core import setup

import os
import re
import sys

# Make sure that we import the local minidb module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import minidb


# How is the package going to be called?
PACKAGE = 'minidb'

# List the modules that need to be installed/packaged
MODULES = (
        'minidb',
)

# These metadata fields are simply taken from the Jabberbot module
VERSION = minidb.__version__
WEBSITE = minidb.__website__
LICENSE = minidb.__license__
DESCRIPTION = minidb.__doc__

# Extract name and e-mail ("Firstname Lastname <mail@example.org>")
AUTHOR, EMAIL = re.match(r'(.*) <(.*)>', minidb.__author__).groups()

setup(name=PACKAGE,
      version=VERSION,
      description=DESCRIPTION,
      author=AUTHOR,
      author_email=EMAIL,
      license=LICENSE,
      url=WEBSITE,
      py_modules=MODULES,
      download_url=WEBSITE+PACKAGE+'-'+VERSION+'.tar.gz')

