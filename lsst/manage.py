#!/usr/bin/env python
import os
import sys
from os.path import join, pardir, abspath, dirname, split


# fix PYTHONPATH and DJANGO_SETTINGS for us
# django settings module
DJANGO_SETTINGS_MODULE = '%s.%s' % (split(abspath(dirname(__file__)))[1], 'settings')
# pythonpath dirs
PYTHONPATH = [
    join(dirname(__file__), pardir),
]

# inject few paths to pythonpath
for p in PYTHONPATH:
    if p not in sys.path:
        sys.path.insert(0, p)

sys.path.append('/afs/cern.ch/user/w/wenaus/public/work/virtualenv/django1.6.1__python2.6.6__lsst/appdir/lsst-git')
sys.path.append('/afs/cern.ch/user/w/wenaus/public/work/virtualenv/django1.6.1__python2.6.6__lsst/appdir/core-git')

for p in sys.path:
    print p

os.environ['DJANGO_SETTINGS_MODULE'] = DJANGO_SETTINGS_MODULE


if __name__ == "__main__":
#    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "bigpandamon.settings")

    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
