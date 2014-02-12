"""
lsst.settings.base 

"""

from os.path import dirname, join

from core import common
import lsst

TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates" or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    join(dirname(lsst.__file__), 'templates'),
    join(dirname(common.__file__), 'templates'),

)


ROOT_URLCONF = 'lsst.urls'

SITE_ID = 2

# email
EMAIL_SUBJECT_PREFIX = 'bigpandamon-lsst: '


