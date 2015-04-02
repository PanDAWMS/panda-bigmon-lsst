from os.path import dirname, join

import core
from core import common
from core.common.settings.base import COMMON_INSTALLED_APPS
import lsst
import filebrowser
import pbm


VERSIONS = {
    'core': core.__versionstr__,
    'lsst': lsst.__versionstr__,
}


TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates" or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    join(dirname(lsst.__file__), 'templates'),
    join(dirname(common.__file__), 'templates'),
    join(dirname(filebrowser.__file__), 'templates'),
    join(dirname(pbm.__file__), 'templates'),

)

INSTALLED_APPS_BIGPANDAMON_LSST = (
    ### BigPanDAmon core
    'core.common',
    'core.table',
#    'core.graphics', #NOT-IMPLEMENTED
    'core.pandajob',
    'core.resource',
    'core.status_summary',
#    'core.htcondor', #NOT-NEEDED-IN-LSST
#    'core.task', #NOT-IMPLEMENTED
    'filebrowser',
    'pbm',
    'pbm.templatetags',
)
INSTALLED_APPS = COMMON_INSTALLED_APPS + INSTALLED_APPS_BIGPANDAMON_LSST

MIDDLEWARE_CLASSES = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'lsst.userauthorization.processAuth',
    )

ROOT_URLCONF = 'lsst.urls'

SITE_ID = 2

# email
EMAIL_SUBJECT_PREFIX = 'bigpandamon-lsst: '


