
from os.path import dirname, join

import core
from core import common
from core.common.settings.base import COMMON_INSTALLED_APPS
import lsst


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

)

INSTALLED_APPS_BIGPANDAMON_LSST = (
    ### BigPanDAmon core
    'core.common',
    'core.table',
#    'core.graphics', #NOT-IMPLEMENTED
    'core.pandajob',
    'core.resource',
#    'core.htcondor', #NOT-NEEDED-IN-LSST
#    'core.task', #NOT-IMPLEMENTED
)
INSTALLED_APPS = COMMON_INSTALLED_APPS + INSTALLED_APPS_BIGPANDAMON_LSST


ROOT_URLCONF = 'lsst.urls'

SITE_ID = 2

# email
EMAIL_SUBJECT_PREFIX = 'bigpandamon-lsst: '


