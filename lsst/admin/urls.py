from django.conf.urls import patterns, include, url
from django.conf import settings
from django.conf.urls.static import static
from django.views.generic import TemplateView

from django.conf import settings


from lsst.admin import views as adviews

urlpatterns = patterns('',
    url(r'^reqplot/$', adviews.listReqPlot, name='reqPlot'),
    url(r'^$', adviews.listReqPlot, name='reqPlot'),
)
