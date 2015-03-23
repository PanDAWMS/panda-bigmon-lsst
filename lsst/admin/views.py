import logging, re, json, commands, os, copy
from datetime import datetime, timedelta
import time
import json
from django.http import HttpResponse
from django.shortcuts import render_to_response, render, redirect
from django.template import RequestContext, loader
from django.db.models import Count
from django import forms
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
from django.utils.cache import patch_cache_control, patch_response_headers
from core.common.settings import STATIC_URL, FILTER_UI_ENV, defaultDatetimeFormat

from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger

from core.common.models import RequestStat
from core.common.settings.config import ENV
from time import gmtime, strftime


from lsst.views import initRequest
from lsst.views import extensibleURL

def login(request):
    if len(request.session['userdn'])==0:
       return redirect('https://bigpanda.cern.ch')
    return True
    
def logout(request):
    try:
        del request.session['member_id']
    except KeyError:
        pass
    return HttpResponse("You're logged out.")    

def adMain(request):

    valid, response = initRequest(request)
    if not valid: return response

    data = {\
       'request': request,
       'url' : request.path,\
    }

    return render_to_response('adMain.html', data, RequestContext(request))

def listReqPlot(request):
    valid, response = initRequest(request)
    if not valid: return response

    sortby='id'
    if 'sortby' in request.GET:
        sortby=request.GET['sortby']
 
    LAST_N_HOURS_MAX=7*24
    limit=5000
    if 'hours' in request.session['requestParams']:
        LAST_N_HOURS_MAX = int(request.session['requestParams']['hours'])
    if 'days' in request.session['requestParams']:
        LAST_N_HOURS_MAX = int(request.session['requestParams']['days'])*24

    if u'display_limit' in request.session['requestParams']:
        display_limit = int(request.session['requestParams']['display_limit'])
    else:
        display_limit = 1000
    nmax = display_limit

    if LAST_N_HOURS_MAX>=168:
       flag=12
    elif LAST_N_HOURS_MAX>=48:
       flag=6
    else:
       flag=2

    startdate = None
    if not startdate:
        startdate = timezone.now() - timedelta(hours=LAST_N_HOURS_MAX)
    enddate = None
    if enddate == None:
        enddate = timezone.now()#.strftime(defaultDatetimeFormat)

    query = { 'qtime__range' : [startdate.strftime(defaultDatetimeFormat), enddate.strftime(defaultDatetimeFormat)] }

    values = 'urls', 'qtime','remote','qduration','duration'
    reqs=[]
    reqs = RequestStat.objects.filter(**query).order_by(sortby).reverse().values(*values)

    reqHist = {}
    drHist =[]

    mons=[]
    for req in reqs:
        mon={}
        #mon['duration'] = (req['qduration'] - req['qtime']).seconds
        mon['duration'] = req['duration']
        mon['urls'] = req['urls']
        mon['remote'] = req['remote']
        mon['qduration']=req['qduration'].strftime('%Y-%m-%d %H:%M:%S')
        mon['qtime'] =  req['qtime'].strftime('%Y-%m-%d %H:%M:%S')
        mons.append(mon)

        ##plot
        tm=req['qtime']
        tm = tm - timedelta(hours=tm.hour % flag, minutes=tm.minute, seconds=tm.second, microseconds=tm.microsecond)
        if not tm in reqHist: reqHist[tm] = 0
        reqHist[tm] += 1

        ##plot -view duration
        dr=int(mon['duration'])
        drHist.append(dr)

    kys = reqHist.keys()
    kys.sort()
    reqHists = []
    for k in kys:
        reqHists.append( [ k, reqHist[k] ] )

    drcount=[[x,drHist.count(x)] for x in set(drHist)]
    drcount.sort()

    #do paging

    paginator = Paginator(mons, 200)
    page = request.GET.get('page')
    try:
        reqPages = paginator.page(page)
    except PageNotAnInteger:
        reqPages = paginator.page(1)
    except EmptyPage:
        reqPages = paginator.page(paginator.num_pages)

    data = {\
       'mons': mons[:nmax],
       'nmax': nmax,
       'request': request,
       'reqPages': reqPages,
       'url' : extensibleURL(request),
       'drHist': drcount,
       'reqHist': reqHists,\
    }

    return render_to_response('req_plot.html', data, RequestContext(request))

