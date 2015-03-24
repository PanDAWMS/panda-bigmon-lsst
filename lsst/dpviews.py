#
# dpviews.py
#
# Prototyping for Data Product Catalog and associated functionality
#
import logging, re, json, commands, os, copy
from datetime import datetime, timedelta
from datetime import tzinfo
import time
import json
from urlparse import urlparse

from django.http import HttpResponse
from django.shortcuts import render_to_response, render, redirect
from django.template import RequestContext, loader
from django.db.models import Count, Sum
from django import forms
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
from django.utils.cache import patch_cache_control, patch_response_headers
from django.utils import timezone
from django.contrib import messages

import boto.dynamodb2
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, AllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.types import STRING
from boto.dynamodb2.types import STRING_SET

from settings.local import aws

from atlas.prodtask.models import TRequest, TProject, RequestStatus, ProductionTask, StepTemplate, StepExecution, InputRequestList, ProductionContainer, ProductionDataset, Ttrfconfig

from core.pandajob.models import PandaJob, Jobsactive4, Jobsdefined4, Jobswaiting4, Jobsarchived4, Jobsarchived
from core.common.models import JediTasks
from core.common.models import Filestable4 
from core.common.models import FilestableArch
from core.common.models import JediDatasets
from core.common.settings.config import ENV
from core.common.settings import STATIC_URL, FILTER_UI_ENV, defaultDatetimeFormat

import views

ENV['MON_VO'] = 'ATLAS'
viewParams = {}
viewParams['MON_VO'] = ENV['MON_VO']

req_fields = [ 'project_id', 'phys_group', 'campaign', 'manager', 'provenance', 'request_type', 'project', 'is_fast' ]
jeditask_fields = [ 'jeditaskid', 'cloud', 'processingtype', 'superstatus', 'status', 'ramcount', 'walltime', 'currentpriority', 'transhome', 'corecount', 'progress', 'failurerate', ]

# entity types supported in searches
entitytypes = [
    [ 'dataset', 'Datasets and containers' ],
    ]

## Open Amazon DynamoDB databases
dyndb = DynamoDBConnection(aws_access_key_id=aws['AWS_ACCESS_KEY_ATLAS'], aws_secret_access_key=aws['AWS_SECRET_KEY_ATLAS'])
usertable = Table('user', connection=dyndb)
projecttable = Table('project', connection=dyndb)
requesttable = Table('request', connection=dyndb)

def doRequest(request):

    ## Set default page lifetime in the http header, for the use of the front end cache
    request.session['max_age_minutes'] = 6

    ## by default, show main intro
    mode = 'intro'
    if 'mode' in request.GET: mode = request.GET['mode']
    query = {}
    dataset = None
    scope = None
    tid = None
    tidnum = None
    reqid = None
    dataset_form = DatasetForm()

    if request.method == 'POST':

        ## if POST, we have a form input to process
        formdata = request.POST.copy()
        request.session['requestParams'] = formdata
        #if 'action' in request.POST and request.POST['action'] == 'edit_from_db':

        if request.user.is_authenticated():
            formdata['requester'] = request.user.get_full_name()

        if formdata['type'] == 'dataset':
            dataset_form = DatasetForm(data=request.POST)
            ## proceed with the dataset search
            form = DatasetForm(formdata)
            if form.is_valid():
                formdata = form.cleaned_data.copy()
                mode = 'dataset'
                dataset = formdata['dataset']
            else:
                messages.warning(request, "The requested dataset search is not valid")
        else:
            messages.warning(request, "Unrecognized form submitted")

    else:
        ## GET
        request.session['requestParams'] = request.GET.copy()
        for f in req_fields:
            if f in request.GET:
                query[f] = request.GET[f]
        if 'reqid' in request.GET:
            reqid = request.GET['reqid']
            query['reqid'] = reqid
            mode = 'reqid'
        elif 'dataset' in request.GET:
            dataset = request.GET['dataset']
            mode = 'dataset'
            dataset_form = DatasetForm(data=request.GET)
        else:
            query['reqid__gte'] = 920

    if 'nosearch' in request.session['requestParams']: nosearch = True
    else: nosearch = False

    #projects = projecttable.scan()
    #projects = TProject.objects.using('deft_adcr').all().values()
    projectd = {}
    #for p in projects:
    #    projectd[p['project']] = p['description']
    projects = TProject.objects.using('deft_adcr').all().values()
    for p in projects:
        projectd[p['project']] = p

    reqs = []
    indatasets = []
    thisProject = None
    if mode in ('request', 'reqid'):
        reqs = TRequest.objects.using('deft_adcr').filter(**query).order_by('reqid').reverse().values()

    for r in reqs:
        if 'project_id' in r and r['project_id']:
            r['projectdata'] = projectd[r['project_id']]
        else:
            r['projectdata'] = None
    if len(reqs) > 0 and reqid: thisProject = reqs[0]['projectdata']

    reqstatd = {}
    query = {}
    if reqid: query['request_id'] = reqid
    reqstats = RequestStatus.objects.using('deft_adcr').filter(**query).values()
    for rs in reqstats:
        reqstatd[rs['request_id']] = rs
    for r in reqs:
        rid = r['reqid']
        if r['ref_link']:
            refparsed = urlparse(r['ref_link'])
            r['ref_link_path'] = os.path.basename(refparsed.path)[:18]
        if rid in reqstatd:
            r['comment'] = reqstatd[rid]['comment']
            r['status'] = reqstatd[rid]['status']
            r['timestamp'] = reqstatd[rid]['timestamp']
            r['owner'] = reqstatd[rid]['owner']
        else:
            r['timestamp'] = timezone.now() - timedelta(days=365*20)
            r['status'] = '?'
        if 'comment' in r and r['comment'].endswith('by WebUI'): r['comment'] = ''

    ## get event count for each slice
    sliceevs = InputRequestList.objects.using('deft_adcr').filter(request_id=reqid).order_by('slice').reverse().values('id','dataset__events')

    datasets = containers = tasks = jeditasks = jedidatasets = steps = slices = files = dsslices = []
    events_processed = None
    request_columns = None
    jobsum = []
    jobsumd = {}
    jeditaskstatus = {}
    cloudtodo = {}
    dsevents = {}
    dseventsprodsys = {}
    if reqid:
        events_processed = {}
        ## Prepare information for the particular request
        datasets = ProductionDataset.objects.using('deft_adcr').filter(rid=reqid).order_by('name').values()
        containers = ProductionContainer.objects.using('deft_adcr').filter(rid=reqid).order_by('name').values()
        tasks = ProductionTask.objects.using('deft_adcr').filter(request_id=reqid).order_by('id').values()
        steps = StepExecution.objects.using('deft_adcr').filter(request_id=reqid).values()
        slices = InputRequestList.objects.using('deft_adcr').filter(request_id=reqid).order_by('slice').reverse().values()
        jeditasks = JediTasks.objects.filter(reqid=reqid,tasktype='prod').values(*jeditask_fields)
        amitags = Ttrfconfig.objects.using('grisli').all().values()
        amitagd = {}
        for t in amitags:
            t['params'] = ""
            lparams = t['lparams'].split(',')
            vparams = t['vparams'].split(',')
            i=0
            for p in lparams:
                if vparams[i] != 'NONE':
                    txt = "&nbsp; %s=%s" % ( lparams[i], vparams[i] )
                    t['params'] += txt
                i += 1
            ctag = "%s%s" % ( t['tag'], t['cid'] )
            amitagd[ctag] = t
        jeditaskd = {}
        for t in jeditasks:
            jeditaskd[t['jeditaskid']] = t
            jeditaskstatus[t['jeditaskid']] = t['superstatus']

        indsdict = {}
        for t in tasks:
            if t['id'] in jeditaskd: t['jeditask'] = jeditaskd[t['id']]
            indsdict[t['inputdataset']] = 1

        ## get records for input datasets
        for s in slices:
            if s['dataset_id']: indsdict[s['dataset_id']] = 1
        indslist = indsdict.keys()
        indatasets = ProductionDataset.objects.using('deft_adcr').filter(name__in=indslist).values()

        ## get records for input containers
        indslist = []
        for s in slices:
            if s['dataset_id']: indslist.append(s['dataset_id'])
        incontainers = ProductionContainer.objects.using('deft_adcr').filter(name__in=indslist).values()

        ## get task datasets
        taskdsdict = {}
        for ds in datasets:
            if ds['task_id'] not in taskdsdict: taskdsdict[ds['task_id']] = []
            taskdsdict[ds['task_id']].append(ds)

        for t in tasks:
            if t['id'] in taskdsdict: t['dslist'] = taskdsdict[t['id']]

        ## get input dataset info associated with tasks
        taskl = []
        for t in tasks:
            taskl.append(t['id'])
        tdsquery = {}
        tdsquery['jeditaskid__in'] = taskl
        tdsquery['type'] = 'input'
        tdsets = JediDatasets.objects.filter(**tdsquery).values('cloud','jeditaskid','nfiles','nfilesfinished','nfilesfailed','nevents','type')
        if len(tdsets) > 0:
            for ds in tdsets:
                if ds['type'] == 'input':
                    if reqid not in dsevents: dsevents[reqid] = 0
                    dsevents[reqid] += ds['nevents']
                if jeditaskd[ds['jeditaskid']]['superstatus'] in ( 'broken', 'aborted' ) : continue
                cloud = jeditaskd[ds['jeditaskid']]['cloud']  
                if cloud not in cloudtodo:
                    cloudtodo[cloud] = {}
                    cloudtodo[cloud]['nfiles'] = 0
                    cloudtodo[cloud]['nfilesfinished'] = 0
                    cloudtodo[cloud]['nfilesfailed'] = 0
                cloudtodo[cloud]['nfiles'] += ds['nfiles']
                cloudtodo[cloud]['nfilesfinished'] += ds['nfilesfinished']
                cloudtodo[cloud]['nfilesfailed'] += ds['nfilesfailed']

        ## add info to slices
        sliceids = {}
        for s in slices:
            sliceids[s['id']] = s['slice']
        clones = {}
        for s in slices:
            if s['dataset_id']:
                s['dataset_id_html'] = s['dataset_id'].replace(s['brief'],'<b>%s</b>' % s['brief'])
            for ds in indatasets:
                if ds['name'] == s['dataset_id']: s['dataset_data'] = ds
            if 'cloned_from_id' in s and s['cloned_from_id'] and s['cloned_from_id'] in sliceids:
                    s['cloned_from'] = sliceids[s['cloned_from_id']]
                    if not sliceids[s['cloned_from_id']] in clones: clones[sliceids[s['cloned_from_id']]] = []
                    clones[sliceids[s['cloned_from_id']]].append(sliceids[s['id']])
        for s in slices:
            if s['slice'] in clones: s['clones'] = clones[s['slice']]

        taskjobd = {}
        if reqid:
            ## job counts per task
            tjquery = { 'reqid' : reqid, 'prodsourcelabel' : 'managed' }
            taskjobs = Jobsarchived4.objects.filter(**tjquery).values('jeditaskid','jobstatus').annotate(Count('jobstatus')).annotate(Sum('nevents')).order_by('jeditaskid','jobstatus')
            taskjobs_arch = Jobsarchived.objects.filter(**tjquery).values('jeditaskid','jobstatus').annotate(Count('jobstatus')).annotate(Sum('nevents')).order_by('jeditaskid','jobstatus')
            tjd = {}
            tjda = {}
            for j in taskjobs_arch:
                if j['jeditaskid'] not in tjda:
                    tjda[j['jeditaskid']] = {}
                    tjda[j['jeditaskid']]['totjobs'] = 0
                tjda[j['jeditaskid']]['totjobs'] += j['jobstatus__count']
                tjda[j['jeditaskid']][j['jobstatus']] = {}
                tjda[j['jeditaskid']][j['jobstatus']]['nevents'] = j['nevents__sum']
                tjda[j['jeditaskid']][j['jobstatus']]['njobs'] = j['jobstatus__count']
            for j in taskjobs:
                if j['jeditaskid'] not in tjda:
                    tjda[j['jeditaskid']] = {}
                    tjda[j['jeditaskid']]['totjobs'] = 0
                tjda[j['jeditaskid']]['totjobs'] += j['jobstatus__count']
            for j in taskjobs:
                if j['jeditaskid'] not in tjd: tjd[j['jeditaskid']] = {}
                if j['jeditaskid'] in tjda and j['jobstatus'] in tjda[j['jeditaskid']]:
                    j['nevents__sum'] += tjda[j['jeditaskid']][j['jobstatus']]['nevents']
                    j['jobstatus__count'] += tjda[j['jeditaskid']][j['jobstatus']]['njobs']
                pct = 100.*float(j['jobstatus__count'])/float(tjda[j['jeditaskid']]['totjobs'])
                pct = int(pct)
                print pct
                if j['nevents__sum'] > 0:
                    tjd[j['jeditaskid']][j['jobstatus']] = "<span class='%s'>%s:%.0f%% (%s)/%s evs</span>" % ( j['jobstatus'], j['jobstatus'], pct, j['jobstatus__count'], j['nevents__sum'] )
                else:
                    tjd[j['jeditaskid']][j['jobstatus']] = "<span class='%s'>%s:%.0f%% (%s)</span>" % ( j['jobstatus'], j['jobstatus'], pct, j['jobstatus__count'] )
            for t in tjd:
                tstates = []
                for s in tjd[t]:
                    tstates.append(tjd[t][s])
                tstates.sort()
                taskjobd[t] = tstates

        ## get the needed step templates (ctags)
        tasklist = []
        ctagd = {}
        for st in steps:
            ctagd[st['step_template_id']] = 1
        ctagl = ctagd.keys
        ctags = StepTemplate.objects.using('deft_adcr').filter(id__in=ctagl).order_by('ctag').values()
        for st in steps:
            ## add ctags to steps
            for ct in ctags:
                if st['step_template_id'] == ct['id']:
                    st['ctag'] = ct
            ## add ctag details
            if 'ctag' in st and st['ctag']['ctag'] in amitagd: st['ctagdetails'] = amitagd[st['ctag']['ctag']]
            evtag = "%s %s" % (st['ctag']['step'],st['ctag']['ctag'])
            ## add tasks to steps
            st['tasks'] = []
            for t in tasks:
                t['jedistatus'] = jeditaskstatus[t['id']]
                if t['id'] in taskjobd: t['jobstats'] = taskjobd[t['id']]
                if t['step_id'] == st['id']:
                    st['tasks'].append(t)
                    tasklist.append(t['name'])
                    if t['status'] not in ( 'aborted', 'broken' ):
                        if evtag not in events_processed: events_processed[evtag] = 0
                        events_processed[evtag] += t['total_events']            

        ## for each slice, add its steps
        for sl in slices:
            sl['steps'] = []
            for st in steps:
                if st['slice_id'] == sl['id']: sl['steps'].append(st)
            ## prepare dump of all fields
            reqd = {}
            colnames = []
            request_columns = []
            try:
                req = reqs[0]
                colnames = req.keys()
                colnames.sort()
                for k in colnames:
                    val = req[k]
                    if req[k] == None:
                        val = ''
                        continue
                    pair = { 'name' : k, 'value' : val }
                    request_columns.append(pair)
            except IndexError:
                reqd = {}

    ## gather summary info for request listings
    if reqid or (mode == 'request'):
        query = {}
        if reqid: query = { 'request' : reqid }

        ## slice counts per request
        slicecounts = InputRequestList.objects.using('deft_adcr').filter(**query).values('request').annotate(Count('request'))   
        nsliced = {}
        for s in slicecounts:
            nsliced[s['request']] = s['request__count']
        for r in reqs:
            if r['reqid'] in nsliced:
                r['nslices'] = nsliced[r['reqid']]
            else:
                r['nslices'] = None

        ## requested event counts, from slice info, where not set to -1
        reqevents = InputRequestList.objects.using('deft_adcr').filter(**query).values('request').annotate(Sum('input_events'))
        nreqevd = {}
        for t in reqevents:
            if t['input_events__sum'] > 0:
                nreqevd[t['request']] = t['input_events__sum']
            else:
                if t['request'] in dsevents: nreqevd[t['request']] = dsevents[t['request']]
        for r in reqs:
            if r['reqid'] in nreqevd:
                nEvents = float(nreqevd[r['reqid']])/1000.
                r['nrequestedevents'] = nEvents
            else:
                r['nrequestedevents'] = None

        ## task counts
        taskcounts = ProductionTask.objects.using('deft_adcr').filter(**query).values('request','step__step_template__step','status').annotate(Count('status')).order_by('request','step__step_template__step','status')
        ntaskd = {}
        for t in taskcounts:
            if t['request'] not in ntaskd: ntaskd[t['request']] = {}
            if t['step__step_template__step'] not in ntaskd[t['request']]: ntaskd[t['request']][t['step__step_template__step']] = {}
            ntaskd[t['request']][t['step__step_template__step']][t['status']] = t['status__count']
        for r in reqs:
            if r['reqid'] in ntaskd:
                stepl = []
                for istep in ntaskd[r['reqid']]:
                    statel = []
                    for istate in ntaskd[r['reqid']][istep]:
                        statel.append([istate, ntaskd[r['reqid']][istep][istate]])
                    statel.sort()
                    stepl.append([istep, statel])
                stepl.sort()
                r['ntasks'] = stepl
            else:
                r['ntasks'] = None

        ## cloud and core count info from JEDI tasks
        tcquery = { 'prodsourcelabel' : 'managed' }
        if reqid: tcquery['reqid'] = reqid
        startdate = timezone.now() - timedelta(hours=30*24)
        startdate = startdate.strftime(defaultDatetimeFormat)
        tcquery['modificationtime__gte'] = startdate
        taskcounts = JediTasks.objects.filter(**tcquery).values('reqid','processingtype','cloud','corecount','superstatus').annotate(Count('superstatus')).order_by('reqid','processingtype','cloud','corecount','superstatus')
        ntaskd = {}
        for t in taskcounts:
            if t['reqid'] not in ntaskd: ntaskd[t['reqid']] = {}
            if t['processingtype'] not in ntaskd[t['reqid']]: ntaskd[t['reqid']][t['processingtype']] = {}
            if t['cloud'] not in ntaskd[t['reqid']][t['processingtype']]: ntaskd[t['reqid']][t['processingtype']][t['cloud']] = {}
            if t['corecount'] not in ntaskd[t['reqid']][t['processingtype']][t['cloud']]: ntaskd[t['reqid']][t['processingtype']][t['cloud']][t['corecount']] = {}
            ntaskd[t['reqid']][t['processingtype']][t['cloud']][t['corecount']][t['superstatus']] = t['superstatus__count']

        for r in reqs:
            if r['reqid'] in ntaskd:
                ## get the input totals for each step
                steptotd = {}
                for typ, tval in ntaskd[r['reqid']].items():
                    for cloud, cval in tval.items():
                        if cloud in cloudtodo:
                            if typ not in steptotd: steptotd[typ] = 0
                            steptotd[typ] += cloudtodo[cloud]['nfiles']         
                ## build the table
                r['clouddist'] = ntaskd[r['reqid']]
                cdtxt = []
                for typ, tval in ntaskd[r['reqid']].items():
                    for cloud, cval in tval.items():
                        if cloud in cloudtodo:
                            tobedone = cloudtodo[cloud]['nfiles'] - cloudtodo[cloud]['nfilesfinished']
                            failtxt = ""
                            todotxt = ""
                            if cloud != '' and cloudtodo[cloud]['nfiles'] > 0:
                                if tobedone > 0:
                                    done = cloudtodo[cloud]['nfilesfinished']
                                    donepct = 100. * ( float(done) / float(cloudtodo[cloud]['nfiles']) )
                                    todotxt = "%.0f%% </td><td> <a href='/tasks/?reqid=%s&cloud=%s&processingtype=%s&days=90'>%s/%s</a> </td><td>" % (donepct, r['reqid'], cloud, typ, done, cloudtodo[cloud]['nfiles'])
                                    width = int(200.*cloudtodo[cloud]['nfiles']/steptotd[typ])
                                    todotxt += " &nbsp; <progress style='width:%spx' max='100' value='%s'></progress>" % (width, donepct )
                                else:
                                    todotxt = "<a href='/tasks/?reqid=%s&cloud=%s&processingtype=%s&days=90'>none still to do</a>" % ( r['reqid'], cloud, typ)
                                if cloudtodo[cloud]['nfilesfailed'] > 0:
                                    failtxt = " &nbsp; <font color=red>%.0f%% failed (%s inputs)</font>" % ( 100.*float(cloudtodo[cloud]['nfilesfailed'])/float(cloudtodo[cloud]['nfiles']), cloudtodo[cloud]['nfilesfailed'] )
                            if cloud != '':
                                txt = "<tr><td>%s</td><td>%s</td><td> %s </td><td> %s </td></tr>" % ( typ, cloud, todotxt, failtxt )
                                cdtxt.append(txt)
                        for ncore, nval in cval.items():
                            txt = "<tr><td>%s</td><td>%s</td><td colspan=20> <b>%s-core: " % ( typ, cloud, ncore )
                            states = nval.keys()
                            states.sort()
                            for s in states:
                                txt += " &nbsp; <span class='%s'>%s</span>:%s" % ( s, s, nval[s] )
                            txt += "</b></td></tr>"
                            cdtxt.append(txt)
                cdtxt.sort()
                r['clouddisttxt'] = cdtxt
            else:
                r['clouddist'] = None

        ## cloud and core count event production info from PanDA jobs
        tcquery = { 'prodsourcelabel' : 'managed' }
        if reqid: tcquery['reqid'] = reqid
        jobcounts = Jobsarchived4.objects.filter(**tcquery).values('reqid','processingtype','cloud','corecount','jobstatus').annotate(Count('jobstatus')).annotate(Sum('nevents')).order_by('reqid','processingtype','cloud','corecount','jobstatus')
        njobd = {}
        for t in jobcounts:
            if t['reqid'] not in njobd: njobd[t['reqid']] = {}
            if t['processingtype'] not in njobd[t['reqid']]: njobd[t['reqid']][t['processingtype']] = {}
            if t['cloud'] not in njobd[t['reqid']][t['processingtype']]: njobd[t['reqid']][t['processingtype']][t['cloud']] = {}
            if t['corecount'] not in njobd[t['reqid']][t['processingtype']][t['cloud']]: njobd[t['reqid']][t['processingtype']][t['cloud']][t['corecount']] = {}
            nev = float(t['nevents__sum'])/1000.
            njobd[t['reqid']][t['processingtype']][t['cloud']][t['corecount']][t['jobstatus']] = { 'events' : nev, 'jobs' : t['jobstatus__count'] }

        for r in reqs:
            if r['reqid'] in njobd:
                cdtxt = []
                for typ, tval in njobd[r['reqid']].items():
                    for cloud, cval in tval.items():
                        for ncore, nval in cval.items():
                            txt = "%s %s %s-core: " % ( typ, cloud, ncore )
                            states = nval.keys()
                            states.sort()
                            for s in states:
                                txt += " &nbsp; <span class='%s'>%s</span>:%sk evs (%s jobs)" % ( s, s, nval[s]['events'], nval[s]['jobs'] )
                            cdtxt.append(txt)
                cdtxt.sort()
                r['jobdisttxt'] = cdtxt

        ## processed event counts, from prodsys task info
        eventcounts = ProductionTask.objects.using('deft_adcr').filter(**query).exclude(status__in=['aborted','broken']).values('request','step__step_template__step').annotate(Sum('total_events'))
        ntaskd = {}
        for t in eventcounts:
            if t['request'] not in ntaskd: ntaskd[t['request']] = {}
            ntaskd[t['request']][t['step__step_template__step']] = t['total_events__sum']
        for r in reqs:
            if r['reqid'] in ntaskd:
                stepl = []
                for istep in ntaskd[r['reqid']]:
                    nEvents = float(ntaskd[r['reqid']][istep])/1000.
                    stepl.append([istep, nEvents ])
                stepl.sort()
                r['nprocessedevents'] = stepl
                if r['nrequestedevents'] and r['nrequestedevents'] > 0:
                    if 'completedevpct' not in r: r['completedevpct'] = []
                    for istep in ntaskd[r['reqid']]:
                        ndone = float(ntaskd[r['reqid']][istep])/1000.
                        nreq = r['nrequestedevents']
                        pct = int(ndone / nreq * 100.)
                        r['completedevpct'].append([ istep, pct ])
                    r['completedevpct'].sort()
            else:
                r['nprocessedevents'] = None

        ## processed event counts, from job info (last 3 days only)
        query = {}
        if reqid: query['reqid'] = reqid
        query['jobstatus'] = 'finished'
        query['prodsourcelabel'] = 'managed'
        values = [ 'reqid', 'processingtype' ]
        jobsum = Jobsarchived4.objects.filter(**query).values(*values).annotate(Sum('nevents')).order_by('reqid', 'processingtype')
        jobsumd = {}
        for j in jobsum:
            j['nevents__sum'] = float(j['nevents__sum'])/1000.
            if j['reqid'] not in jobsumd: jobsumd[j['reqid']] = []
            jobsumd[j['reqid']].append([ j['processingtype'], j['nevents__sum'] ])
        for r in reqs:
            if r['reqid'] in jobsumd:
                r['jobsumd'] = jobsumd[r['reqid']]
            else:
                r['jobsumd'] = None

    ## dataset search mode
    njeditasks = 0
    if dataset:
        if dataset.endswith('/'): dataset = dataset.strip('/')
        if dataset.find(':') >= 0:
            scope = dataset[:dataset.find(':')]
            dataset = dataset[dataset.find(':')+1:]
        mat = re.match('.*(_tid[0-9\_]+)$', dataset)
        if mat:
            tid = mat.group(1)
            dataset = dataset.replace(tid,'')        
            mat = re.match('_tid[0]*([0-9]+)', tid)
            if mat: tidnum = int(mat.group(1))

        fields = dataset.split('.')
        if len(fields) == 6:
            # try to interpret
            format = fields[5]
            taskname = '%s.%s.%s.%s.%s' % ( fields[0], fields[1], fields[2], fields[3], fields[5] )
            jeditasks = JediTasks.objects.filter(taskname=taskname,tasktype='prod').order_by('jeditaskid').values()

        if len(jeditasks) > 0:
            # get associated datasets
            tlist = []
            for t in jeditasks: tlist.append(t['jeditaskid'])
            dsquery = {}
            dsquery['jeditaskid__in'] = tlist
            dsets = JediDatasets.objects.filter(**dsquery).values()
            dsetd = {}
            for ds in dsets:
                if ds['type'] == 'pseudo_input': continue
                if ds['jeditaskid'] not in dsetd: dsetd[ds['jeditaskid']] = []
                dsetd[ds['jeditaskid']].append(ds)
            for t in jeditasks:
                if t['jeditaskid'] in dsetd:
                    dsetd[t['jeditaskid']] = sorted(dsetd[t['jeditaskid']], key=lambda x:x['datasetname'], reverse=True)
                    t['datasets'] = dsetd[t['jeditaskid']]
            # mark the tasks that have the exact dataset (modulo tid)
            for t in jeditasks:
                has_dataset = False
                for ds in t['datasets']:
                    if ds['datasetname'].startswith(dataset):
                        has_dataset = True
                        njeditasks += 1
                t['has_dataset'] = has_dataset

        datasets = ProductionDataset.objects.using('deft_adcr').filter(name__startswith=dataset).values()
        if len(datasets) == 0:
            messages.info(request, "No matching datasets found")
        else:
            messages.info(request, "%s matching prodsys datasets found" % len(datasets))

        if len(datasets) > 0:
            # get production tasks associated with datasets
            tlist = []
            for ds in datasets:
                step = ds
                tlist.append(ds['task_id'])
            tquery = {}
            tquery['id__in'] = tlist
            ptasks = ProductionTask.objects.using('deft_adcr').filter(**tquery)
            taskd = {}
            for t in ptasks:
                taskd[t.id] = t
            for ds in datasets:
                if ds['task_id'] in taskd: ds['ptask'] = taskd[ds['task_id']]

        containers = ProductionContainer.objects.using('deft_adcr').filter(name__startswith=dataset).values()        
        if len(containers) == 0:
            messages.info(request, "No matching containers found")
        else:
            messages.info(request, "%s matching containers found" % len(containers))
        dsslices = InputRequestList.objects.using('deft_adcr').filter(dataset__name__startswith=dataset).values()
        if len(dsslices) == 0:
            pass # messages.info(request, "No slices using this dataset found")
        else:
            messages.info(request, "%s slices found" % len(dsslices))
        jedidatasets = JediDatasets.objects.filter(datasetname__startswith=dataset).order_by('jeditaskid').values()
        if len(jedidatasets) == 0:
            pass # messages.info(request, "No matching JEDI datasets found")
        else:
            messages.info(request, "%s matching JEDI datasets found" % len(jedidatasets))

        ## check for jobs with output destined for this dataset
        files = []
        query = { 'dataset' : dataset }
        if 'pandaid' in request.session['requestParams']: query['pandaid'] = request.session['requestParams']['pandaid']
        #files.extend(Filestable4.objects.filter(dataset=dataset,type='output').order_by('pandaid').values())
        #files.extend(FilestableArch.objects.filter(dataset=dataset,type='output').order_by('pandaid').values())
        #if len(files) == 0: messages.info(request, "No PanDA jobs creating files for this dataset found")

    reqsuml = attSummaryDict(request, reqs, req_fields)
    showfields = list(jeditask_fields)
    showfields.remove('jeditaskid')
    jtasksuml = attSummaryDict(request, jeditasks, showfields)

    if events_processed:
        # Convert from dict to ordered list
        evkeys = events_processed.keys()
        evkeys.sort()
        evpl = []
        for e in evkeys:
            evpl.append([e, float(events_processed[e])/1000.])
        events_processed = evpl

    if 'sortby' in request.session['requestParams']:
        if reqs:
            if request.session['requestParams']['sortby'] == 'reqid':
                reqs = sorted(reqs, key=lambda x:x['reqid'], reverse=True)
            if request.session['requestParams']['sortby'] == 'timestamp':
                reqs = sorted(reqs, key=lambda x:x['timestamp'], reverse=True)
    if len(reqs) > 0 and 'info_fields' in reqs[0] and reqs[0]['info_fields']:
        info_fields = json.loads(reqs[0]['info_fields'])
    else:
        info_fields = None
    if len(reqs) > 0:
        req = reqs[0]
    else:
        req = None
    xurl = views.extensibleURL(request)
    nosorturl = views.removeParam(xurl, 'sortby',mode='extensible')
    data = {
        'viewParams' : viewParams,
        'xurl' : xurl,
        'nosorturl' : nosorturl,
        'mode' : mode,
        'reqid' : reqid,
        'dataset' : dataset,
        'thisProject' : thisProject,
        'projects' : projectd,
        'request' : req,
        'info_fields' : info_fields,
        'requests' : reqs,
        'reqsuml' : reqsuml,
        'jtasksuml' : jtasksuml,
        'datasets' : datasets,
        'jedidatasets' : jedidatasets,
        'jeditasks' : jeditasks,
        'njeditasks' : njeditasks,
        'containers' : containers,
        'tasks' : tasks,
        'steps' : steps,
        'slices' : slices,
        'files' : files,
        'dataset_form' : dataset_form,
        'events_processed' : events_processed,
        'request_columns' : request_columns,
        'jobsum' : jobsum,
        'jobsumd' : jobsumd,
        'dsslices' : dsslices,
        'scope' : scope,
        'tid' : tid,
        'tidnum' : tidnum,
    }
    response = render_to_response('dpMain.html', data, RequestContext(request))
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes']*60)
    return response

def attSummaryDict(request, reqs, flist):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    
    for req in reqs:
        for f in flist:
            if f in req and req[f]:
                if not f in sumd: sumd[f] = {}
                if not req[f] in sumd[f]: sumd[f][req[f]] = 0
                sumd[f][req[f]] += 1

    ## convert to ordered lists
    suml = []
    for f in sumd:
        itemd = {}
        itemd['field'] = f
        iteml = []
        kys = sumd[f].keys()
        kys.sort()
        for ky in kys:
            iteml.append({ 'kname' : ky, 'kvalue' : sumd[f][ky] })
        if 'sortby' in request.GET and request.GET['sortby'] == 'count':
            iteml = sorted(iteml, key=lambda x:x['kvalue'], reverse=True)
        else:
            iteml = sorted(iteml, key=lambda x:str(x['kname']).lower())
        itemd['list'] = iteml
        suml.append(itemd)
        suml = sorted(suml, key=lambda x:x['field'])
    return suml

class DatasetForm(forms.Form):
    #class Media:
    #    css = {"all": ("app.css",)}

    #error_css_class = 'error'
    #required_css_class = 'required'

    def clean_entname(self):
        exists, txt = checkExistingTag(self.cleaned_data['dataset'])
        return self.cleaned_data['dataset']

    type = forms.ChoiceField(label='Entry type', widget=forms.HiddenInput(), choices=entitytypes, initial='dataset' )

    dataset = forms.CharField(label='Dataset', max_length=250, \
            help_text="Enter dataset or container name")
