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
from django.db.models import Count
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

#from settings.local import aws

# A crapshoot whether they're available because of the branch mess
try:
    from atlas.prodtask.models import TRequest, TProject, RequestStatus, ProductionTask, StepTemplate, StepExecution, InputRequestList, ProductionContainer, ProductionDataset
except:
    pass
from core.common.models import JediTasks
from core.common.models import Filestable4 
from core.common.models import FilestableArch
from core.common.models import JediDatasets
from core.common.settings.config import ENV

import views

ENV['MON_VO'] = 'ATLAS'
viewParams = {}
viewParams['MON_VO'] = ENV['MON_VO']

req_fields = [ 'project_id', 'phys_group', 'campaign', 'manager', 'provenance', 'request_type', 'project', 'is_fast' ]
jeditask_fields = [ 'cloud', 'processingtype', 'superstatus', 'status' ]

# entity types supported in searches
entitytypes = [
    [ 'dataset', 'Datasets and containers' ],
    ]

## Open Amazon DynamoDB databases
#dyndb = DynamoDBConnection(aws_access_key_id=aws['AWS_ACCESS_KEY_ATLAS'], aws_secret_access_key=aws['AWS_SECRET_KEY_ATLAS'])
#usertable = Table('user', connection=dyndb)
#projecttable = Table('project', connection=dyndb)
#requesttable = Table('request', connection=dyndb)

def doRequest(request):

    ## by default, show main intro
    mode = 'intro'
    if 'mode' in request.GET: mode = request.GET['mode']
    query = {}
    dataset = None
    reqid = None
    dataset_form = DatasetForm()

    if request.method == 'POST':

        ## if POST, we have a form input to process
        formdata = request.POST.copy()
        requestParams = formdata
        #if 'action' in request.POST and request.POST['action'] == 'edit_from_db':

        if request.user.is_authenticated():
            formdata['requester'] = request.user.get_full_name()
        print 'formdata', formdata

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
        requestParams = request.GET.copy()
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
        else:
            query['reqid__gte'] = 920

    #projects = projecttable.scan()
    #projects = TProject.objects.using('deft_adcr').all().values()
    projectd = {}
    #for p in projects:
    #    projectd[p['project']] = p['description']
    projects = TProject.objects.using('deft_adcr').all().values()
    for p in projects:
        projectd[p['project']] = p

    reqs = []
    thisProject = None
    if mode in ('request', 'reqid'):
        reqs = TRequest.objects.using('deft_adcr').filter(**query).order_by('reqid').reverse().values()
    for r in reqs:
        if 'project_id' in r and r['project_id']:
            r['projectdata'] = projectd[r['project_id']]
        else:
            r['projectdata'] = None
    if reqid: print reqs
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

    datasets = containers = tasks = jeditasks = jedidatasets = steps = slices = files = []
    events_processed = None
    request_columns = None
    if reqid:
        events_processed = {}
        ## Prepare information for the particular request
        datasets = ProductionDataset.objects.using('deft_adcr').filter(rid=reqid).order_by('name').values()
        containers = ProductionContainer.objects.using('deft_adcr').filter(rid=reqid).order_by('name').values()
        tasks = ProductionTask.objects.using('deft_adcr').filter(request_id=reqid).order_by('id').values()
        steps = StepExecution.objects.using('deft_adcr').filter(request_id=reqid).values()
        slices = InputRequestList.objects.using('deft_adcr').filter(request_id=reqid).order_by('slice').values()
        jeditasks = JediTasks.objects.filter(reqid=reqid,tasktype='prod').values(*jeditask_fields)

        ## get records for input datasets
        indslist = []
        for s in slices:
            if s['dataset_id']: indslist.append(s['dataset_id'])
        indatasets = ProductionDataset.objects.using('deft_adcr').filter(name__in=indslist).values()

        ## get records for input containers
        indslist = []
        for s in slices:
            if s['dataset_id']: indslist.append(s['dataset_id'])
        incontainers = ProductionContainer.objects.using('deft_adcr').filter(name__in=indslist).values()
        print 'containers', incontainers

        ## add info to slices
        for s in slices:
            if s['dataset_id']:
                s['dataset_id_html'] = s['dataset_id'].replace(s['brief'],'<b>%s</b>' % s['brief'])
            for ds in indatasets:
                if ds['name'] == s['dataset_id']: s['dataset_data'] = ds

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
            evtag = "%s %s" % (st['ctag']['step'],st['ctag']['ctag'])
            ## add tasks to steps
            st['tasks'] = []
            for t in tasks:
                if t['step_id'] == st['id']:
                    st['tasks'].append(t)
                    tasklist.append(t['name'])
                    if t['status'] not in ( 'aborted', 'broken' ):
                        if evtag not in events_processed: events_processed[evtag] = 0
                        events_processed[evtag] += t['total_events']            
        ## get the needed task records

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
    elif mode == 'request':
        slicecounts = InputRequestList.objects.using('deft_adcr').all().values('request').annotate(Count('request'))   
        nsliced = {}
        for s in slicecounts:
            nsliced[s['request']] = s['request__count']
        for r in reqs:
            if r['reqid'] in nsliced:
                r['nslices'] = nsliced[r['reqid']]
            else:
                r['nslices'] = None
        taskcounts = ProductionTask.objects.using('deft_adcr').all().values('request__reqid','status').annotate(Count('status')).order_by('request__reqid','status')
        ntaskd = {}
        for t in taskcounts:
            if t['request__reqid'] not in ntaskd: ntaskd[t['request__reqid']] = {}
            ntaskd[t['request__reqid']][t['status']] = t['status__count']
        for r in reqs:
            if r['reqid'] in ntaskd:
                sl = []
                for s in ntaskd[r['reqid']]:
                    sl.append([s, ntaskd[r['reqid']][s]])
                sl.sort()
                r['ntasks'] = sl
            else:
                r['ntasks'] = None

    if dataset:
        print 'dataset', dataset
        datasets = ProductionDataset.objects.using('deft_adcr').filter(name__startswith=dataset).values()
        if len(datasets) == 0: messages.info(request, "No matching datasets found")
        print 'datasets', datasets
        containers = ProductionContainer.objects.using('deft_adcr').filter(name__startswith=dataset).values()
        if len(containers) == 0: messages.info(request, "No matching containers found")
        print 'containers', containers
        slices = InputRequestList.objects.using('deft_adcr').filter(dataset__name__startswith=dataset).values()
        if len(slices) == 0: messages.info(request, "No slices using this dataset found")
        print 'slices', slices
        jedidatasets = JediDatasets.objects.filter(datasetname=dataset).values()
        if len(jedidatasets) == 0: messages.info(request, "No matching JEDI datasets found")

        ## check for jobs with output destined for this dataset
        files = []
        query = { 'dataset' : dataset }
        if 'pandaid' in requestParams: query['pandaid'] = requestParams['pandaid']
        #files.extend(Filestable4.objects.filter(dataset=dataset,type='output').order_by('pandaid').values())
        #files.extend(FilestableArch.objects.filter(dataset=dataset,type='output').order_by('pandaid').values())
        #if len(files) == 0: messages.info(request, "No PanDA jobs creating files for this dataset found")

    reqsuml = attSummaryDict(request, reqs, req_fields)
    jtasksuml = attSummaryDict(request, jeditasks, jeditask_fields)

    if events_processed:
        # Convert from dict to ordered list
        evkeys = events_processed.keys()
        evkeys.sort()
        evpl = []
        for e in evkeys:
            evpl.append([e, float(events_processed[e])/1000000.])
        events_processed = evpl
    print events_processed

    if 'sortby' in requestParams:
        if reqs:
            if requestParams['sortby'] == 'reqid':
                reqs = sorted(reqs, key=lambda x:x['reqid'], reverse=True)
            if requestParams['sortby'] == 'timestamp':
                reqs = sorted(reqs, key=lambda x:x['timestamp'], reverse=True)

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
        'requests' : reqs,
        'reqsuml' : reqsuml,
        'jtasksuml' : jtasksuml,
        'datasets' : datasets,
        'jedidatasets' : jedidatasets,
        'containers' : containers,
        'tasks' : tasks,
        'steps' : steps,
        'slices' : slices,
        'files' : files,
        'dataset_form' : dataset_form,
        'events_processed' : events_processed,
        'request_columns' : request_columns,
    }
    response = render_to_response('dpMain.html', data, RequestContext(request))
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
