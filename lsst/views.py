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

from core.common.utils import getPrefix, getContextVariables, QuerySetChain
from core.common.settings import STATIC_URL, FILTER_UI_ENV, defaultDatetimeFormat
from core.pandajob.models import PandaJob, Jobsactive4, Jobsdefined4, Jobswaiting4, Jobsarchived4, Jobsarchived
from core.resource.models import Schedconfig
from core.common.models import Filestable4 
from core.common.models import Datasets
from core.common.models import Sitedata
from core.common.models import FilestableArch
from core.common.models import Users
from core.common.models import Jobparamstable
from core.common.models import JobparamstableArch
from core.common.models import Metatable
from core.common.models import MetatableArch
from core.common.models import Logstable
from core.common.models import Jobsdebug
from core.common.models import Cloudconfig
from core.common.models import Incidents
from core.common.models import Pandalog
from core.common.models import JediJobRetryHistory
from core.common.models import JediTasks
from core.common.models import Etask
from core.common.models import JediTaskparams
from core.common.models import JediEvents
from core.common.models import JediDatasets
from core.common.models import JediDatasetContents
from core.common.models import JediWorkQueue
from core.common.settings.config import ENV

from settings.local import dbaccess
import ErrorCodes
errorFields = []
errorCodes = {}
errorStages = {}

try:
    hostname = commands.getoutput('hostname')
    if hostname.find('.') > 0: hostname = hostname[:hostname.find('.')]
except:
    hostname = ''

callCount = 0
homeCloud = {}
objectStores = {}
pandaSites = {}
cloudList = [ 'CA', 'CERN', 'DE', 'ES', 'FR', 'IT', 'ND', 'NL', 'RU', 'TW', 'UK', 'US' ]

statelist = [ 'defined', 'waiting', 'pending', 'assigned', 'throttled', \
             'activated', 'sent', 'starting', 'running', 'holding', \
             'transferring', 'finished', 'failed', 'cancelled', 'merging']
sitestatelist = [ 'defined', 'waiting', 'assigned', 'throttled',  'activated', 'sent', 'starting', 'running', 'holding', 'transferring', 'finished', 'failed', 'cancelled' ]
eventservicestatelist = [ 'ready', 'sent', 'running', 'finished', 'cancelled', 'discarded', 'done' ]
taskstatelist = [ 'registered', 'defined', 'assigning', 'ready', 'pending', 'scouting', 'scouted', 'running', 'prepared', 'done', 'failed', 'finished', 'aborting', 'aborted', 'finishing', 'topreprocess', 'preprocessing', 'tobroken', 'broken', 'toretry', 'toincexec', 'rerefine' ]
taskstatelist_short = [ 'reg', 'def', 'assgn', 'rdy', 'pend', 'scout', 'sctd', 'run', 'prep', 'done', 'fail', 'finish', 'abrtg', 'abrtd', 'finishg', 'toprep', 'preprc', 'tobrok', 'broken', 'retry', 'incexe', 'refine' ]
taskstatedict = []
for i in range (0, len(taskstatelist)):
    tsdict = { 'state' : taskstatelist[i], 'short' : taskstatelist_short[i] }
    taskstatedict.append(tsdict)


errorcodelist = [ 
    { 'name' : 'brokerage', 'error' : 'brokerageerrorcode', 'diag' : 'brokerageerrordiag' },
    { 'name' : 'ddm', 'error' : 'ddmerrorcode', 'diag' : 'ddmerrordiag' },
    { 'name' : 'exe', 'error' : 'exeerrorcode', 'diag' : 'exeerrordiag' },
    { 'name' : 'jobdispatcher', 'error' : 'jobdispatchererrorcode', 'diag' : 'jobdispatchererrordiag' },
    { 'name' : 'pilot', 'error' : 'piloterrorcode', 'diag' : 'piloterrordiag' },
    { 'name' : 'sup', 'error' : 'superrorcode', 'diag' : 'superrordiag' },
    { 'name' : 'taskbuffer', 'error' : 'taskbuffererrorcode', 'diag' : 'taskbuffererrordiag' },
    { 'name' : 'transformation', 'error' : 'transexitcode', 'diag' : None },
    ]


_logger = logging.getLogger('bigpandamon')
viewParams = {}
requestParams = {}

LAST_N_HOURS_MAX = 0
JOB_LIMIT = 0
TFIRST = timezone.now()
TLAST = timezone.now() - timedelta(hours=2400)
PLOW = 1000000
PHIGH = -1000000

standard_fields = [ 'processingtype', 'computingsite', 'destinationse', 'jobstatus', 'prodsourcelabel', 'produsername', 'jeditaskid', 'workinggroup', 'transformation', 'cloud', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr', 'computingelement', 'specialhandling', 'priorityrange' ]
standard_sitefields = [ 'region', 'gocname', 'nickname', 'status', 'tier', 'comment_field', 'cloud', 'allowdirectaccess', 'allowfax', 'copytool', 'faxredirector', 'retry', 'timefloor' ]
standard_taskfields = [ 'tasktype', 'superstatus', 'corecount', 'taskpriority', 'username', 'transuses', 'transpath', 'workinggroup', 'processingtype', 'cloud', 'campaign', 'project', 'stream', 'tag', 'reqid', ]

VOLIST = [ 'atlas', 'bigpanda', 'htcondor', 'lsst', ]
VONAME = { 'atlas' : 'ATLAS', 'bigpanda' : 'BigPanDA', 'htcondor' : 'HTCondor', 'lsst' : 'LSST', '' : '' }
VOMODE = ' '

def setupSiteInfo():
    global homeCloud, objectStores, pandaSites, callCount
    callCount += 1
    if len(homeCloud) > 0 and callCount%100 != 1 and 'refresh' not in requestParams: return
    sflist = ('siteid','status','cloud','tier','comment_field','objectstore','catchall')
    sites = Schedconfig.objects.filter().exclude(cloud='CMS').values(*sflist)
    for site in sites:
        pandaSites[site['siteid']] = {}
        for f in ( 'siteid', 'status', 'tier', 'comment_field', 'cloud' ):
            pandaSites[site['siteid']][f] = site[f]
        homeCloud[site['siteid']] = site['cloud']
        if site['catchall'].find('log_to_objectstore') >= 0 or site['objectstore'] != '':
            #print 'object store site', site['siteid'], site['catchall'], site['objectstore']
            try:
                fpath = getFilePathForObjectStore(site['objectstore'],filetype="logs")
                #### dirty hack
                fpath = fpath.replace('root://atlas-objectstore.cern.ch/atlas/logs','https://atlas-objectstore.cern.ch:1094/atlas/logs')
                if fpath != "" and fpath.startswith('http'): objectStores[site['siteid']] = fpath
            except:
                pass

def initRequest(request):
    global VOMODE, ENV, viewParams, hostname
    if 'USER' in os.environ and os.environ['USER'] != 'apache':
        request.session['debug'] = True
    elif 'debug' in request.GET and request.GET['debug'] == 'insider':
        request.session['debug'] = True
    else:
        request.session['debug'] = False

    if len(hostname) > 0: request.session['hostname'] = hostname

    ## Set default page lifetime in the http header, for the use of the front end cache
    request.session['max_age_minutes'] = 3

    ## Is it an https connection with a legit cert presented by the user?
    if 'SSL_CLIENT_S_DN' in request.META:
        request.session['userdn'] = request.META['SSL_CLIENT_S_DN']
        userrec = Users.objects.filter(dn__startswith=request.session['userdn']).values()
        if len(userrec) > 0:
            request.session['username'] = userrec[0]['name']

    ENV['MON_VO'] = ''
    viewParams['MON_VO'] = ''
    VOMODE = ''
    for vo in VOLIST:
        if request.META['HTTP_HOST'].startswith(vo):
            VOMODE = vo
    ## If DB is Oracle, set vomode to atlas
    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        VOMODE = 'atlas'
    ENV['MON_VO'] = VONAME[VOMODE]
    viewParams['MON_VO'] = ENV['MON_VO']
    global requestParams
    global errorFields, errorCodes, errorStages
    requestParams = {}

    if request.method == 'POST':
        for p in request.POST:
            if p in ( 'csrfmiddlewaretoken', ): continue
            pval = request.POST[p]
            pval = pval.replace('+',' ')
            requestParams[p.lower()] = pval
    else:
        for p in request.GET:
            pval = request.GET[p]
            pval = pval.replace('+',' ')
            pval = pval.replace('#','')
            ## is it int, if it's supposed to be?
            if p.lower() in ( 'days', 'hours', 'limit', 'display_limit', 'taskid', 'jeditaskid', 'jobsetid', 'corecount', 'taskpriority', 'priority', 'attemptnr', 'statenotupdated', 'tasknotupdated', ):
                try:
                    i = int(request.GET[p])
                except:
                    data = {
                        'viewParams' : viewParams,
                        'requestParams' : requestParams,
                        "errormessage" : "Illegal value '%s' for %s" % ( pval, p ),
                        }
                    return False, render_to_response('errorPage.html', data, RequestContext(request))
            requestParams[p.lower()] = pval
    setupSiteInfo()
    if len(errorFields) == 0:
        codes = ErrorCodes.ErrorCodes()
        errorFields, errorCodes, errorStages = codes.getErrorCodes()
    return True, None

def setupView(request, opmode='', hours=0, limit=-99, querytype='job'):
    global viewParams
    global LAST_N_HOURS_MAX, JOB_LIMIT
    deepquery = False
    fields = standard_fields
    if 'limit' in requestParams:
        JOB_LIMIT = int(requestParams['limit'])
    elif limit != -99 and limit > 0:
        JOB_LIMIT = limit
    elif VOMODE == 'atlas':
        JOB_LIMIT = 6000
    else:
        JOB_LIMIT = 10000

    if VOMODE == 'atlas':
        LAST_N_HOURS_MAX = 12
    else:
        LAST_N_HOURS_MAX = 7*24

    if VOMODE == 'atlas':
        if 'cloud' not in fields: fields.append('cloud')
        if 'atlasrelease' not in fields: fields.append('atlasrelease')
        if 'produsername' in requestParams or 'jeditaskid' in requestParams or 'user' in requestParams:
            if 'jobsetid' not in fields: fields.append('jobsetid')
            if ('hours' not in requestParams) and ('days' not in requestParams) and ('jobsetid' in requestParams or 'taskid' in requestParams or 'jeditaskid' in requestParams):
                ## Cases where deep query is safe
                deepquery = True
        else:
            if 'jobsetid' in fields: fields.remove('jobsetid')
    else:
        fields.append('vo')
    if hours > 0:
        ## Call param overrides default hours, but not a param on the URL
        LAST_N_HOURS_MAX = hours
    ## For site-specific queries, allow longer time window
    if 'computingsite' in requestParams:
        LAST_N_HOURS_MAX = 12
    if 'jobtype' in requestParams and requestParams['jobtype'] == 'eventservice':
        LAST_N_HOURS_MAX = 3*24
    ## hours specified in the URL takes priority over the above
    if 'hours' in requestParams:
        LAST_N_HOURS_MAX = int(requestParams['hours'])
    if 'days' in requestParams:
        LAST_N_HOURS_MAX = int(requestParams['days'])*24
    ## Exempt single-job, single-task etc queries from time constraint
    if 'jeditaskid' in requestParams: deepquery = True
    if 'taskid' in requestParams: deepquery = True
    if 'pandaid' in requestParams: deepquery = True
    if 'jobname' in requestParams: deepquery = True
    if 'batchid' in requestParams: deepquery = True
    if deepquery:
        opmode = 'notime'
        hours = LAST_N_HOURS_MAX = 24*180
        JOB_LIMIT = 999999
    if opmode != 'notime':
        if LAST_N_HOURS_MAX <= 72 :
            viewParams['selection'] = ", last %s hours" % LAST_N_HOURS_MAX
        else:
            viewParams['selection'] = ", last %d days" % (float(LAST_N_HOURS_MAX)/24.)
        if JOB_LIMIT < 999999 and JOB_LIMIT > 0:
            viewParams['selection'] += ", limit %s per table" % JOB_LIMIT
        viewParams['selection'] += ". &nbsp; <b>Params:</b> "
        #if 'days' not in requestParams:
        #    viewParams['selection'] += "hours=%s" % LAST_N_HOURS_MAX
        #else:
        #    viewParams['selection'] += "days=%s" % int(LAST_N_HOURS_MAX/24)
        if JOB_LIMIT < 100000 and JOB_LIMIT > 0:
            viewParams['selection'] += "  &nbsp; <b>limit=</b>%s" % JOB_LIMIT
    else:
        viewParams['selection'] = ""
    for param in requestParams:
        if requestParams[param] == 'None': continue
        if requestParams[param] == '': continue
        if param == 'display_limit': continue
        if param == 'sortby': continue
        viewParams['selection'] += "  &nbsp; <b>%s=</b>%s " % ( param, requestParams[param] )

    startdate = None
    if 'date_from' in requestParams:
        time_from_struct = time.strptime(requestParams['date_from'],'%Y-%m-%d')
        startdate = datetime.utcfromtimestamp(time.mktime(time_from_struct)).strftime(defaultDatetimeFormat)
    if not startdate:
        startdate = timezone.now() - timedelta(hours=LAST_N_HOURS_MAX)
        startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = None
    if 'date_to' in requestParams:
        time_from_struct = time.strptime(requestParams['date_to'],'%Y-%m-%d')
        enddate = datetime.utcfromtimestamp(time.mktime(time_from_struct)).strftime(defaultDatetimeFormat)
    if 'earlierthan' in requestParams:
        enddate = timezone.now() - timedelta(hours=int(requestParams['earlierthan']))
        enddate = enddate.strftime(defaultDatetimeFormat)
    if 'earlierthandays' in requestParams:
        enddate = timezone.now() - timedelta(hours=int(requestParams['earlierthandays'])*24)
        enddate = enddate.strftime(defaultDatetimeFormat)
    if enddate == None:
        enddate = timezone.now().strftime(defaultDatetimeFormat)
    query = { 'modificationtime__range' : [startdate, enddate] }
    ### Add any extensions to the query determined from the URL
    for vo in [ 'atlas', 'lsst' ]:
        if request.META['HTTP_HOST'].startswith(vo):
            query['vo'] = vo
    for param in requestParams:
        if param in ('hours', 'days'): continue
        if param == 'cloud' and requestParams[param] == 'All': continue
        elif param == 'priorityrange':
            mat = re.match('([0-9]+)\:([0-9]+)', requestParams[param])
            if mat:
                plo = int(mat.group(1))
                phi = int(mat.group(2))
                query['currentpriority__gte'] = plo
                query['currentpriority__lte'] = phi                
        elif param == 'jobsetrange':
            mat = re.match('([0-9]+)\:([0-9]+)', requestParams[param])
            if mat:
                plo = int(mat.group(1))
                phi = int(mat.group(2))
                query['jobsetid__gte'] = plo
                query['jobsetid__lte'] = phi 
        elif param == 'user' or param == 'username':
            if querytype == 'task':
                query['username__icontains'] = requestParams[param].strip()
            else:
                query['produsername__icontains'] = requestParams[param].strip()
        elif param in ( 'project', ) and querytype == 'task':
            val = requestParams[param]
            query['taskname__istartswith'] = val
        elif param in ( 'outputfiletype', ) and querytype != 'task':
            val = requestParams[param]
            query['destinationdblock__icontains'] = val
        elif param in ( 'stream', 'tag') and querytype == 'task':
            val = requestParams[param]
            query['taskname__icontains'] = val
        elif param == 'reqid_from' and querytype == 'task':
            val = int(requestParams[param])
            query['reqid__gte'] = val
        elif param == 'reqid_to' and querytype == 'task':
            val = int(requestParams[param])
            query['reqid__lte'] = val
        elif param == 'reqid_from' and querytype != 'task':
            ## Obtain the job list corresponding to reqid range, via tasks
            tquery = {}
            tquery['reqid__gte'] = int(requestParams[param])
            if 'reqid_to' in requestParams: tquery['reqid__lte'] = int(requestParams['reqid_to'])
            print "Task query to get reqids:", tquery
            tres = JediTasks.objects.filter(**tquery).values('jeditaskid')
            tlist = []
            for res in tres:
                tlist.append(res['jeditaskid'])
            query['jeditaskid__in'] = tlist
        elif param == 'reqid_to' and querytype != 'task':
            pass

        if querytype == 'task':
            for field in JediTasks._meta.get_all_field_names():
                        #for param in requestParams:
                        if param == field:
                            if param == 'transpath':
                                query['%s__endswith' % param] = requestParams[param]
                            elif param in ( 'taskname', ):
                                ## support wildcarding
                                if requestParams[param][1:-1].find('*') > 0:
                                    ## If there are embedded wildcards...
                                    tokens = requestParams[param].split('*')
                                    for t in tokens:
                                        if len(t) == 0: continue
                                        if t == tokens[0]:
                                            query['%s__istartswith' % param] = t
                                        elif t == tokens[-1]:
                                            query['%s__iendswith' % param] = t
                                        else:
                                            query['%s__icontains' % param] = t
                                elif requestParams[param].startswith('*') and requestParams[param].endswith('*'):
                                    query['%s__icontains' % param] = requestParams[param].replace('*','')
                                elif requestParams[param].endswith('*'):
                                    query['%s__istartswith' % param] = requestParams[param].replace('*','')                                    
                                    #query['%s__icontains' % param] = [ requestParams[param].replace('*',''), ]
                                elif requestParams[param].startswith('*'):
                                    query['%s__iendswith' % param] = requestParams[param].replace('*','')
                                    #query['%s__icontains' % param] = requestParams[param].replace('*','')
                                else:
                                    query[param] = requestParams[param]
                            elif param == 'tasktype':
                                ttype = requestParams[param]
                                if ttype.startswith('anal'): ttype='anal'
                                elif ttype.startswith('prod'): ttype='prod'
                                query[param] = ttype
                            else:
                                query[param] = requestParams[param]
                        if param == 'eventservice':
                            query['eventservice'] = 1
        else:
            for field in Jobsactive4._meta.get_all_field_names():
                if param == field:
                    if param == 'specialhandling':
                        query['specialhandling__contains'] = requestParams[param]
                    elif param == 'transformation' or param == 'transpath':
                        query['%s__endswith' % param] = requestParams[param]
                    elif param == 'modificationhost' and requestParams[param].find('@') < 0:
                        query['%s__contains' % param] = requestParams[param]
                    elif param == 'jeditaskid':
                        if requestParams['jeditaskid'] != 'None':
                            if int(requestParams['jeditaskid']) < 4000000:
                                query['taskid'] = requestParams[param]
                            else:
                                query[param] = requestParams[param]
                    elif param == 'taskid':
                        if requestParams['taskid'] != 'None': query[param] = requestParams[param]
                    elif param == 'pandaid':
                        try:
                            pid = requestParams['pandaid']
                            if pid.find(',') >= 0:
                                pidl = pid.split(',')
                                query['pandaid__in'] = pidl
                            else:
                                query['pandaid'] = int(pid)
                        except:
                            query['jobname'] = requestParams['pandaid']
                    elif param in ( 'computingsite', ):
                        ## support wildcarding
                        if requestParams[param].startswith('*') and requestParams[param].endswith('*'):
                            query['%s__icontains' % param] = requestParams[param].replace('*','')
                        elif requestParams[param].endswith('*'):
                            query['%s__istartswith' % param] = requestParams[param].replace('*','')
                        elif requestParams[param].startswith('*'):
                            query['%s__iendswith' % param] = requestParams[param].replace('*','')
                        elif requestParams[param] == 'None':
                            query[param] = None
                        else:
                            query[param] = requestParams[param]
                    elif requestParams[param].find('|') > 0:
                        vals = requestParams[param].split('|')
                        query[param+"__in"] = vals
                    elif param == 'jobstatus' and requestParams[param] == 'finished' and  ( ('mode' in requestParams and requestParams['mode'] == 'eventservice') or ('jobtype' in requestParams and requestParams['jobtype'] == 'eventservice') ):
                        query['jobstatus__in'] = ( 'finished', 'cancelled' )
                    else:
                        query[param] = requestParams[param]
    if 'jobtype' in requestParams:
        jobtype = requestParams['jobtype']
    else:
        jobtype = opmode
    if jobtype.startswith('anal'):
        query['prodsourcelabel__in'] = ['panda', 'user', 'prod_test', 'rc_test']
    elif jobtype.startswith('prod'):
        query['prodsourcelabel__in'] = ['managed', 'prod_test', 'rc_test']
    elif jobtype == 'groupproduction':
        query['prodsourcelabel'] = 'managed'
        query['workinggroup__isnull'] = False
    elif jobtype == 'eventservice':
        query['specialhandling__in'] = ( 'eventservice', 'esmerge' )
    elif jobtype.find('test') >= 0:
        query['prodsourcelabel__icontains'] = jobtype
    print 'Query:', query
    return query

def cleanJobList(jobl, mode='drop'):
    if 'mode' in requestParams and requestParams['mode'] == 'nodrop': mode='nodrop'
    jobs = addJobMetadata(jobl)
    for job in jobs:
        if isEventService(job):
            if 'taskbuffererrorcode' in job and job['taskbuffererrorcode'] == 111:
                job['taskbuffererrordiag'] = 'Rerun scheduled to pick up unprocessed events'
                job['piloterrorcode'] = 0
                job['piloterrordiag'] = 'Job terminated by signal from PanDA server'
                job['jobstatus'] = 'finished'
            if 'taskbuffererrorcode' in job and job['taskbuffererrorcode'] == 112:
                job['taskbuffererrordiag'] = 'All events processed, merge job created'
                job['piloterrorcode'] = 0
                job['piloterrordiag'] = 'Job terminated by signal from PanDA server'
                job['jobstatus'] = 'finished'
            if 'taskbuffererrorcode' in job and job['taskbuffererrorcode'] == 114:
                job['taskbuffererrordiag'] = 'No rerun to pick up unprocessed, at max attempts'
                job['piloterrorcode'] = 0
                job['piloterrordiag'] = 'Job terminated by signal from PanDA server'
                job['jobstatus'] = 'finished'
            if 'taskbuffererrorcode' in job and job['taskbuffererrorcode'] == 115:
                job['taskbuffererrordiag'] = 'No events remaining, other jobs still processing'
                job['piloterrorcode'] = 0
                job['piloterrordiag'] = 'Job terminated by signal from PanDA server'
                job['jobstatus'] = 'finished'
            if 'taskbuffererrorcode' in job and job['taskbuffererrorcode'] == 116:
                job['taskbuffererrordiag'] = 'No remaining event ranges to allocate'
                job['piloterrorcode'] = 0
                job['piloterrordiag'] = 'Job terminated by signal from PanDA server'
                job['jobstatus'] = 'finished'
            if 'jobmetrics' in job:
                pat = re.compile('.*mode\=([^\s]+).*HPCStatus\=([A-Za-z0-9]+)')
                mat = pat.match(job['jobmetrics'])
                if mat:
                    job['jobmode'] = mat.group(1)
                    job['substate'] = mat.group(2)
                pat = re.compile('.*coreCount\=([0-9]+)')
                mat = pat.match(job['jobmetrics'])
                if mat:
                    job['corecount'] = mat.group(1)

        if 'destinationdblock' in job:
            ddbfields = job['destinationdblock'].split('.')
            if len(ddbfields) == 6:
                job['outputfiletype'] = ddbfields[4]
            elif len(ddbfields) >= 7:
                job['outputfiletype'] = ddbfields[6]
            else:
                job['outputfiletype'] = '?'
            #print job['destinationdblock'], job['outputfiletype']

        try:
            job['homecloud'] = homeCloud[job['computingsite']]
        except:
            job['homecloud'] = None
        if 'produsername' in job and not job['produsername']:
            if job['produserid']:
                job['produsername'] = job['produserid']
            else:
                job['produsername'] = 'Unknown'
        if job['transformation']: job['transformation'] = job['transformation'].split('/')[-1]
        if (job['jobstatus'] == 'failed' or job['jobstatus'] == 'cancelled') and 'brokerageerrorcode' in job:
            job['errorinfo'] = errorInfo(job,nchars=70)
        else:
            job['errorinfo'] = ''
        job['jobinfo'] = ''
        if isEventService(job):
            if 'taskbuffererrordiag' in job and len(job['taskbuffererrordiag']) > 0:
                job['jobinfo'] = job['taskbuffererrordiag']
            elif 'specialhandling' in job and job['specialhandling'] == 'esmerge':
                job['jobinfo'] = 'Event service merge job'
            else:
                job['jobinfo'] = 'Event service job'
        job['duration'] = ""
        job['durationsec'] = 0
        #if job['jobstatus'] in ['finished','failed','holding']:
        if 'endtime' in job and 'starttime' in job and job['starttime']:
            starttime = job['starttime']
            if job['endtime']:
                endtime = job['endtime']
            else:
                endtime = timezone.now()
            duration = max(endtime - starttime, timedelta(seconds=0))
            ndays = duration.days
            strduration = str(timedelta(seconds=duration.seconds))
            job['duration'] = "%s:%s" % ( ndays, strduration )
            job['durationsec'] = duration.seconds
        job['waittime'] = ""
        #if job['jobstatus'] in ['running','finished','failed','holding','cancelled','transferring']:
        if 'creationtime' in job and 'starttime' in job and job['creationtime']:
            creationtime = job['creationtime']
            if job['starttime']:
                starttime = job['starttime']
            else:
                starttime = timezone.now()
            wait = starttime - creationtime
            ndays = wait.days
            strwait = str(timedelta(seconds=wait.seconds))
            job['waittime'] = "%s:%s" % (ndays, strwait)
        if 'currentpriority' in job:
            plo = int(job['currentpriority'])-int(job['currentpriority'])%100
            phi = plo+99
            job['priorityrange'] = "%d:%d" % ( plo, phi )
        if 'jobsetid' in job and job['jobsetid']:
            plo = int(job['jobsetid'])-int(job['jobsetid'])%100
            phi = plo+99
            job['jobsetrange'] = "%d:%d" % ( plo, phi )

    ## drop duplicate jobs
    droplist = []
    job1 = {}
    newjobs = []
    for job in jobs:
        pandaid = job['pandaid']
        dropJob = 0
        if pandaid in job1:
            ## This is a duplicate. Drop it.
            dropJob = 1
        else:
            job1[pandaid] = 1
        if (dropJob == 0):
            newjobs.append(job)
    jobs = newjobs

    if mode == 'nodrop':
        print 'job list cleaned'
        return jobs
    ## If the list is for a particular JEDI task, filter out the jobs superseded by retries
    taskids = {}
    for job in jobs:
        if 'jeditaskid' in job: taskids[job['jeditaskid']] = 1
    droplist = []
    newjobs = []
    if len(taskids) == 1:
        for task in taskids:
            retryquery = {}
            retryquery['jeditaskid'] = task
            retries = JediJobRetryHistory.objects.filter(**retryquery).order_by('newpandaid').values()
    for job in jobs:
        dropJob = 0
        pandaid = job['pandaid']
        if len(taskids) == 1:
            for retry in retries:
                if retry['oldpandaid'] == pandaid and retry['newpandaid'] != pandaid and (retry['relationtype'] == '' or retry['relationtype'] == 'retry' or ( job['processingtype'] == 'pmerge' and retry['relationtype'] == 'merge')):
                    ## there is a retry for this job. Drop it.
                    dropJob = retry['newpandaid']
        if isEventService(job):
            if 'jobstatus' in requestParams and requestParams['jobstatus'] == 'cancelled' and job['jobstatus'] != 'cancelled':
                dropJob = 1
            else:
                dropJob = 0
        if (dropJob == 0):
            newjobs.append(job)
        else:
            droplist.append( { 'pandaid' : pandaid, 'newpandaid' : dropJob } )
    droplist = sorted(droplist, key=lambda x:-x['pandaid'])
    jobs = newjobs
    global TFIRST, TLAST, PLOW, PHIGH
    TFIRST = timezone.now()
    TLAST = timezone.now() - timedelta(hours=2400)
    PLOW = 1000000
    PHIGH = -1000000
    for job in jobs:
        if job['modificationtime'] > TLAST: TLAST = job['modificationtime']
        if job['modificationtime'] < TFIRST: TFIRST = job['modificationtime']
        if job['currentpriority'] > PHIGH: PHIGH = job['currentpriority']
        if job['currentpriority'] < PLOW: PLOW = job['currentpriority']
    jobs = sorted(jobs, key=lambda x:-x['pandaid'])

    print 'job list cleaned'
    return jobs

def cleanTaskList(tasks):
    for task in tasks:
        if task['transpath']: task['transpath'] = task['transpath'].split('/')[-1]
        if task['statechangetime'] == None: task['statechangetime'] = task['modificationtime']

    ## Get status of input processing as indicator of task progress
    dsquery = {}
    dsquery['type__in'] = ['input', 'pseudo_input' ]
    dsquery['masterid__isnull'] = True
    taskl = []
    for t in tasks:
        taskl.append(t['jeditaskid'])
    dsquery['jeditaskid__in'] = taskl
    dsets = JediDatasets.objects.filter(**dsquery).values('jeditaskid','nfiles','nfilesfinished','nfilesfailed')
    dsinfo = {}
    if len(dsets) > 0:
        for ds in dsets:        
            taskid = ds['jeditaskid']
            if taskid not in dsinfo:
                dsinfo[taskid] = []
            dsinfo[taskid].append(ds)
    for task in tasks:
        if len(task['errordialog']) > 100: task['errordialog'] = task['errordialog'][:90]+'...'
        if 'reqid' in task and task['reqid'] < 100000 and task['reqid'] > 100 and task['reqid'] != 300 and not task['tasktype'].startswith('anal'):
            task['deftreqid'] = task['reqid']
        #if task['status'] == 'running' and task['jeditaskid'] in dsinfo:
        dstotals = {}
        dstotals['nfiles'] = 0
        dstotals['nfilesfinished'] = 0
        dstotals['nfilesfailed'] = 0
        dstotals['pctfinished'] = 0
        dstotals['pctfailed'] = 0
        if (task['jeditaskid'] in dsinfo):
            nfiles = 0
            nfinished = 0
            nfailed = 0
            for ds in dsinfo[task['jeditaskid']]:
                if int(ds['nfiles']) > 0:
                    nfiles += int(ds['nfiles'])
                    nfinished += int(ds['nfilesfinished'])
                    nfailed += int(ds['nfilesfailed'])
            if nfiles > 0:
                dstotals = {}
                dstotals['nfiles'] = nfiles
                dstotals['nfilesfinished'] = nfinished
                dstotals['nfilesfailed'] = nfailed
                dstotals['pctfinished'] = int(100.*nfinished/nfiles)
                dstotals['pctfailed'] = int(100.*nfailed/nfiles)

        task['dsinfo'] = dstotals

    if 'sortby' in requestParams:
        sortby = requestParams['sortby']
        if sortby == 'time-ascending':
            tasks = sorted(tasks, key=lambda x:x['modificationtime'])
        if sortby == 'time-descending':
            tasks = sorted(tasks, key=lambda x:x['modificationtime'], reverse=True)
        if sortby == 'statetime-descending':
            tasks = sorted(tasks, key=lambda x:x['statechangetime'], reverse=True)
        elif sortby == 'priority':
            tasks = sorted(tasks, key=lambda x:x['taskpriority'], reverse=True)
        elif sortby == 'nfiles':
            tasks = sorted(tasks, key=lambda x:x['dsinfo']['nfiles'], reverse=True)
        elif sortby == 'pctfinished':
            tasks = sorted(tasks, key=lambda x:x['dsinfo']['pctfinished'], reverse=True)
        elif sortby == 'pctfailed':
            tasks = sorted(tasks, key=lambda x:x['dsinfo']['pctfailed'], reverse=True)
        elif sortby == 'taskname':
            tasks = sorted(tasks, key=lambda x:x['taskname'])
        elif sortby == 'jeditaskid' or sortby == 'taskid':
            tasks = sorted(tasks, key=lambda x:-x['jeditaskid'])
    else:
        sortby = "jeditaskid"
        tasks = sorted(tasks, key=lambda x:-x['jeditaskid'])

    return tasks

def jobSummaryDict(request, jobs, fieldlist = None):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    if fieldlist:
        flist = fieldlist
    else:
        flist = standard_fields
    for job in jobs:
        for f in flist:
            if f in job and job[f]:
                if f == 'taskid' and int(job[f]) < 1000000 and 'produsername' not in requestParams: continue
                if f == 'specialhandling':
                    if not 'specialhandling' in sumd: sumd['specialhandling'] = {}
                    shl = job['specialhandling'].split()
                    for v in shl:
                        if not v in sumd['specialhandling']: sumd['specialhandling'][v] = 0
                        sumd['specialhandling'][v] += 1
                else:
                    if not f in sumd: sumd[f] = {}
                    if not job[f] in sumd[f]: sumd[f][job[f]] = 0
                    sumd[f][job[f]] += 1
        for extra in ( 'jobmode', 'substate', 'outputfiletype' ):
            if extra in job:
                if not extra in sumd: sumd[extra] = {}
                if not job[extra] in sumd[extra]: sumd[extra][job[extra]] = 0
                sumd[extra][job[extra]] += 1

    ## event service
    esjobdict = {}
    esjobs = []
    for job in jobs:
        if isEventService(job):
            esjobs.append(job['pandaid'])
            esjobdict[job['pandaid']] = {}
            for s in eventservicestatelist:
                esjobdict[job['pandaid']][s] = 0
    if len(esjobs) > 0:
        sumd['eventservice'] = {}
        esquery = {}
        esquery['pandaid__in'] = esjobs
        evtable = JediEvents.objects.filter(**esquery).values('pandaid','status')
        for ev in evtable:
            evstat = eventservicestatelist[ev['status']]
            if evstat not in sumd['eventservice']:
                sumd['eventservice'][evstat] = 0
            sumd['eventservice'][evstat] += 1
            esjobdict[ev['pandaid']][evstat] += 1

    ## convert to ordered lists
    suml = []
    for f in sumd:
        itemd = {}
        itemd['field'] = f
        iteml = []
        kys = sumd[f].keys()
        if f in  ( 'priorityrange', 'jobsetrange' ):
            skys = []
            for k in kys:
                skys.append( { 'key' : k, 'val' : int(k[:k.index(':')]) } )
            skys = sorted(skys, key=lambda x:x['val'])
            kys = []
            for sk in skys:
                kys.append(sk['key'])
        elif f in ( 'attemptnr', 'jeditaskid', 'taskid', ):
            kys = sorted(kys, key=lambda x:int(x))
        else:
            kys.sort()
        for ky in kys:
            iteml.append({ 'kname' : ky, 'kvalue' : sumd[f][ky] })
        if 'sortby' in requestParams and requestParams['sortby'] == 'count':
            iteml = sorted(iteml, key=lambda x:x['kvalue'], reverse=True)
        elif f not in ( 'priorityrange', 'jobsetrange', 'attemptnr', 'jeditaskid', 'taskid', ):
            iteml = sorted(iteml, key=lambda x:str(x['kname']).lower())
        itemd['list'] = iteml
        suml.append(itemd)
        suml = sorted(suml, key=lambda x:x['field'])
    return suml, esjobdict

def siteSummaryDict(sites):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    sumd['category'] = {}
    sumd['category']['test'] = 0
    sumd['category']['production'] = 0
    sumd['category']['analysis'] = 0
    sumd['category']['multicloud'] = 0
    for site in sites:
        for f in standard_sitefields:
            if f in site:
                if not f in sumd: sumd[f] = {}
                if not site[f] in sumd[f]: sumd[f][site[f]] = 0
                sumd[f][site[f]] += 1
        isProd = True
        if site['siteid'].find('ANALY') >= 0:
            isProd = False
            sumd['category']['analysis'] += 1
        if site['siteid'].lower().find('test') >= 0:
            isProd = False
            sumd['category']['test'] += 1
        if (site['multicloud'] is not None) and (site['multicloud'] != 'None') and (re.match('[A-Z]+',site['multicloud'])):
            sumd['category']['multicloud'] += 1
        if isProd: sumd['category']['production'] += 1
    if VOMODE != 'atlas': del sumd['cloud']
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
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x:x['field'])
    return suml

def userSummaryDict(jobs):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    for job in jobs:
        if 'produsername' in job and job['produsername'] != None:
            user = job['produsername'].lower()
        else:
            user = 'Unknown'
        if not user in sumd:
            sumd[user] = {}
            for state in statelist:
                sumd[user][state] = 0
            sumd[user]['name'] = job['produsername']
            sumd[user]['cputime'] = 0
            sumd[user]['njobs'] = 0
            for state in statelist:
                sumd[user]['n'+state] = 0
            sumd[user]['nsites'] = 0
            sumd[user]['sites'] = {}
            sumd[user]['nclouds'] = 0
            sumd[user]['clouds'] = {}
            sumd[user]['nqueued'] = 0
            sumd[user]['latest'] = timezone.now() - timedelta(hours=2400)
            sumd[user]['pandaid'] = 0
        cloud = job['cloud']
        site = job['computingsite']
        cpu = float(job['cpuconsumptiontime'])/1.
        state = job['jobstatus']
        if job['modificationtime'] > sumd[user]['latest']: sumd[user]['latest'] = job['modificationtime']
        if job['pandaid'] > sumd[user]['pandaid']: sumd[user]['pandaid'] = job['pandaid']
        sumd[user]['cputime'] += cpu
        sumd[user]['njobs'] += 1
        if 'n%s' % (state) not in sumd[user]:
            sumd[user]['n' + state] = 0
        sumd[user]['n'+state] += 1
        if not site in sumd[user]['sites']: sumd[user]['sites'][site] = 0
        sumd[user]['sites'][site] += 1
        if not site in sumd[user]['clouds']: sumd[user]['clouds'][cloud] = 0
        sumd[user]['clouds'][cloud] += 1
    for user in sumd:
        sumd[user]['nsites'] = len(sumd[user]['sites'])
        sumd[user]['nclouds'] = len(sumd[user]['clouds'])
        sumd[user]['nqueued'] = sumd[user]['ndefined'] + sumd[user]['nwaiting'] + sumd[user]['nassigned'] + sumd[user]['nactivated']
        sumd[user]['cputime'] = "%d" % float(sumd[user]['cputime'])
    ## convert to list ordered by username
    ukeys = sumd.keys()
    ukeys.sort()
    suml = []
    for u in ukeys:
        uitem = {}
        uitem['name'] = u
        uitem['latest'] = sumd[u]['pandaid']
        uitem['dict'] = sumd[u]
        suml.append(uitem)
    suml = sorted(suml, key=lambda x:-x['latest'])
    return suml

def taskSummaryDict(request, tasks, fieldlist = None):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    if fieldlist:
        flist = fieldlist
    else:
        flist = standard_taskfields
    for task in tasks:
        for f in flist:
            if 'tasktype' in requestParams and requestParams['tasktype'].startswith('analy'):
                ## Remove the noisy useless parameters in analysis listings
                if flist in ( 'reqid', 'stream', 'tag' ): continue

            if len(task['taskname'].split('.')) == 5:
                if f == 'project':
                    try:
                        if not f in sumd: sumd[f] = {}
                        project = task['taskname'].split('.')[0]
                        if not project in sumd[f]: sumd[f][project] = 0
                        sumd[f][project] += 1
                    except:
                        pass
                if f == 'stream':
                    try:
                        if not f in sumd: sumd[f] = {}
                        stream = task['taskname'].split('.')[2]
                        if not re.match('[0-9]+',stream):
                            if not stream in sumd[f]: sumd[f][stream] = 0
                            sumd[f][stream] += 1
                    except:
                        pass
                if f == 'tag':
                    try:
                        if not f in sumd: sumd[f] = {}
                        tags = task['taskname'].split('.')[4]
                        if not tags.startswith('job_'):
                            tagl = tags.split('_')
                            for tag in tagl:
                                if not tag in sumd[f]: sumd[f][tag] = 0
                                sumd[f][tag] += 1
                    except:
                        pass
            if f in task and task[f]:
                val = task[f]
                if val == 'anal': val = 'analy'
                if not f in sumd: sumd[f] = {}
                if not val in sumd[f]: sumd[f][val] = 0
                sumd[f][val] += 1
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
        iteml = sorted(iteml, key=lambda x:str(x['kname']).lower())
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x:x['field'])
    return suml

def wgTaskSummary(request, fieldname='workinggroup', view='production', taskdays=3):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    query = {}
    hours = 24*taskdays
    startdate = timezone.now() - timedelta(hours=hours)
    startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = timezone.now().strftime(defaultDatetimeFormat)
    query['modificationtime__range'] = [startdate, enddate]
    if fieldname == 'workinggroup': query['workinggroup__isnull'] = False
    if view == 'production':
        query['tasktype'] = 'prod'
    elif view == 'analysis':
        query['tasktype'] = 'anal'
    summary = JediTasks.objects.filter(**query).values(fieldname,'status').annotate(Count('status')).order_by(fieldname,'status')
    totstates = {}
    tottasks = 0
    wgsum = {}
    for state in taskstatelist:
        totstates[state] = 0
    for rec in summary:
        wg = rec[fieldname]
        status = rec['status']
        count = rec['status__count']
        if status not in taskstatelist: continue
        tottasks += count
        totstates[status] += count
        if wg not in wgsum:
            wgsum[wg] = {}
            wgsum[wg]['name'] = wg
            wgsum[wg]['count'] = 0
            wgsum[wg]['states'] = {}
            wgsum[wg]['statelist'] = []
            for state in taskstatelist:
                wgsum[wg]['states'][state] = {}
                wgsum[wg]['states'][state]['name'] = state
                wgsum[wg]['states'][state]['count'] = 0
        wgsum[wg]['count'] += count
        wgsum[wg]['states'][status]['count'] += count

    ## convert to ordered lists
    suml = []
    for f in wgsum:
        itemd = {}
        itemd['field'] = f
        itemd['count'] = wgsum[f]['count']
        kys = taskstatelist
        iteml = []
        for ky in kys:
            iteml.append({ 'kname' : ky, 'kvalue' : wgsum[f]['states'][ky]['count'] })
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x:x['field'])
    return suml

def extensibleURL(request, xurl = ''):
    """ Return a URL that is ready for p=v query extension(s) to be appended """
    if xurl == '': xurl = request.get_full_path()
    if xurl.endswith('/'): xurl = xurl[0:len(xurl)-1]
    if xurl.find('?') > 0:
        xurl += '&'
    else:
        xurl += '?'
    #if 'jobtype' in requestParams:
    #    xurl += "jobtype=%s&" % requestParams['jobtype']
    return xurl

def mainPage(request):
    valid, response = initRequest(request)
    if not valid: return response
    setupView(request)
    debuginfo = None
    if request.session['debug']:
        debuginfo = "<h2>Debug info</h2>"
        from django.conf import settings
        for name in dir(settings):
            debuginfo += "%s = %s<br>" % ( name, getattr(settings, name) )
        debuginfo += "<br>******* Environment<br>"
        for env in os.environ:
            debuginfo += "%s = %s<br>" % ( env, os.environ[env] )

    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        data = {
            'prefix': getPrefix(request),
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'debuginfo' : debuginfo
        }
        data.update(getContextVariables(request))
        response = render_to_response('lsst-mainPage.html', data, RequestContext(request))
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes']*60)
        return response
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        return  HttpResponse('json', mimetype='text/html')
    else:
        return  HttpResponse('not understood', mimetype='text/html')

def helpPage(request):
    valid, response = initRequest(request)
    if not valid: return response
    setupView(request)
    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        data = {
            'prefix': getPrefix(request),
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
        }
        data.update(getContextVariables(request))
        return render_to_response('completeHelp.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        return  HttpResponse('json', mimetype='text/html')
    else:
        return  HttpResponse('not understood', mimetype='text/html')

def errorInfo(job, nchars=300, mode='html'):
    errtxt = ''
    err1 = ''
    if int(job['brokerageerrorcode']) != 0:
        errtxt += 'Brokerage error %s: %s <br>' % ( job['brokerageerrorcode'], job['brokerageerrordiag'] )
        if err1  == '': err1 = "Broker: %s" % job['brokerageerrordiag']
    if int(job['ddmerrorcode']) != 0:
        errtxt += 'DDM error %s: %s <br>' % ( job['ddmerrorcode'], job['ddmerrordiag'] )
        if err1  == '': err1 = "DDM: %s" % job['ddmerrordiag']
    if int(job['exeerrorcode']) != 0:
        errtxt += 'Executable error %s: %s <br>' % ( job['exeerrorcode'], job['exeerrordiag'] )
        if err1  == '': err1 = "Exe: %s" % job['exeerrordiag']
    if int(job['jobdispatchererrorcode']) != 0:
        errtxt += 'Dispatcher error %s: %s <br>' % ( job['jobdispatchererrorcode'], job['jobdispatchererrordiag'] )
        if err1  == '': err1 = "Dispatcher: %s" % job['jobdispatchererrordiag']
    if int(job['piloterrorcode']) != 0:
        errtxt += 'Pilot error %s: %s <br>' % ( job['piloterrorcode'], job['piloterrordiag'] )
        if err1  == '': err1 = "Pilot: %s" % job['piloterrordiag']
    if int(job['superrorcode']) != 0:
        errtxt += 'Sup error %s: %s <br>' % ( job['superrorcode'], job['superrordiag'] )
        if err1  == '': err1 = job['superrordiag']
    if int(job['taskbuffererrorcode']) != 0:
        errtxt += 'Task buffer error %s: %s <br>' % ( job['taskbuffererrorcode'], job['taskbuffererrordiag'] )
        if err1  == '': err1 = 'Taskbuffer: %s' % job['taskbuffererrordiag']
    if job['transexitcode'] != '' and job['transexitcode'] is not None and int(job['transexitcode']) > 0:
        errtxt += 'Trf exit code %s.' % job['transexitcode']
        if err1  == '': err1 = 'Trf exit code %s' % job['transexitcode']
    desc = getErrorDescription(job)
    if len(desc) > 0:
        errtxt += '%s<br>' % desc
        if err1 == '': err1 = getErrorDescription(job,mode='string')
    if len(errtxt) > nchars:
        ret = errtxt[:nchars] + '...'
    else:
        ret = errtxt[:nchars]
    if err1.find('lost heartbeat') >= 0: err1 = 'lost heartbeat'
    if err1.lower().find('unknown transexitcode') >= 0: err1 = 'unknown transexit'
    if err1.find(' at ') >= 0: err1 = err1[:err1.find(' at ')-1]
    if errtxt.find('lost heartbeat') >= 0: err1 = 'lost heartbeat'
    err1 = err1.replace('\n',' ')
    if mode == 'html':
        return errtxt
    else:
        return err1[:nchars]

def jobList(request, mode=None, param=None):
    valid, response = initRequest(request)
    if not valid: return response
    eventservice = False
    if 'mode' in requestParams and requestParams['mode'] == 'eventservice':
        eventservice = True
    if 'jobtype' in requestParams and requestParams['jobtype'] == 'eventservice':
        eventservice = True
    query = setupView(request)
    if 'batchid' in requestParams:
        query['batchid'] = requestParams['batchid']
    jobs = []
    if request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        values = Jobsactive4._meta.get_all_field_names()
    elif eventservice:
        values = 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime', 'pandaid', 'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'currentpriority', 'creationtime', 'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag', 'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode', 'destinationse', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr', 'jobname', 'proddblock', 'destinationdblock', 'jobmetrics'
    else:
        values = 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime', 'pandaid', 'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid', 'currentpriority', 'creationtime', 'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag', 'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode', 'destinationse', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr', 'jobname', 'computingelement', 'proddblock', 'destinationdblock'
    global JOB_LIMIT
    if 'limit' in requestParams:
        JOB_LIMIT = int(requestParams['limit'])
    if 'transferringnotupdated' in requestParams:
        jobs = stateNotUpdated(request, state='transferring', values=values)
    elif 'statenotupdated' in requestParams:
        jobs = stateNotUpdated(request, values=values)
    else:
        jobs.extend(Jobsdefined4.objects.filter(**query)[:JOB_LIMIT].values(*values))
        jobs.extend(Jobsactive4.objects.filter(**query)[:JOB_LIMIT].values(*values))
        jobs.extend(Jobswaiting4.objects.filter(**query)[:JOB_LIMIT].values(*values))
        jobs.extend(Jobsarchived4.objects.filter(**query)[:JOB_LIMIT].values(*values))
        if 'jobstatus' not in requestParams or requestParams['jobstatus'] in ( 'finished', 'failed', 'cancelled' ):
            jobs.extend(Jobsarchived.objects.filter(**query)[:JOB_LIMIT].values(*values))

    ## If the list is for a particular JEDI task, filter out the jobs superseded by retries
    taskids = {}
    for job in jobs:
        if 'jeditaskid' in job: taskids[job['jeditaskid']] = 1
    dropmode = True
    if 'mode' in requestParams and requestParams['mode'] == 'nodrop': dropmode = False
    droplist = []
    if dropmode and (len(taskids) == 1):
        print 'doing the drop'
        for task in taskids:
            retryquery = {}
            retryquery['jeditaskid'] = task
            retries = JediJobRetryHistory.objects.filter(**retryquery).order_by('newpandaid').values()
        newjobs = []
        for job in jobs:
            dropJob = 0
            pandaid = job['pandaid']
            for retry in retries:
                if retry['oldpandaid'] == pandaid and retry['newpandaid'] != pandaid and (retry['relationtype'] == '' or retry['relationtype'] == 'retry'):
                    ## there is a retry for this job. Drop it.
                    dropJob = retry['newpandaid']
            if dropJob == 0 or isEventService(job):
                newjobs.append(job)
            else:
                droplist.append( { 'pandaid' : pandaid, 'newpandaid' : dropJob } )
        droplist = sorted(droplist, key=lambda x:-x['pandaid'])
        jobs = newjobs

    jobs = cleanJobList(jobs)
    njobs = len(jobs)
    jobtype = ''
    if 'jobtype' in requestParams:
        jobtype = requestParams['jobtype']
    elif '/analysis' in request.path:
        jobtype = 'analysis'
    elif '/production' in request.path:
        jobtype = 'production'

    if u'display_limit' in requestParams and int(requestParams['display_limit']) < njobs:
        display_limit = int(requestParams['display_limit'])
        url_nolimit = removeParam(request.get_full_path(), 'display_limit')
    else:
        display_limit = 3000
        url_nolimit = request.get_full_path()
    njobsmax = display_limit

    if 'sortby' in requestParams:
        sortby = requestParams['sortby']
        if sortby == 'time-ascending':
            jobs = sorted(jobs, key=lambda x:x['modificationtime'])
        if sortby == 'time-descending':
            jobs = sorted(jobs, key=lambda x:x['modificationtime'], reverse=True)
        if sortby == 'statetime-descending':
            jobs = sorted(jobs, key=lambda x:x['statechangetime'], reverse=True)
        elif sortby == 'priority':
            jobs = sorted(jobs, key=lambda x:x['currentpriority'], reverse=True)
        elif sortby == 'attemptnr':
            jobs = sorted(jobs, key=lambda x:x['attemptnr'], reverse=True)
        elif sortby == 'duration':
            jobs = sorted(jobs, key=lambda x:x['durationsec'], reverse=True)
        elif sortby == 'PandaID':
            pass
    else:
        sortby = "PandaID"

    taskname = ''
    if 'jeditaskid' in requestParams:
        taskname = getTaskName('jeditaskid',requestParams['jeditaskid'])
    if 'taskid' in requestParams:
        taskname = getTaskName('jeditaskid',requestParams['taskid'])

    if 'produsername' in requestParams:
        user = requestParams['produsername']
    elif 'user' in requestParams:
        user = requestParams['user']
    else:
        user = None

    ## set up google flow diagram
    flowstruct = buildGoogleFlowDiagram(jobs=jobs)

    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        sumd, esjobdict = jobSummaryDict(request, jobs)
        if esjobdict and len(esjobdict) > 0:
            for job in jobs:
                if job['pandaid'] in esjobdict and job['specialhandling'].find('esmerge') < 0:
                    esjobstr = 'Dispatched event states: '
                    for s in esjobdict[job['pandaid']]:
                        if esjobdict[job['pandaid']][s] > 0:
                            esjobstr += " %s(%s) " % ( s, esjobdict[job['pandaid']][s] )
                    job['esjobstr'] = esjobstr
        xurl = extensibleURL(request)
        nosorturl = removeParam(xurl, 'sortby',mode='extensible')
        nosorturl = removeParam(nosorturl, 'display_limit', mode='extensible')
        data = {
            'prefix': getPrefix(request),
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'jobList': jobs[:njobsmax],
            'jobtype' : jobtype,
            'njobs' : njobs,
            'user' : user,
            'sumd' : sumd,
            'xurl' : xurl,
            'droplist' : droplist,
            'ndrops' : len(droplist),
            'tfirst' : TFIRST,
            'tlast' : TLAST,
            'plow' : PLOW,
            'phigh' : PHIGH,
            'url_nolimit' : url_nolimit,
            'display_limit' : display_limit,
            'sortby' : sortby,
            'nosorturl' : nosorturl,
            'taskname' : taskname,
            'flowstruct' : flowstruct,
        }
        data.update(getContextVariables(request))
        if eventservice:
            return render_to_response('jobListES.html', data, RequestContext(request))
        else:
            return render_to_response('jobList.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        return  HttpResponse(json.dumps(jobs, cls=DateEncoder), mimetype='text/html')

def isEventService(job):
    if 'specialhandling' in job and job['specialhandling'] and ( job['specialhandling'].find('eventservice') >= 0 or job['specialhandling'].find('esmerge') >= 0 ):
        return True
    else:
        return False

@csrf_exempt
def jobInfo(request, pandaid=None, batchid=None, p2=None, p3=None, p4=None):
    valid, response = initRequest(request)
    if not valid: return response
    eventservice = False
    query = setupView(request, hours=365*24)
    jobid = ''
    if 'creator' in requestParams:
        ## Find the job that created the specified file.
        fquery = {}
        fquery['lfn'] = requestParams['creator']
        fquery['type'] = 'output'
        fileq = Filestable4.objects.filter(**fquery)
        fileq = fileq.values('pandaid','type')
        if fileq and len(fileq) > 0:
            pandaid = fileq[0]['pandaid']
        else:
            fileq = FilestableArch.objects.filter(**fquery).values('pandaid','type')
            if fileq and len(fileq) > 0:
                pandaid = fileq[0]['pandaid']
    if pandaid:
        jobid = pandaid
        try:
            query['pandaid'] = int(pandaid)
        except:
            query['jobname'] = pandaid
    if batchid:
        jobid = batchid
        query['batchid'] = batchid
    if 'pandaid' in requestParams:
        pandaid = requestParams['pandaid']
        jobid = pandaid
        query['pandaid'] = int(pandaid)
    elif 'batchid' in requestParams:
        batchid = requestParams['batchid']
        jobid = "'"+batchid+"'"
        query['batchid'] = batchid
    elif 'jobname' in requestParams:
        jobid = requestParams['jobname']
        query['jobname'] = jobid

    jobs = []
    if pandaid or batchid:
        startdate = timezone.now() - timedelta(hours=LAST_N_HOURS_MAX)
        jobs.extend(Jobsdefined4.objects.filter(**query).values())
        jobs.extend(Jobsactive4.objects.filter(**query).values())
        jobs.extend(Jobswaiting4.objects.filter(**query).values())
        jobs.extend(Jobsarchived4.objects.filter(**query).values())
        if len(jobs) == 0:
            jobs.extend(Jobsarchived.objects.filter(**query).values())
        jobs = cleanJobList(jobs, mode='nodrop')
        
    if len(jobs) == 0:
        data = {
            'prefix': getPrefix(request),
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'pandaid': pandaid,
            'job': None,
            'jobid' : jobid,
        }
        return render_to_response('jobInfo.html', data, RequestContext(request))

    job = {}
    colnames = []
    columns = []
    try:
        job = jobs[0]
        pandaid = job['pandaid']
        colnames = job.keys()
        colnames.sort()
        for k in colnames:
            val = job[k]
            if job[k] == None:
                val = ''
                continue
            pair = { 'name' : k, 'value' : val }
            columns.append(pair)
    except IndexError:
        job = {}

    ## Check for logfile extracts
    logs = Logstable.objects.filter(pandaid=pandaid)
    if logs:
        logextract = logs[0].log1
    else:
        logextract = None

    ## Get job files. First look in JEDI datasetcontents
    files = []
    files.extend(JediDatasetContents.objects.filter(pandaid=pandaid).order_by('type').values())
    ninput = 0
    if len(files) > 0:
        for f in files:
            if f['type'] == 'input': ninput += 1
            f['fsizemb'] = "%0.2f" % (f['fsize']/1000000.)
            dsets = JediDatasets.objects.filter(datasetid=f['datasetid']).values()
            if len(dsets) > 0:
                f['datasetname'] = dsets[0]['datasetname']
    if True:
    #if ninput == 0:
        files.extend(Filestable4.objects.filter(pandaid=pandaid).order_by('type').values())
        if len(files) == 0:
            files.extend(FilestableArch.objects.filter(pandaid=pandaid).order_by('type').values())
        if len(files) > 0:
            for f in files:
                if 'creationdate' not in f: f['creationdate'] = f['modificationtime']
                if 'fileid' not in f: f['fileid'] = f['row_id']
                if 'datasetname' not in f: f['datasetname'] = f['dataset']
                if 'modificationtime' in f: f['oldfiletable'] = 1
                if 'destinationdblock' in f and f['destinationdblock'] is not None:
                    f['destinationdblock_vis'] = f['destinationdblock'].split('_')[-1]
    files = sorted(files, key=lambda x:x['type'])
    nfiles = len(files) 
    logfile = {} 
    for file in files:
        if file['type'] == 'log': 
            logfile['lfn'] = file['lfn'] 
            logfile['guid'] = file['guid'] 
            if 'destinationse' in file:
                logfile['site'] = file['destinationse'] 
            else:
                logfilerec = Filestable4.objects.filter(pandaid=pandaid, lfn=logfile['lfn']).values()
                if len(logfilerec) == 0:
                    logfilerec = FilestableArch.objects.filter(pandaid=pandaid, lfn=logfile['lfn']).values()
                if len(logfilerec) > 0:
                    logfile['site'] = logfilerec[0]['destinationse']
                    logfile['guid'] = logfilerec[0]['guid']
            logfile['scope'] = file['scope']
        file['fsize'] = int(file['fsize']/1000000)

    if 'pilotid' in job and job['pilotid'] is not None and job['pilotid'].startswith('http'):
        stdout = job['pilotid'].split('|')[0]
        stderr = stdout.replace('.out','.err')
        stdlog = stdout.replace('.out','.log')
    else:
        stdout = stderr = stdlog = None

    ## Check for object store based log
    oslogpath = None
    if 'computingsite' in job and job['computingsite'] in objectStores:
        ospath = objectStores[job['computingsite']]
        if 'lfn' in logfile:
            if ospath.endswith('/'):
                oslogpath = ospath + logfile['lfn']
            else:
                oslogpath = ospath + '/' + logfile['lfn']

    ## Check for debug info
    if 'specialhandling' in job and job['specialhandling'].find('debug') >= 0:
        debugmode = True
    else:
        debugmode = False
    debugstdout = None
    if debugmode:
        if 'showdebug' in requestParams:
            debugstdoutrec = Jobsdebug.objects.filter(pandaid=pandaid).values()
            if len(debugstdoutrec) > 0:
                if 'stdout' in debugstdoutrec: debugstdout = debugstdoutrec['stdout']

    if 'transformation' in job and job['transformation'] is not None and job['transformation'].startswith('http'):
        job['transformation'] = "<a href='%s'>%s</a>" % ( job['transformation'], job['transformation'].split('/')[-1] )

    if 'metastruct' in job:
        job['metadata'] = json.dumps(job['metastruct'], sort_keys=True, indent=4, separators=(',', ': '))

    ## Get job parameters
    jobparamrec = Jobparamstable.objects.filter(pandaid=pandaid)
    jobparams = None
    if len(jobparamrec) > 0:
        jobparams = jobparamrec[0].jobparameters
    #else:
    #    jobparamrec = JobparamstableArch.objects.filter(pandaid=pandaid)
    #    if len(jobparamrec) > 0:
    #        jobparams = jobparamrec[0].jobparameters

    dsfiles = []
    ## If this is a JEDI job, look for job retries
    if 'jeditaskid' in job and job['jeditaskid'] > 0:
        ## Look for retries of this job
        retryquery = {}
        retryquery['jeditaskid'] = job['jeditaskid']
        retryquery['oldpandaid'] = job['pandaid']
        retries = JediJobRetryHistory.objects.filter(**retryquery).order_by('newpandaid').reverse().values()
        pretryquery = {}
        pretryquery['jeditaskid'] = job['jeditaskid']
        pretryquery['newpandaid'] = job['pandaid']
        pretries = JediJobRetryHistory.objects.filter(**pretryquery).order_by('oldpandaid').reverse().values()
    else:
        retries = None
        pretries = None

    ## jobset info
    libjob = None
    runjobs = []
    mergejobs = []
    if 'jobsetid' in job and job['jobsetid'] > 0:
        jsquery = {}
        jsquery['jobsetid'] = job['jobsetid']
        jsquery['produsername'] = job['produsername']
        values = [ 'pandaid', 'prodsourcelabel', 'processingtype', 'transformation' ]
        jsjobs = []
        jsjobs.extend(Jobsdefined4.objects.filter(**jsquery).values(*values))
        jsjobs.extend(Jobsactive4.objects.filter(**jsquery).values(*values))
        jsjobs.extend(Jobswaiting4.objects.filter(**jsquery).values(*values))
        jsjobs.extend(Jobsarchived4.objects.filter(**jsquery).values(*values))
        jsjobs.extend(Jobsarchived.objects.filter(**jsquery).values(*values))
        if len(jsjobs) > 0:
            for j in jsjobs:
                id = j['pandaid']
                if j['transformation'].find('runAthena') >= 0:
                    runjobs.append(id)
                elif j['transformation'].find('buildJob') >= 0:
                    libjob = id
                if j['processingtype'] == 'usermerge':
                    mergejobs.append(id)

    esjobstr = ''
    if isEventService(job):
        ## for ES jobs, prepare the event table
        esjobdict = {}
        for s in eventservicestatelist:
            esjobdict[s] = 0
        evtable = JediEvents.objects.filter(pandaid=job['pandaid']).order_by('-def_min_eventid').values('def_min_eventid','def_max_eventid','processed_upto_eventid','status','job_processid','attemptnr')
        fileids = {}
        datasetids = {}
        #for evrange in evtable:
        #    fileids[int(evrange['fileid'])] = {}
        #    datasetids[int(evrange['datasetid'])] = {}
        flist = []
        for f in fileids:
            flist.append(f)
        dslist = []
        for ds in datasetids:
            dslist.append(ds)
        datasets = JediDatasets.objects.filter(datasetid__in=dslist).values()
        dsfiles = JediDatasetContents.objects.filter(fileid__in=flist).values()        
        #for ds in datasets:
        #    datasetids[int(ds['datasetid'])]['dict'] = ds
        #for f in dsfiles:
        #    fileids[int(f['fileid'])]['dict'] = f
        for evrange in evtable:
            #evrange['fileid'] = fileids[int(evrange['fileid'])]['dict']['lfn']
            #evrange['datasetid'] = datasetids[evrange['datasetid']]['dict']['datasetname']
            evrange['status'] = eventservicestatelist[evrange['status']]
            esjobdict[evrange['status']] += 1
        esjobstr = ''
        for s in esjobdict:
            if esjobdict[s] > 0:
                esjobstr += " %s(%s) " % ( s, esjobdict[s] )
    else:
        evtable = []

    ## For LSST, pick up parameters from jobparams
    if VOMODE == 'lsst' or ('vo' in job and job['vo'] == 'lsst'):
        lsstData = {}
        if jobparams:
            lsstParams = re.match('.*PIPELINE_TASK\=([a-zA-Z0-9]+).*PIPELINE_PROCESSINSTANCE\=([0-9]+).*PIPELINE_STREAM\=([0-9\.]+)',jobparams)
            if lsstParams:
                lsstData['pipelinetask'] = lsstParams.group(1)
                lsstData['processinstance'] = lsstParams.group(2)
                lsstData['pipelinestream'] = lsstParams.group(3)
    else:
        lsstData = None

    if 'jobstatus' in job and (job['jobstatus'] == 'failed' or job['jobstatus'] == 'holding'):
        errorinfo = getErrorDescription(job)
        if len(errorinfo) > 0:
            job['errorinfo'] = errorinfo

    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        data = {
            'prefix': getPrefix(request),
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'pandaid': pandaid,
            'job': job,
            'columns' : columns,
            'files' : files,
            'dsfiles' : dsfiles,
            'nfiles' : nfiles,
            'logfile' : logfile,
            'oslogpath' : oslogpath,
            'stdout' : stdout,
            'stderr' : stderr,
            'stdlog' : stdlog,
            'jobparams' : jobparams,
            'jobid' : jobid,
            'lsstData' : lsstData,
            'logextract' : logextract,
            'retries' : retries,
            'pretries' : pretries,
            'eventservice' : isEventService(job),
            'evtable' : evtable[:100],
            'debugmode' : debugmode,
            'debugstdout' : debugstdout,
            'libjob' : libjob,
            'runjobs' : runjobs,
            'mergejobs' : mergejobs,
            'esjobstr': esjobstr,
        }
        data.update(getContextVariables(request))
        if isEventService(job):
            return render_to_response('jobInfoES.html', data, RequestContext(request))
        else:
            return render_to_response('jobInfo.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        return  HttpResponse('json', mimetype='text/html')
    else:
        return  HttpResponse('not understood', mimetype='text/html')

def userList(request):
    valid, response = initRequest(request)
    if not valid: return response
    nhours = 90*24
    query = setupView(request, hours=nhours, limit=-99)
    if VOMODE == 'atlas':
        view = 'database'
    else:
        view = 'dynamic'
    if 'view' in requestParams:
        view = requestParams['view']
    sumd = []
    jobsumd = []
    userdb = []
    userdbl = []
    userstats = {}
    if view == 'database':
        startdate = timezone.now() - timedelta(hours=nhours)
        startdate = startdate.strftime(defaultDatetimeFormat)
        enddate = timezone.now().strftime(defaultDatetimeFormat)
        query = { 'latestjob__range' : [startdate, enddate] }
        #viewParams['selection'] = ", last %d days" % (float(nhours)/24.)
        ## Use the users table
        userdb = Users.objects.filter(**query).order_by('name')
        if 'sortby' in requestParams:
            sortby = requestParams['sortby']
            if sortby == 'name':
                userdb = Users.objects.filter(**query).order_by('name')
            elif sortby == 'njobs':
                userdb = Users.objects.filter(**query).order_by('njobsa').reverse()
            elif sortby == 'date':
                userdb = Users.objects.filter(**query).order_by('latestjob').reverse()
            elif sortby == 'cpua1':
                userdb = Users.objects.filter(**query).order_by('cpua1').reverse()
            elif sortby == 'cpua7':
                userdb = Users.objects.filter(**query).order_by('cpua7').reverse()
            elif sortby == 'cpup1':
                userdb = Users.objects.filter(**query).order_by('cpup1').reverse()
            elif sortby == 'cpup7':
                userdb = Users.objects.filter(**query).order_by('cpup7').reverse()
            else:
                userdb = Users.objects.filter(**query).order_by('name')
        else:
            userdb = Users.objects.filter(**query).order_by('name')

        anajobs = 0
        n1000 = 0
        n10k = 0
        nrecent3 = 0
        nrecent7 = 0
        nrecent30 = 0
        nrecent90 = 0
        ## Move to a list of dicts and adjust CPU unit
        for u in userdb:
            udict = {}
            udict['name'] = u.name
            udict['njobsa'] = u.njobsa
            if u.cpua1: udict['cpua1'] = "%0.1f" % (int(u.cpua1)/3600.)
            if u.cpua7: udict['cpua7'] = "%0.1f" % (int(u.cpua7)/3600.)
            if u.cpup1: udict['cpup1'] = "%0.1f" % (int(u.cpup1)/3600.)
            if u.cpup7: udict['cpup7'] = "%0.1f" % (int(u.cpup7)/3600.)
            udict['latestjob'] = u.latestjob
            userdbl.append(udict)

            if u.njobsa > 0: anajobs += u.njobsa
            if u.njobsa >= 1000: n1000 += 1
            if u.njobsa >= 10000: n10k += 1
            if u.latestjob != None:
                latest = timezone.now() - u.latestjob
                if latest.days < 4: nrecent3 += 1
                if latest.days < 8: nrecent7 += 1
                if latest.days < 31: nrecent30 += 1
                if latest.days < 91: nrecent90 += 1
        userstats['anajobs'] = anajobs
        userstats['n1000'] = n1000
        userstats['n10k'] = n10k
        userstats['nrecent3'] = nrecent3
        userstats['nrecent7'] = nrecent7
        userstats['nrecent30'] = nrecent30
        userstats['nrecent90'] = nrecent90
    else:
        if VOMODE == 'atlas':
            nhours = 12
        else:
            nhours = 7*24
        query = setupView(request, hours=nhours, limit=5000)
        ## dynamically assemble user summary info
        values = 'produsername','cloud','computingsite','cpuconsumptiontime','jobstatus','transformation','prodsourcelabel','specialhandling','vo','modificationtime','pandaid', 'atlasrelease', 'processingtype', 'workinggroup', 'currentpriority'
        jobs = QuerySetChain(\
                        Jobsdefined4.objects.filter(**query).order_by('-modificationtime')[:JOB_LIMIT].values(*values),
                        Jobsactive4.objects.filter(**query).order_by('-modificationtime')[:JOB_LIMIT].values(*values),
                        Jobswaiting4.objects.filter(**query).order_by('-modificationtime')[:JOB_LIMIT].values(*values),
                        Jobsarchived4.objects.filter(**query).order_by('-modificationtime')[:JOB_LIMIT].values(*values),
        )
        jobs = cleanJobList(jobs)
        sumd = userSummaryDict(jobs)
        sumparams = [ 'jobstatus', 'prodsourcelabel', 'specialhandling', 'transformation', 'processingtype', 'workinggroup', 'priorityrange', 'jobsetrange' ]
        if VOMODE == 'atlas':
            sumparams.append('atlasrelease')
        else:
            sumparams.append('vo')
        jobsumd = jobSummaryDict(request, jobs, sumparams)
    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'xurl' : extensibleURL(request),
            'url' : request.path,
            'sumd' : sumd,
            'jobsumd' : jobsumd,
            'userdb' : userdbl,
            'userstats' : userstats,
            'tfirst' : TFIRST,
            'tlast' : TLAST,
            'plow' : PLOW,
            'phigh' : PHIGH,
        }
        data.update(getContextVariables(request))
        return render_to_response('userList.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        resp = sumd
        return  HttpResponse(json.dumps(resp), mimetype='text/html')

def userInfo(request, user=''):
    valid, response = initRequest(request)
    if not valid: return response
    if user == '':
        if 'user' in requestParams: user = requestParams['user']
        if 'produsername' in requestParams: user = requestParams['produsername']

    ## Tasks owned by the user
    startdate = timezone.now() - timedelta(hours=7*24)
    startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = timezone.now().strftime(defaultDatetimeFormat)
    query = { 'modificationtime__range' : [startdate, enddate] }
    query['username__icontains'] = user.strip()
    tasks = JediTasks.objects.filter(**query).values()
    tasks = sorted(tasks, key=lambda x:-x['jeditaskid'])
    tasks = cleanTaskList(tasks)
    ntasks = len(tasks)
    tasksumd = taskSummaryDict(request,tasks)

    ## Jobs
    limit = 5000
    query = setupView(request,hours=72,limit=limit)
    query['produsername__icontains'] = user.strip()
    jobs = []
    values = 'produsername','cloud','computingsite','cpuconsumptiontime','jobstatus','transformation','prodsourcelabel','specialhandling','vo','modificationtime','pandaid', 'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid', 'currentpriority', 'creationtime', 'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag', 'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr', 'jobname', 'proddblock', 'destinationdblock',
    jobs.extend(Jobsdefined4.objects.filter(**query)[:JOB_LIMIT].values(*values))
    jobs.extend(Jobsactive4.objects.filter(**query)[:JOB_LIMIT].values(*values))
    jobs.extend(Jobswaiting4.objects.filter(**query)[:JOB_LIMIT].values(*values))
    jobs.extend(Jobsarchived4.objects.filter(**query)[:JOB_LIMIT].values(*values))
    jobsetids = None
    if len(jobs) == 0 or (len(jobs) < limit and LAST_N_HOURS_MAX > 72):
        jobs.extend(Jobsarchived.objects.filter(**query)[:JOB_LIMIT].values(*values))
#         if len(jobs) < limit and ntasks == 0:
#             ## try at least to find some old jobsets
#             startdate = timezone.now() - timedelta(hours=30*24)
#             startdate = startdate.strftime(defaultDatetimeFormat)
#             enddate = timezone.now().strftime(defaultDatetimeFormat)
#             query = { 'modificationtime__range' : [startdate, enddate] }
#             query['produsername'] = user
#             jobsetids = Jobsarchived.objects.filter(**query).values('jobsetid').distinct()
    jobs = cleanJobList(jobs)
    query = { 'name__icontains' : user.strip() }
    userdb = Users.objects.filter(**query).values()
    if len(userdb) > 0:
        userstats = userdb[0]
        user = userstats['name']
        for field in ['cpua1', 'cpua7', 'cpup1', 'cpup7' ]:
            try:
                userstats[field] = "%0.1f" % ( float(userstats[field])/3600.)
            except:
                userstats[field] = '-'
    else:
        userstats = None

    ## Divide up jobs by jobset and summarize
    jobsets = {}
    for job in jobs:
        if 'jobsetid' not in job or job['jobsetid'] == None: continue
        if job['jobsetid'] not in jobsets:
            jobsets[job['jobsetid']] = {}
            jobsets[job['jobsetid']]['jobsetid'] = job['jobsetid']
            jobsets[job['jobsetid']]['jobs'] = []
        jobsets[job['jobsetid']]['jobs'].append(job)
    for jobset in jobsets:
        jobsets[jobset]['sum'] = jobStateSummary(jobsets[jobset]['jobs'])
        jobsets[jobset]['njobs'] = len(jobsets[jobset]['jobs'])
        tfirst = timezone.now()
        tlast = timezone.now() - timedelta(hours=2400)
        plow = 1000000
        phigh = -1000000
        for job in jobsets[jobset]['jobs']:
            if job['modificationtime'] > tlast: tlast = job['modificationtime']
            if job['modificationtime'] < tfirst: tfirst = job['modificationtime']
            if job['currentpriority'] > phigh: phigh = job['currentpriority']
            if job['currentpriority'] < plow: plow = job['currentpriority']
        jobsets[jobset]['tfirst'] = tfirst
        jobsets[jobset]['tlast'] = tlast
        jobsets[jobset]['plow'] = plow
        jobsets[jobset]['phigh'] = phigh
    jobsetl = []
    jsk = jobsets.keys()
    jsk.sort(reverse=True)
    for jobset in jsk:
        jobsetl.append(jobsets[jobset])

    njobsmax = len(jobs)
    if 'display_limit' in requestParams and int(requestParams['display_limit']) < len(jobs):
        display_limit = int(requestParams['display_limit'])
        njobsmax = display_limit
        url_nolimit = removeParam(request.get_full_path(), 'display_limit')
    else:
        display_limit = 3000
        njobsmax = display_limit
        url_nolimit = request.get_full_path()

    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        sumd = userSummaryDict(jobs)
        flist =  [ 'jobstatus', 'prodsourcelabel', 'processingtype', 'specialhandling', 'transformation', 'jobsetid', 'jeditaskid', 'computingsite', 'cloud', 'workinggroup', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr', 'priorityrange', 'jobsetrange' ]
        if VOMODE != 'atlas':
            flist.append('vo')
        else:
            flist.append('atlasrelease')
        jobsumd = jobSummaryDict(request, jobs, flist)
        njobsetmax = 200
        xurl = extensibleURL(request)
        nosorturl = removeParam(xurl, 'sortby',mode='extensible')
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'xurl' : xurl,
            'nosorturl' : nosorturl,
            'user' : user,
            'sumd' : sumd,
            'jobsumd' : jobsumd,
            'jobList' : jobs[:njobsmax],
            'njobs' : len(jobs),
            'query' : query,
            'userstats' : userstats,
            'tfirst' : TFIRST,
            'tlast' : TLAST,
            'plow' : PLOW,
            'phigh' : PHIGH,
            'jobsets' : jobsetl[:njobsetmax-1],
            'njobsetmax' : njobsetmax,
            'njobsets' : len(jobsetl),
            'url_nolimit' : url_nolimit,
            'display_limit' : display_limit,
            'tasks': tasks,
            'ntasks' : ntasks,
            'tasksumd' : tasksumd,
        }
        data.update(getContextVariables(request))
        return render_to_response('userInfo.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        resp = sumd
        return  HttpResponse(json.dumps(resp), mimetype='text/html')

def siteList(request):
    valid, response = initRequest(request)
    if not valid: return response
    setupView(request, opmode='notime')
    query = {}
    ### Add any extensions to the query determined from the URL  
    if VOMODE == 'lsst': query['siteid__contains'] = 'LSST'
    prod = False
    for param in requestParams:
        if param == 'category' and requestParams[param] == 'multicloud':
            query['multicloud__isnull'] = False
        if param == 'category' and requestParams[param] == 'analysis':
            query['siteid__contains'] = 'ANALY'
        if param == 'category' and requestParams[param] == 'test':
            query['siteid__icontains'] = 'test'
        if param == 'category' and requestParams[param] == 'production':
            prod = True
        for field in Schedconfig._meta.get_all_field_names():
            if param == field:
                query[param] = requestParams[param]

    siteres = Schedconfig.objects.filter(**query).exclude(cloud='CMS').values()
    mcpres = Schedconfig.objects.filter(status='online').exclude(cloud='CMS').exclude(siteid__icontains='test').values('siteid','multicloud','cloud').order_by('siteid')
    sites = []
    for site in siteres:
        if 'category' in requestParams and requestParams['category'] == 'multicloud':
            if (site['multicloud'] == 'None') or (not re.match('[A-Z]+',site['multicloud'])): continue
        sites.append(site)
    if 'sortby' in requestParams and requestParams['sortby'] == 'maxmemory':
        sites = sorted(sites, key=lambda x:-x['maxmemory'])
    elif 'sortby' in requestParams and requestParams['sortby'] == 'maxtime':
        sites = sorted(sites, key=lambda x:-x['maxtime'])
    elif 'sortby' in requestParams and requestParams['sortby'] == 'gocname':
        sites = sorted(sites, key=lambda x:x['gocname'])
    else:
        sites = sorted(sites, key=lambda x:x['siteid'])
    if prod:
        newsites = []
        for site in sites:
            if site['siteid'].find('ANALY') >= 0:
                pass
            elif site['siteid'].lower().find('test') >= 0:
                pass
            else:
                newsites.append(site)
        sites = newsites
    for site in sites:
        if site['maxtime'] and (site['maxtime'] > 0) : site['maxtime'] = "%.1f" % ( float(site['maxtime'])/3600. )
        site['space'] = "%d" % (site['space']/1000.)

    if VOMODE == 'atlas' and (len(requestParams) == 0 or 'cloud' in requestParams):
        clouds = Cloudconfig.objects.filter().exclude(name='CMS').exclude(name='OSG').values()
        clouds = sorted(clouds, key=lambda x:x['name'])
        mcpsites = {}
        for cloud in clouds:
            cloud['display'] = True
            if 'cloud' in requestParams and requestParams['cloud'] != cloud['name']: cloud['display'] = False
            mcpsites[cloud['name']] = []
            for site in sites:
                if site['siteid'] == cloud['tier1']:
                    cloud['space'] = site['space']
                    cloud['tspace'] = site['tspace']
            for site in mcpres:
                mcpclouds = site['multicloud'].split(',')
                if cloud['name'] in mcpclouds or cloud['name'] == site['cloud']:
                    sited = {}
                    sited['name'] = site['siteid']
                    sited['cloud'] = site['cloud']
                    if site['cloud'] == cloud['name']:
                        sited['type'] = 'home'
                    else:
                        sited['type'] = 'mcp'
                    mcpsites[cloud['name']].append(sited)
            cloud['mcpsites'] = ''
            for s in mcpsites[cloud['name']]:
                if s['type'] == 'home':
                    cloud['mcpsites'] += "<b>%s</b> &nbsp; " % s['name']
                else:
                    cloud['mcpsites'] += "%s &nbsp; " % s['name']
    else:
        clouds = None
    xurl = extensibleURL(request)
    nosorturl = removeParam(xurl, 'sortby',mode='extensible')
    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        sumd = siteSummaryDict(sites)
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'sites': sites,
            'clouds' : clouds,
            'sumd' : sumd,
            'xurl' : xurl,
            'nosorturl' : nosorturl,
        }
        if 'cloud' in requestParams: data['mcpsites'] = mcpsites[requestParams['cloud']]
        #data.update(getContextVariables(request))
        return render_to_response('siteList.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        resp = sites
        return  HttpResponse(json.dumps(resp), mimetype='text/html')

def siteInfo(request, site=''):
    valid, response = initRequest(request)
    if not valid: return response
    if site == '' and 'site' in requestParams: site = requestParams['site']
    setupView(request)
    startdate = timezone.now() - timedelta(hours=LAST_N_HOURS_MAX)
    startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = timezone.now().strftime(defaultDatetimeFormat)
    query = {'siteid__iexact' : site}
    sites = Schedconfig.objects.filter(**query)
    colnames = []
    try:
        siterec = sites[0]
        colnames = siterec.get_all_fields()
    except IndexError:
        siterec = None
    HPC = False
    njobhours = 12
    if siterec.catchall.find('HPC') >= 0:
        HPC = True
        njobhours = 48

    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        attrs = []
        if siterec:
            attrs.append({'name' : 'GOC name', 'value' : siterec.gocname })
            if HPC: attrs.append({'name' : 'HPC', 'value' : 'This is a High Performance Computing (HPC) supercomputer queue' })
            if siterec.catchall.find('log_to_objectstore') >= 0:
                attrs.append({'name' : 'Object store logs', 'value' : 'Logging to object store is enabled' })
            if len(siterec.objectstore) > 0:
                fields = siterec.objectstore.split('|')
                nfields = len(fields)
                for nf in range (0, len(fields)):
                    if nf == 0:
                        attrs.append({'name' : 'Object store location', 'value' : fields[0] })
                    else:
                        fields2 = fields[nf].split('^')
                        if len(fields2) > 1:
                            ostype = fields2[0]
                            ospath = fields2[1]
                            attrs.append({'name' : 'Object store %s path' % ostype, 'value' : ospath })
                
            if siterec.nickname != site:
                attrs.append({'name' : 'Queue (nickname)', 'value' : siterec.nickname })
            if len(sites) > 1:
                attrs.append({'name' : 'Total queues for this site', 'value' : len(sites) })
            attrs.append({'name' : 'Status', 'value' : siterec.status })
            if len(siterec.comment_field) > 0:
                attrs.append({'name' : 'Comment', 'value' : siterec.comment_field })
            attrs.append({'name' : 'Cloud', 'value' : siterec.cloud })
            if len(siterec.multicloud) > 0:
                attrs.append({'name' : 'Multicloud', 'value' : siterec.multicloud })
            attrs.append({'name' : 'Tier', 'value' : siterec.tier })
            attrs.append({'name' : 'DDM endpoint', 'value' : siterec.ddm })
            attrs.append({'name' : 'Maximum memory', 'value' : "%.1f GB" % (float(siterec.maxmemory)/1000.) })
            if siterec.maxtime > 0:
                attrs.append({'name' : 'Maximum time', 'value' : "%.1f hours" % (float(siterec.maxtime)/3600.) })
            attrs.append({'name' : 'Space', 'value' : "%d TB as of %s" % ((float(siterec.space)/1000.), siterec.tspace.strftime('%m-%d %H:%M')) })
            attrs.append({'name' : 'Last modified', 'value' : "%s" % (siterec.lastmod.strftime('%Y-%m-%d %H:%M')) })

            iquery = {}
            startdate = timezone.now() - timedelta(hours=24*30)
            startdate = startdate.strftime(defaultDatetimeFormat)
            enddate = timezone.now().strftime(defaultDatetimeFormat)
            iquery['at_time__range'] = [startdate, enddate]
            iquery['description__contains'] = 'queue=%s' % siterec.nickname
            incidents = Incidents.objects.filter(**iquery).order_by('at_time').reverse().values()
        else:
            incidents = []
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'site' : siterec,
            'queues' : sites,
            'colnames' : colnames,
            'attrs' : attrs,
            'incidents' : incidents,
            'name' : site,
            'njobhours' : njobhours,
        }
        data.update(getContextVariables(request))
        return render_to_response('siteInfo.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        resp = []
        for job in jobList:
            resp.append({ 'pandaid': job.pandaid, 'status': job.jobstatus, 'prodsourcelabel': job.prodsourcelabel, 'produserid' : job.produserid})
        return  HttpResponse(json.dumps(resp), mimetype='text/html')

def siteSummary(query, notime=True):
    summary = []
    querynotime = copy.deepcopy(query)
    if notime: del querynotime['modificationtime__range']
    summary.extend(Jobsactive4.objects.filter(**querynotime).values('cloud','computingsite','jobstatus').annotate(Count('jobstatus')).order_by('cloud','computingsite','jobstatus'))     
    summary.extend(Jobsdefined4.objects.filter(**querynotime).values('cloud','computingsite','jobstatus').annotate(Count('jobstatus')).order_by('cloud','computingsite','jobstatus'))
    summary.extend(Jobswaiting4.objects.filter(**querynotime).values('cloud','computingsite','jobstatus').annotate(Count('jobstatus')).order_by('cloud','computingsite','jobstatus'))
    summary.extend(Jobsarchived4.objects.filter(**query).values('cloud','computingsite','jobstatus').annotate(Count('jobstatus')).order_by('cloud','computingsite','jobstatus'))
    return summary

def taskSummaryData(query):
    summary = []
    querynotime = query
    del querynotime['modificationtime__range']
    print "Task summary query", query
    summary.extend(Jobsactive4.objects.filter(**querynotime).values('taskid','jobstatus').annotate(Count('jobstatus')).order_by('taskid','jobstatus'))
    summary.extend(Jobsdefined4.objects.filter(**querynotime).values('taskid','jobstatus').annotate(Count('jobstatus')).order_by('taskid','jobstatus'))
    summary.extend(Jobswaiting4.objects.filter(**querynotime).values('taskid','jobstatus').annotate(Count('jobstatus')).order_by('taskid','jobstatus'))
    summary.extend(Jobsarchived4.objects.filter(**query).values('taskid','jobstatus').annotate(Count('jobstatus')).order_by('taskid','jobstatus'))
    summary.extend(Jobsactive4.objects.filter(**querynotime).values('jeditaskid','jobstatus').annotate(Count('jobstatus')).order_by('jeditaskid','jobstatus'))
    summary.extend(Jobsdefined4.objects.filter(**querynotime).values('jeditaskid','jobstatus').annotate(Count('jobstatus')).order_by('jeditaskid','jobstatus'))
    summary.extend(Jobswaiting4.objects.filter(**querynotime).values('jeditaskid','jobstatus').annotate(Count('jobstatus')).order_by('jeditaskid','jobstatus'))
    summary.extend(Jobsarchived4.objects.filter(**query).values('jeditaskid','jobstatus').annotate(Count('jobstatus')).order_by('jeditaskid','jobstatus'))

    return summary

def voSummary(query):
    summary = []
    querynotime = query
    del querynotime['modificationtime__range']
    summary.extend(Jobsactive4.objects.filter(**querynotime).values('vo','jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobsdefined4.objects.filter(**querynotime).values('vo','jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobswaiting4.objects.filter(**querynotime).values('vo','jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobsarchived4.objects.filter(**query).values('vo','jobstatus').annotate(Count('jobstatus')))
    return summary

def wgSummary(query):
    summary = []
    querynotime = query
    del querynotime['modificationtime__range']
    summary.extend(Jobsdefined4.objects.filter(**querynotime).values('workinggroup','jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobsactive4.objects.filter(**querynotime).values('workinggroup','jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobswaiting4.objects.filter(**querynotime).values('workinggroup','jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobsarchived4.objects.filter(**query).values('workinggroup','jobstatus').annotate(Count('jobstatus')))
    return summary

def wnSummary(query):
    summary = []
    querynotime = query
    # del querynotime['modificationtime__range']    ### creates inconsistency with job lists. Stick to advertised 12hrs
    summary.extend(Jobsactive4.objects.filter(**querynotime).values('modificationhost', 'jobstatus').annotate(Count('jobstatus')).order_by('modificationhost', 'jobstatus'))
    summary.extend(Jobsarchived4.objects.filter(**query).values('modificationhost', 'jobstatus').annotate(Count('jobstatus')).order_by('modificationhost', 'jobstatus'))
    return summary

def wnInfo(request,site,wnname='all'):
    """ Give worker node level breakdown of site activity. Spot hot nodes, error prone nodes. """
    valid, response = initRequest(request)
    if not valid: return response
    errthreshold = 15
    if wnname != 'all':
        query = setupView(request,hours=12,limit=999999)
        query['modificationhost__endswith'] = wnname
    else:
        query = setupView(request,hours=12,limit=999999)
    query['computingsite'] = site
    wnsummarydata = wnSummary(query)
    totstates = {}
    totjobs = 0
    wns = {}
    wnPlotFailed = {}
    wnPlotFinished = {}
    for state in sitestatelist:
        totstates[state] = 0
    for rec in wnsummarydata:
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        wnfull = rec['modificationhost']
        wnsplit = wnfull.split('@')
        if len(wnsplit) == 2:
            if wnname == 'all': 
                wn = wnsplit[1]
            else:
                wn = wnfull
            slot = wnsplit[0]
        else:
            wn = wnfull
            slot = '1'
        if wn.startswith('aipanda'): continue
        if jobstatus == 'failed':
            if not wn in wnPlotFailed: wnPlotFailed[wn] = 0
            wnPlotFailed[wn] += count
        elif jobstatus == 'finished':
            if not wn in wnPlotFinished: wnPlotFinished[wn] = 0
            wnPlotFinished[wn] += count
        totjobs += count
        if jobstatus not in totstates:
            totstates[jobstatus] = 0
        totstates[jobstatus] += count
        if wn not in wns:
            wns[wn] = {}
            wns[wn]['name'] = wn
            wns[wn]['count'] = 0
            wns[wn]['states'] = {}
            wns[wn]['slotd'] = {}
            wns[wn]['statelist'] = []
            for state in sitestatelist:
                wns[wn]['states'][state] = {}
                wns[wn]['states'][state]['name'] = state
                wns[wn]['states'][state]['count'] = 0
        if slot not in wns[wn]['slotd']: wns[wn]['slotd'][slot] = 0
        wns[wn]['slotd'][slot] += 1
        wns[wn]['count'] += count
        if jobstatus not in wns[wn]['states']:
            wns[wn]['states'][jobstatus]={}
            wns[wn]['states'][jobstatus]['count']=0
        wns[wn]['states'][jobstatus]['count'] += count

    ## Convert dict to summary list
    wnkeys = wns.keys()
    wnkeys.sort()
    wntot = len(wnkeys)
    fullsummary = []

    allstated = {}
    allstated['finished'] = allstated['failed'] = 0
    allwns = {}
    allwns['name'] = 'All'
    allwns['count'] = totjobs
    allwns['states'] = totstates
    allwns['statelist'] = []
    for state in sitestatelist:
        allstate = {}
        allstate['name'] = state
        allstate['count'] = totstates[state]
        allstated[state] = totstates[state]
        allwns['statelist'].append(allstate)
    if int(allstated['finished']) + int(allstated['failed']) > 0:
        allwns['pctfail'] = int(100.*float(allstated['failed'])/(allstated['finished']+allstated['failed']))
    else:
        allwns['pctfail'] = 0
    if wnname == 'all': fullsummary.append(allwns)
    avgwns = {}
    avgwns['name'] = 'Average'
    if wntot > 0:
        avgwns['count'] = "%0.2f" % (totjobs/wntot)
    else:
        avgwns['count'] = ''
    avgwns['states'] = totstates
    avgwns['statelist'] = []
    avgstates = {}
    for state in sitestatelist:
        if wntot > 0:
            avgstates[state] = totstates[state]/wntot
        else:
            avgstates[state] = ''
        allstate = {}
        allstate['name'] = state
        if wntot > 0:
            allstate['count'] = "%0.2f" % (int(totstates[state])/wntot)
            allstated[state] = "%0.2f" % (int(totstates[state])/wntot)
        else:
            allstate['count'] = ''
            allstated[state] = ''
        avgwns['statelist'].append(allstate)
        avgwns['pctfail'] = allwns['pctfail']
    if wnname == 'all': fullsummary.append(avgwns)

    for wn in wnkeys:
        outlier = ''
        wns[wn]['slotcount'] = len(wns[wn]['slotd'])
        wns[wn]['pctfail'] = 0
        for state in sitestatelist:
            wns[wn]['statelist'].append(wns[wn]['states'][state])      
        if wns[wn]['states']['finished']['count'] + wns[wn]['states']['failed']['count'] > 0:
            wns[wn]['pctfail'] = int(100.*float(wns[wn]['states']['failed']['count'])/(wns[wn]['states']['finished']['count']+wns[wn]['states']['failed']['count']))
        if float(wns[wn]['states']['finished']['count']) < float(avgstates['finished'])/5. :
            outlier += " LowFinished "
        if float(wns[wn]['states']['failed']['count']) > max(float(avgstates['failed'])*3.,5.) :
            outlier += " HighFailed "
        wns[wn]['outlier'] = outlier
        fullsummary.append(wns[wn])

    if 'sortby' in requestParams:
        if requestParams['sortby'] in sitestatelist:
            fullsummary = sorted(fullsummary, key=lambda x:x['states'][requestParams['sortby']],reverse=True)
        elif requestParams['sortby'] == 'pctfail':
            fullsummary = sorted(fullsummary, key=lambda x:x['pctfail'],reverse=True)

    kys = wnPlotFailed.keys()
    kys.sort()
    wnPlotFailedL = []
    for k in kys:
        wnPlotFailedL.append( [ k, wnPlotFailed[k] ] )

    kys = wnPlotFinished.keys()
    kys.sort()
    wnPlotFinishedL = []
    for k in kys:
        wnPlotFinishedL.append( [ k, wnPlotFinished[k] ] )

    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        xurl = extensibleURL(request)
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'url' : request.path,
            'xurl' : xurl,
            'site' : site,
            'wnname' : wnname,
            'user' : None,
            'summary' : fullsummary,
            'wnPlotFailed' : wnPlotFailedL,
            'wnPlotFinished' : wnPlotFinishedL,
            'hours' : LAST_N_HOURS_MAX,
            'errthreshold' : errthreshold,
        }
        return render_to_response('wnInfo.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        resp = []
        return  HttpResponse(json.dumps(resp), mimetype='text/html')

def dashSummary(request, hours, limit=999999, view='all', cloudview='region', notime=True):
    pilots = getPilotCounts(view)
    query = setupView(request,hours=hours,limit=limit,opmode=view)
    if VOMODE == 'atlas' and len(requestParams) == 0:
        cloudinfol = Cloudconfig.objects.filter().exclude(name='CMS').exclude(name='OSG').values('name','status')
    else:
        cloudinfol = []
    cloudinfo = {}
    for c in cloudinfol:
        cloudinfo[c['name']] = c['status']

    siteinfol = Schedconfig.objects.filter().exclude(cloud='CMS').values('siteid','status')
    siteinfo = {}
    for s in siteinfol:
        siteinfo[s['siteid']] = s['status']    

    sitesummarydata = siteSummary(query, notime)
    clouds = {}
    totstates = {}
    totjobs = 0
    for state in sitestatelist:
        totstates[state] = 0
    for rec in sitesummarydata:
        if cloudview == 'region':
            if rec['computingsite'] in homeCloud:
                cloud = homeCloud[rec['computingsite']]
            else:
                print "ERROR cloud not known", rec
                cloud = ''
        else:
            cloud = rec['cloud']
        site = rec['computingsite']
        if view.find('test') < 0:
            if view != 'analysis' and site.startswith('ANALY'): continue
            if view == 'analysis' and not site.startswith('ANALY'): continue
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        if jobstatus not in sitestatelist: continue
        totjobs += count
        totstates[jobstatus] += count
        if cloud not in clouds:
            clouds[cloud] = {}
            clouds[cloud]['name'] = cloud
            if cloud in cloudinfo: clouds[cloud]['status'] = cloudinfo[cloud]
            clouds[cloud]['count'] = 0
            clouds[cloud]['pilots'] = 0
            clouds[cloud]['sites'] = {}
            clouds[cloud]['states'] = {}
            clouds[cloud]['statelist'] = []
            for state in sitestatelist:
                clouds[cloud]['states'][state] = {}
                clouds[cloud]['states'][state]['name'] = state
                clouds[cloud]['states'][state]['count'] = 0
        clouds[cloud]['count'] += count
        clouds[cloud]['states'][jobstatus]['count'] += count
        if site not in clouds[cloud]['sites']:
            clouds[cloud]['sites'][site] = {}
            clouds[cloud]['sites'][site]['name'] = site
            if site in siteinfo: clouds[cloud]['sites'][site]['status'] = siteinfo[site]
            clouds[cloud]['sites'][site]['count'] = 0
            if site in pilots:
                clouds[cloud]['sites'][site]['pilots'] = pilots[site]['count']
                clouds[cloud]['pilots'] += pilots[site]['count']
            else:
                clouds[cloud]['sites'][site]['pilots'] = 0
            clouds[cloud]['sites'][site]['states'] = {}
            for state in sitestatelist:
                clouds[cloud]['sites'][site]['states'][state] = {}
                clouds[cloud]['sites'][site]['states'][state]['name'] = state
                clouds[cloud]['sites'][site]['states'][state]['count'] = 0
        clouds[cloud]['sites'][site]['count'] += count
        clouds[cloud]['sites'][site]['states'][jobstatus]['count'] += count

    ## Go through the sites, add any that are missing (because they have no jobs in the interval)
    if cloudview != 'cloud':
        for site in pandaSites:
            if view.find('test') < 0:
                if view != 'analysis' and site.startswith('ANALY'): continue
                if view == 'analysis' and not site.startswith('ANALY'): continue
            cloud = pandaSites[site]['cloud']
            if cloud not in clouds:
                ## Bail. Adding sites is one thing; adding clouds is another
                continue
            if site not in clouds[cloud]['sites']:
                clouds[cloud]['sites'][site] = {}
                clouds[cloud]['sites'][site]['name'] = site
                if site in siteinfo: clouds[cloud]['sites'][site]['status'] = siteinfo[site]
                clouds[cloud]['sites'][site]['count'] = 0
                clouds[cloud]['sites'][site]['pctfail'] = 0
                if site in pilots:
                    clouds[cloud]['sites'][site]['pilots'] = pilots[site]['count']
                    clouds[cloud]['pilots'] += pilots[site]['count']
                else:
                    clouds[cloud]['sites'][site]['pilots'] = 0
                clouds[cloud]['sites'][site]['states'] = {}
                for state in sitestatelist:
                    clouds[cloud]['sites'][site]['states'][state] = {}
                    clouds[cloud]['sites'][site]['states'][state]['name'] = state
                    clouds[cloud]['sites'][site]['states'][state]['count'] = 0

    ## Convert dict to summary list
    cloudkeys = clouds.keys()
    cloudkeys.sort()
    fullsummary = []
    allstated = {}
    allstated['finished'] = allstated['failed'] = 0
    allclouds = {}
    allclouds['name'] = 'All'
    allclouds['count'] = totjobs
    allclouds['pilots'] = 0
    allclouds['sites'] = {}
    allclouds['states'] = totstates
    allclouds['statelist'] = []
    for state in sitestatelist:
        allstate = {}
        allstate['name'] = state
        allstate['count'] = totstates[state]
        allstated[state] = totstates[state]
        allclouds['statelist'].append(allstate)
    if int(allstated['finished']) + int(allstated['failed']) > 0:
        allclouds['pctfail'] = int(100.*float(allstated['failed'])/(allstated['finished']+allstated['failed']))
    else:
        allclouds['pctfail'] = 0
    for cloud in cloudkeys:
        allclouds['pilots'] += clouds[cloud]['pilots']
    fullsummary.append(allclouds)
    for cloud in cloudkeys:
        for state in sitestatelist:
            clouds[cloud]['statelist'].append(clouds[cloud]['states'][state])
        sites = clouds[cloud]['sites']
        sitekeys = sites.keys()
        sitekeys.sort()        
        cloudsummary = []
        for site in sitekeys:
            sitesummary = []
            for state in sitestatelist:
                sitesummary.append(sites[site]['states'][state])
            sites[site]['summary'] = sitesummary
            if sites[site]['states']['finished']['count'] + sites[site]['states']['failed']['count'] > 0:
                sites[site]['pctfail'] = int(100.*float(sites[site]['states']['failed']['count'])/(sites[site]['states']['finished']['count']+sites[site]['states']['failed']['count']))
            else:
                sites[site]['pctfail'] = 0

            cloudsummary.append(sites[site])
        clouds[cloud]['summary'] = cloudsummary
        if clouds[cloud]['states']['finished']['count'] + clouds[cloud]['states']['failed']['count'] > 0:
            clouds[cloud]['pctfail'] =  int(100.*float(clouds[cloud]['states']['failed']['count'])/(clouds[cloud]['states']['finished']['count']+clouds[cloud]['states']['failed']['count']))

        fullsummary.append(clouds[cloud])

    if 'sortby' in requestParams:
        if requestParams['sortby'] in statelist:
            fullsummary = sorted(fullsummary, key=lambda x:x['states'][requestParams['sortby']],reverse=True)
            cloudsummary = sorted(cloudsummary, key=lambda x:x['states'][requestParams['sortby']],reverse=True)
            for cloud in clouds:
                clouds[cloud]['summary'] = sorted(clouds[cloud]['summary'], key=lambda x:x['states'][requestParams['sortby']]['count'],reverse=True)
        elif requestParams['sortby'] == 'pctfail':
            fullsummary = sorted(fullsummary, key=lambda x:x['pctfail'],reverse=True)
            cloudsummary = sorted(cloudsummary, key=lambda x:x['pctfail'],reverse=True)
            for cloud in clouds:
                clouds[cloud]['summary'] = sorted(clouds[cloud]['summary'], key=lambda x:x['pctfail'],reverse=True)
    return fullsummary

def dashTaskSummary(request, hours, limit=999999, view='all'):
    query = setupView(request,hours=hours,limit=limit,opmode=view) 

    tasksummarydata = taskSummaryData(query)
    tasks = {}
    totstates = {}
    totjobs = 0
    for state in sitestatelist:
        totstates[state] = 0

    taskids = []
    for rec in tasksummarydata:
        if 'jeditaskid' in rec and rec['jeditaskid'] and rec['jeditaskid'] > 0:
            taskids.append( { 'jeditaskid' : rec['jeditaskid'] } )
        elif 'taskid' in rec and rec['taskid'] and rec['taskid'] > 0 :
            taskids.append( { 'taskid' : rec['taskid'] } )
    tasknamedict = taskNameDict(taskids)

    for rec in tasksummarydata:
        if 'jeditaskid' in rec and rec['jeditaskid'] and rec['jeditaskid'] > 0:
            taskid = rec['jeditaskid']
            tasktype = 'JEDI'
        elif 'taskid' in rec and rec['taskid'] and rec['taskid'] > 0 :
            taskid = rec['taskid']
            tasktype = 'old'
        else:
            continue
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        if jobstatus not in sitestatelist: continue
        totjobs += count
        totstates[jobstatus] += count
        if taskid not in tasks:
            tasks[taskid] = {}
            tasks[taskid]['taskid'] = taskid
            if taskid in tasknamedict:
                tasks[taskid]['name'] = tasknamedict[taskid]
            else:
                tasks[taskid]['name'] = str(taskid)
            tasks[taskid]['count'] = 0
            tasks[taskid]['states'] = {}
            tasks[taskid]['statelist'] = []
            for state in sitestatelist:
                tasks[taskid]['states'][state] = {}
                tasks[taskid]['states'][state]['name'] = state
                tasks[taskid]['states'][state]['count'] = 0
        tasks[taskid]['count'] += count
        tasks[taskid]['states'][jobstatus]['count'] += count

    if view == 'analysis':
        ## Show only tasks starting with 'user.'
        kys = tasks.keys()
        for t in kys:
            if not str(tasks[t]['name'].encode('ascii','ignore')).startswith('user.'): del tasks[t]

    ## Convert dict to summary list
    taskkeys = tasks.keys()
    taskkeys.sort()
    fullsummary = []
    for taskid in taskkeys:
        for state in sitestatelist:
            tasks[taskid]['statelist'].append(tasks[taskid]['states'][state])
        if tasks[taskid]['states']['finished']['count'] + tasks[taskid]['states']['failed']['count'] > 0:
            tasks[taskid]['pctfail'] =  int(100.*float(tasks[taskid]['states']['failed']['count'])/(tasks[taskid]['states']['finished']['count']+tasks[taskid]['states']['failed']['count']))

        fullsummary.append(tasks[taskid])

    if 'sortby' in requestParams:
        if requestParams['sortby'] in sitestatelist:
            fullsummary = sorted(fullsummary, key=lambda x:x['states'][requestParams['sortby']],reverse=True)
        elif requestParams['sortby'] == 'pctfail':
            fullsummary = sorted(fullsummary, key=lambda x:x['pctfail'],reverse=True)

    return fullsummary

def dashboard(request, view='production'):
    valid, response = initRequest(request)
    if not valid: return response
    taskdays = 3

    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        VOMODE = 'atlas'
    else:
        VOMODE = ''
    if VOMODE != 'atlas':
        hours = 24*taskdays
    else:
        hours = 12

    hoursSinceUpdate = 36
    if view == 'production':
        noldtransjobs, transclouds, transrclouds = stateNotUpdated(request, state='transferring', hoursSinceUpdate=hoursSinceUpdate, count=True)
    else:
        hours = 3
        noldtransjobs = 0
        transclouds = []
        transrclouds = []

    errthreshold = 10

    query = setupView(request,hours=hours,limit=999999,opmode=view)

    if 'mode' in requestParams and requestParams['mode'] == 'task':
        return dashTasks(request, hours, view)

    if VOMODE != 'atlas':
        vosummarydata = voSummary(query)
        vos = {}
        for rec in vosummarydata:
            vo = rec['vo']
            #if vo == None: vo = 'Unassigned'
            if vo == None: continue
            jobstatus = rec['jobstatus']
            count = rec['jobstatus__count']
            if vo not in vos:
                vos[vo] = {}
                vos[vo]['name'] = vo
                vos[vo]['count'] = 0
                vos[vo]['states'] = {}
                vos[vo]['statelist'] = []
                for state in sitestatelist:
                    vos[vo]['states'][state] = {}
                    vos[vo]['states'][state]['name'] = state
                    vos[vo]['states'][state]['count'] = 0
            vos[vo]['count'] += count
            vos[vo]['states'][jobstatus]['count'] += count
        ## Convert dict to summary list
        vokeys = vos.keys()
        vokeys.sort()
        vosummary = []
        for vo in vokeys:
            for state in sitestatelist:
                vos[vo]['statelist'].append(vos[vo]['states'][state])
                if int(vos[vo]['states']['finished']['count']) + int(vos[vo]['states']['failed']['count']) > 0:
                    vos[vo]['pctfail'] = int(100.*float(vos[vo]['states']['failed']['count'])/(vos[vo]['states']['finished']['count']+vos[vo]['states']['failed']['count']))
            vosummary.append(vos[vo])

        if 'sortby' in requestParams:
            if requestParams['sortby'] in statelist:
                vosummary = sorted(vosummary, key=lambda x:x['states'][requestParams['sortby']],reverse=True)
            elif requestParams['sortby'] == 'pctfail':
                vosummary = sorted(vosummary, key=lambda x:x['pctfail'],reverse=True)

    else:
        if view == 'production':
            errthreshold = 5
        else:
            errthreshold = 15
        vosummary = []

    cloudview = 'cloud'
    if 'cloudview' in requestParams:
        cloudview = requestParams['cloudview']
    if view == 'analysis': cloudview = 'region'
    elif view != 'production': cloudview = 'N/A'

    fullsummary = dashSummary(request, hours, view, cloudview)

    cloudTaskSummary = wgTaskSummary(request,fieldname='cloud', view=view, taskdays=taskdays)

    request.session['max_age_minutes'] = 6
    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        xurl = extensibleURL(request)
        nosorturl = removeParam(xurl, 'sortby',mode='extensible')
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'url' : request.path,
            'xurl' : xurl,
            'nosorturl' : nosorturl,
            'user' : None,
            'summary' : fullsummary,
            'vosummary' : vosummary,
            'view' : view,
            'mode' : 'site',
            'cloudview': cloudview,
            'hours' : LAST_N_HOURS_MAX,
            'errthreshold' : errthreshold,
            'cloudTaskSummary' : cloudTaskSummary,
            'taskstates' : taskstatedict,
            'taskdays' : taskdays,
            'noldtransjobs' : noldtransjobs,
            'transclouds' : transclouds,
            'transrclouds' : transrclouds,
            'hoursSinceUpdate' : hoursSinceUpdate,
        }
        response = render_to_response('dashboard.html', data, RequestContext(request))
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes']*60)
        return response
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        resp = []
        return  HttpResponse(json.dumps(resp), mimetype='text/html')

def dashAnalysis(request):
    return dashboard(request,view='analysis')

def dashProduction(request):
    return dashboard(request,view='production')

def dashTasks(request, hours, view='production'):
    valid, response = initRequest(request)
    if not valid: return response

    if view == 'production':
        errthreshold = 5
    else:
        errthreshold = 15
    
    if 'days' in requestParams:
        taskdays = int(requestParams['days'])
    else:
        taskdays = 7
    hours = taskdays*24
    query = setupView(request,hours=hours,limit=999999,opmode=view, querytype='task')

    cloudTaskSummary = wgTaskSummary(request,fieldname='cloud', view=view, taskdays=taskdays)

    #taskJobSummary = dashTaskSummary(request, hours, view)     not particularly informative
    taskJobSummary = []  

    if 'display_limit' in requestParams:
        try:
            display_limit = int(requestParams['display_limit'])
        except:
            display_limit = 300
    else:
        display_limit = 300
    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        xurl = extensibleURL(request)
        nosorturl = removeParam(xurl, 'sortby',mode='extensible')
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'url' : request.path,
            'xurl' : xurl,
            'nosorturl' : nosorturl,
            'user' : None,
            'view' : view,
            'mode' : 'task',
            'hours' : hours,
            'errthreshold' : errthreshold,
            'cloudTaskSummary' : cloudTaskSummary,
            'taskstates' : taskstatedict,
            'taskdays' : taskdays,
            'taskJobSummary' : taskJobSummary[:display_limit],
            'display_limit' : display_limit,
        }
        return render_to_response('dashboard.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        resp = []
        return  HttpResponse(json.dumps(resp), mimetype='text/html')

@csrf_exempt
def taskList(request):
    valid, response = initRequest(request)
    if not valid: return response
    if 'tasktype' in requestParams and requestParams['tasktype'].startswith('anal'):
        hours = 3*24
    else:
        hours = 7*24
    eventservice = False
    if 'eventservice' in requestParams: eventservice = True
    if eventservice: hours = 7*24
    query = setupView(request, hours=hours, limit=9999999, querytype='task')
    if 'statenotupdated' in requestParams:
        tasks = taskNotUpdated(request, query)
    else:
        tasks = JediTasks.objects.filter(**query).values()

    tasks = cleanTaskList(tasks)
    ntasks = len(tasks)
    nmax = ntasks

    from django.db import connection
    #print 'SQL query:', connection.queries

    ## For event service, pull the jobs and event ranges
    if eventservice:
        taskl = []
        for task in tasks:
            taskl.append(task['jeditaskid'])
        jquery = {}
        jquery['jeditaskid__in'] = taskl
        jobs = []
        jobs.extend(Jobsactive4.objects.filter(**jquery).values('pandaid','jeditaskid'))
        jobs.extend(Jobsarchived4.objects.filter(**jquery).values('pandaid','jeditaskid'))
        taskdict = {}
        for job in jobs:
            taskdict[job['pandaid']] = job['jeditaskid']
        estaskdict = {}
        esjobs = []
        for job in jobs:
            esjobs.append(job['pandaid'])
        esquery = {}
        esquery['pandaid__in'] = esjobs
        evtable = JediEvents.objects.filter(**esquery).values('pandaid','status')
        for ev in evtable:
            taskid = taskdict[ev['pandaid']]
            if taskid not in estaskdict:
                estaskdict[taskid] = {}
                for s in eventservicestatelist:
                    estaskdict[taskid][s] = 0
            evstat = eventservicestatelist[ev['status']]
            estaskdict[taskid][evstat] += 1
        for task in tasks:
            taskid = task['jeditaskid']
            if taskid in estaskdict:
                estaskstr = ''
                for s in estaskdict[taskid]:
                    if estaskdict[taskid][s] > 0:
                        estaskstr += " %s(%s) " % ( s, estaskdict[taskid][s] )
                task['estaskstr'] = estaskstr

    if 'display_limit' in requestParams and int(requestParams['display_limit']) < nmax:
        display_limit = int(requestParams['display_limit'])
        nmax = display_limit
        url_nolimit = removeParam(request.get_full_path(), 'display_limit')
    else:
        display_limit = 3000
        nmax = display_limit
        url_nolimit = request.get_full_path()

    ## set up google flow diagram
    flowstruct = buildGoogleFlowDiagram(tasks=tasks)

    xurl = extensibleURL(request)
    nosorturl = removeParam(xurl, 'sortby',mode='extensible')
    if request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        return  HttpResponse(json.dumps(tasks, cls=DateEncoder), mimetype='text/html')
    else:
        sumd = taskSummaryDict(request,tasks)
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'tasks': tasks[:nmax],
            'ntasks' : ntasks,
            'sumd' : sumd,
            'xurl' : xurl,
            'nosorturl' : nosorturl,
            'url_nolimit' : url_nolimit,
            'display_limit' : display_limit,
            'flowstruct' : flowstruct,
        }
        if 'eventservice' in requestParams:
            return render_to_response('taskListES.html', data, RequestContext(request))
        else:
            return render_to_response('taskList.html', data, RequestContext(request))

def taskInfo(request, jeditaskid=0):
    jeditaskid = int(jeditaskid)
    valid, response = initRequest(request)
    if not valid: return response
    if 'taskname' in requestParams and requestParams['taskname'].find('*') >= 0:
        return taskList(request)
    setupView(request, hours=365*24, limit=999999999, querytype='task')
    eventservice = False
    query = {}
    tasks = []
    taskrec = None
    colnames = []
    columns = []
    jobsummary = []
    if 'jeditaskid' in requestParams: jeditaskid = int(requestParams['jeditaskid'])
    if jeditaskid != 0:
        query = {'jeditaskid' : jeditaskid}
        tasks = JediTasks.objects.filter(**query).values()
        if len(tasks) > 0:
            if 'eventservice' in tasks[0] and tasks[0]['eventservice'] == 1: eventservice = True
        if eventservice:
            jobsummary = jobSummary2(query, mode='eventservice')
        else:
            ## Exclude merge jobs. Can be misleading. Can show failures with no downstream successes.
            exclude = {'processingtype' : 'pmerge' }
            jobsummary = jobSummary2(query, exclude=exclude)
    elif 'taskname' in requestParams:
        querybyname = {'taskname' : requestParams['taskname'] }
        tasks = JediTasks.objects.filter(**querybyname).values()
        if len(tasks) > 0:
            jeditaskid = tasks[0]['jeditaskid']
        query = {'jeditaskid' : jeditaskid}

    tasks = cleanTaskList(tasks)
    try:
        taskrec = tasks[0]
        colnames = taskrec.keys()
        colnames.sort()
        for k in colnames:
            val = taskrec[k]
            if taskrec[k] == None:
                val = ''
                continue
            pair = { 'name' : k, 'value' : val }
            columns.append(pair)
    except IndexError:
        taskrec = None

    taskpars = JediTaskparams.objects.filter(**query).values()
    jobparams = None
    taskparams = None
    taskparaml = None
    jobparamstxt = []
    if len(taskpars) > 0:
        taskparams = taskpars[0]['taskparams']
        taskparams = json.loads(taskparams)
        tpkeys = taskparams.keys()
        tpkeys.sort()
        taskparaml = []
        for k in tpkeys:
            rec = { 'name' : k, 'value' : taskparams[k] }
            taskparaml.append(rec)
        jobparams = taskparams['jobParameters']
        jobparams.append(taskparams['log'])
        for p in jobparams:
            if p['type'] == 'constant':
                ptxt = p['value']
            elif p['type'] == 'template':
                ptxt = "<i>%s template:</i> value='%s' " % ( p['param_type'], p['value'] )
                for v in p:
                    if v in ['type', 'param_type', 'value' ]: continue
                    ptxt += "  %s='%s'" % ( v, p[v] )
            else:
                ptxt = '<i>unknown parameter type %s:</i> ' % p['type']
                for v in p:
                    if v in ['type', ]: continue
                    ptxt += "  %s='%s'" % ( v, p[v] )
            jobparamstxt.append(ptxt)
        jobparamstxt = sorted(jobparamstxt, key=lambda x:x.lower())

    if taskrec and 'ticketsystemtype' in taskrec and taskrec['ticketsystemtype'] == '' and taskparams != None:
        if 'ticketID' in taskparams: taskrec['ticketid'] = taskparams['ticketID']
        if 'ticketSystemType' in taskparams: taskrec['ticketsystemtype'] = taskparams['ticketSystemType']

    if taskrec:
        taskname = taskrec['taskname']
    elif 'taskname' in requestParams:
        taskname = requestParams['taskname']
    else:
        taskname = ''        

    logtxt = None
    if taskrec and taskrec['errordialog']:
        mat = re.match('^.*"([^"]+)"',taskrec['errordialog'])
        if mat:
            errurl = mat.group(1)
            cmd = "curl -s -f --compressed '%s'" % errurl
            logpfx = u"logtxt: %s\n" % cmd
            logout = commands.getoutput(cmd)
            if len(logout) > 0: logtxt = logout

    dsquery = {}
    dsquery['jeditaskid'] = jeditaskid
    dsets = JediDatasets.objects.filter(**dsquery).values()
    dsinfo = None
    nfiles = 0
    nfinished = 0
    nfailed = 0
    if len(dsets) > 0:
        for ds in dsets:
            if ds['type'] not in ['input', 'pseudo_input' ]: continue
            if ds['masterid']: continue
            if int(ds['nfiles']) > 0:
                nfiles += int(ds['nfiles'])
                nfinished += int(ds['nfilesfinished'])
                nfailed += int(ds['nfilesfailed'])
        dsets = sorted(dsets, key=lambda x:x['datasetname'].lower())
        if nfiles > 0:
            dsinfo = {}
            dsinfo['nfiles'] = nfiles
            dsinfo['nfilesfinished'] = nfinished
            dsinfo['nfilesfailed'] = nfailed
            dsinfo['pctfinished'] = int(100.*nfinished/nfiles)
            dsinfo['pctfailed'] = int(100.*nfailed/nfiles)
    if taskrec: taskrec['dsinfo'] = dsinfo

    ## get dataset types
    dstypesd = {}
    for ds in dsets:
        dstype = ds['type']
        if dstype not in dstypesd: dstypesd[dstype] = 0
        dstypesd[dstype] += 1
    dstkeys = dstypesd.keys()
    dstkeys.sort()
    dstypes = []
    for dst in dstkeys:
        dstd = { 'type' : dst, 'count' : dstypesd[dst] }
        dstypes.append(dstd)

    ## get input containers
    inctrs = []
    if taskparams and 'dsForIN' in taskparams:
        inctrs = [ taskparams['dsForIN'], ]

    ## get output containers
    cquery = {}
    cquery['jeditaskid'] = jeditaskid
    cquery['type__in'] = ( 'output', 'log' )
    outctrs = JediDatasets.objects.filter(**cquery).values_list('containername',flat=True).distinct()
    if len(outctrs) == 0 or outctrs[0] == '':
        outctrs = None

    ## For event service, pull the jobs and event ranges
    if eventservice:
        jquery = {}
        jquery['jeditaskid'] = jeditaskid
        jobs = []
        jobs.extend(Jobsactive4.objects.filter(**jquery).values('pandaid','jeditaskid'))
        jobs.extend(Jobsarchived4.objects.filter(**jquery).values('pandaid','jeditaskid'))
        taskdict = {}
        for job in jobs:
            taskdict[job['pandaid']] = job['jeditaskid']
        estaskdict = {}
        esjobs = []
        for job in jobs:
            esjobs.append(job['pandaid'])
        esquery = {}
        esquery['pandaid__in'] = esjobs
        evtable = JediEvents.objects.filter(**esquery).values('pandaid','status')
        for ev in evtable:
            taskid = taskdict[ev['pandaid']]
            if taskid not in estaskdict:
                estaskdict[taskid] = {}
                for s in eventservicestatelist:
                    estaskdict[taskid][s] = 0
            evstat = eventservicestatelist[ev['status']]
            estaskdict[taskid][evstat] += 1
        if jeditaskid in estaskdict:
            estaskstr = ''
            for s in estaskdict[jeditaskid]:
                if estaskdict[jeditaskid][s] > 0:
                    estaskstr += " %s(%s) " % ( s, estaskdict[jeditaskid][s] )
            taskrec['estaskstr'] = estaskstr

    if request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        resp = []
        return  HttpResponse(json.dumps(resp), mimetype='text/html') 
    else:
        attrs = []
        do_redirect = False
        try:
            if int(jeditaskid) > 0 and int(jeditaskid) < 4000000:
                do_redirect = True
        except:
            pass
        if do_redirect: 
            return redirect('http://panda.cern.ch/?taskname=%s&overview=taskinfo' % jeditaskid)
        if taskrec:
            attrs.append({'name' : 'Status', 'value' : taskrec['status'] })
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'task' : taskrec,
            'taskname' : taskname,
            'taskparams' : taskparams,
            'taskparaml' : taskparaml,
            'jobparams' : jobparamstxt,
            'columns' : columns,
            'attrs' : attrs,
            'jobsummary' : jobsummary,
            'jeditaskid' : jeditaskid,
            'logtxt' : logtxt,
            'datasets' : dsets,
            'dstypes' : dstypes,
            'inctrs' : inctrs,
            'outctrs' : outctrs,
        }
        data.update(getContextVariables(request))
        if eventservice:
            return render_to_response('taskInfoES.html', data, RequestContext(request))       
        else:
            return render_to_response('taskInfo.html', data, RequestContext(request))       

def jobSummaryForTasks(request):
    valid, response = initRequest(request)
    if not valid: return response
    tquery = {}
    tquery['status'] = 'running'
    tquery['tasktype'] = 'prod'
    tquery['jeditaskid__gt'] = 4000000
    tasklist = JediTasks.objects.filter(**tquery).values_list('jeditaskid', flat=True)
    query = setupView(request, hours=3*24, limit=9999999)
    query['jobstatus__in'] = [ 'running', 'finished', 'failed', 'cancelled', 'holding', 'transferring' ]
    query['jeditaskid__in'] = tasklist
    jobs = []
    jobs.extend(Jobsdefined4.objects.filter(**query).values('pandaid','jobstatus','jeditaskid'))
    jobs.extend(Jobswaiting4.objects.filter(**query).values('pandaid','jobstatus','jeditaskid'))
    jobs.extend(Jobsactive4.objects.filter(**query).values('pandaid','jobstatus','jeditaskid'))
    jobs.extend(Jobsarchived4.objects.filter(**query).values('pandaid','jobstatus','jeditaskid'))
    jobs.extend(Jobsarchived.objects.filter(**query).values('pandaid','jobstatus','jeditaskid'))

    ## Filter out the jobs superseded by retries
    retries = JediJobRetryHistory.objects.filter().order_by('newpandaid').values()
    droplist = []
    newjobs = []
    for job in jobs:
        dropJob = 0
        pandaid = job['pandaid']
        for retry in retries:
            if retry['oldpandaid'] == pandaid and retry['newpandaid'] != pandaid and (retry['relationtype'] == '' or retry['relationtype'] == 'retry'):
                ## there is a retry for this job. Drop it.
                dropJob = retry['newpandaid']
        if dropJob == 0:
            newjobs.append(job)
        else:
            droplist.append( { 'pandaid' : pandaid, 'newpandaid' : dropJob } )
    droplist = sorted(droplist, key=lambda x:-x['pandaid'])
    jobs = newjobs

    taskd = {}
    for job in jobs:
        task = job['jeditaskid']
        status = job['jobstatus']
        if task not in taskd:
            taskd[task] = {}
            for s in statelist:
                taskd[task][s] = 0
        taskd[task][status] += 1
    return taskd

def jobSummary2(query, exclude={}, mode='drop'):
    jobs = []
    jobs.extend(Jobsdefined4.objects.filter(**query).exclude(**exclude).values('pandaid','jobstatus','jeditaskid','processingtype'))
    jobs.extend(Jobswaiting4.objects.filter(**query).exclude(**exclude).values('pandaid','jobstatus','jeditaskid','processingtype'))
    jobs.extend(Jobsactive4.objects.filter(**query).exclude(**exclude).values('pandaid','jobstatus','jeditaskid','processingtype'))
    jobs.extend(Jobsarchived4.objects.filter(**query).exclude(**exclude).values('pandaid','jobstatus','jeditaskid','processingtype'))
    jobs.extend(Jobsarchived.objects.filter(**query).exclude(**exclude).values('pandaid','jobstatus','jeditaskid','processingtype'))

    if mode == 'drop':
        ## If the list is for a particular JEDI task, filter out the jobs superseded by retries
        taskids = {}
        for job in jobs:
            if 'jeditaskid' in job: taskids[job['jeditaskid']] = 1
        droplist = []
        if len(taskids) == 1:
            for task in taskids:
                retryquery = {}
                retryquery['jeditaskid'] = task
                retries = JediJobRetryHistory.objects.filter(**retryquery).order_by('newpandaid').values()
            newjobs = []
            for job in jobs:
                dropJob = 0
                pandaid = job['pandaid']
                for retry in retries:
                    if retry['oldpandaid'] == pandaid and retry['newpandaid'] != pandaid and (retry['relationtype'] == '' or retry['relationtype'] == 'retry' or ( job['processingtype'] == 'pmerge' and retry['relationtype'] == 'merge')):
                        ## there is a retry for this job. Drop it.
                        #print 'dropping job', pandaid, ' for retry ', retry['newpandaid']
                        dropJob = retry['newpandaid']
                if dropJob == 0:
                    newjobs.append(job)
                else:
                    droplist.append( { 'pandaid' : pandaid, 'newpandaid' : dropJob } )
            droplist = sorted(droplist, key=lambda x:-x['pandaid'])
            jobs = newjobs

    jobstates = []
    global statelist
    for state in statelist:
        statecount = {}
        statecount['name'] = state
        statecount['count'] = 0
        for job in jobs:
            if mode == 'eventservice' and job['jobstatus'] == 'cancelled':
                job['jobstatus'] = 'finished'
            if job['jobstatus'] == state:
                statecount['count'] += 1
                continue
        jobstates.append(statecount)
    return jobstates

def jobStateSummary(jobs):
    global statelist
    statecount = {}
    for state in statelist:
        statecount[state] = 0
    for job in jobs:
        statecount[job['jobstatus']] += 1
    return statecount

def errorSummaryDict(request,jobs, tasknamedict, testjobs):
    """ take a job list and produce error summaries from it """
    errsByCount = {}
    errsBySite = {}
    errsByUser = {}
    errsByTask = {}
    sumd = {}
    ## histogram of errors vs. time, for plotting
    errHist = {}
    flist = [ 'cloud', 'computingsite', 'produsername', 'taskid', 'jeditaskid', 'processingtype', 'prodsourcelabel', 'transformation', 'workinggroup', 'specialhandling', 'jobstatus' ]

    for job in jobs:
        if not testjobs:
            if job['jobstatus'] not in [ 'failed', 'holding' ]: continue
        site = job['computingsite']
        if 'cloud' in requestParams:
            if site in homeCloud and homeCloud[site] != requestParams['cloud']: continue
        user = job['produsername']
        taskname = ''
        if job['jeditaskid'] > 0:
            taskid = job['jeditaskid']
            if taskid in tasknamedict:
                taskname = tasknamedict[taskid]
            tasktype = 'jeditaskid'
        else:
            taskid = job['taskid']
            if taskid in tasknamedict:
                taskname = tasknamedict[taskid]
            tasktype = 'taskid'
        tm = job['modificationtime']
        tm = tm - timedelta(minutes=tm.minute % 30, seconds=tm.second, microseconds=tm.microsecond)
        if not tm in errHist: errHist[tm] = 0
        errHist[tm] += 1

        ## Overall summary
        for f in flist:
            if job[f]:
                if f == 'taskid' and job[f] < 1000000 and 'produsername' not in requestParams:
                    pass
                else:
                    if not f in sumd: sumd[f] = {}
                    if not job[f] in sumd[f]: sumd[f][job[f]] = 0
                    sumd[f][job[f]] += 1
        if job['specialhandling']:
            if not 'specialhandling' in sumd: sumd['specialhandling'] = {}
            shl = job['specialhandling'].split()
            for v in shl:
                if not v in sumd['specialhandling']: sumd['specialhandling'][v] = 0
                sumd['specialhandling'][v] += 1

        for err in errorcodelist:
            if job[err['error']] != 0 and  job[err['error']] != '' and job[err['error']] != None:
                errval = job[err['error']]
                ## error code of zero is not an error
                if errval == 0 or errval == '0' or errval == None: continue
                errdiag = ''
                try:
                    errnum = int(errval)
                    if err['error'] in errorCodes and errnum in errorCodes[err['error']]:
                        errdiag = errorCodes[err['error']][errnum]
                except:
                    errnum = errval
                errcode = "%s:%s" % ( err['name'], errnum )
                if err['diag']:
                    errdiag = job[err['diag']]
                    
                if errcode not in errsByCount:
                    errsByCount[errcode] = {}
                    errsByCount[errcode]['error'] = errcode
                    errsByCount[errcode]['codename'] = err['error']
                    errsByCount[errcode]['codeval'] = errnum
                    errsByCount[errcode]['diag'] = errdiag
                    errsByCount[errcode]['count'] = 0
                errsByCount[errcode]['count'] += 1
                
                if user not in errsByUser:
                    errsByUser[user] = {}
                    errsByUser[user]['name'] = user
                    errsByUser[user]['errors'] = {}
                    errsByUser[user]['toterrors'] = 0
                if errcode not in errsByUser[user]['errors']:
                    errsByUser[user]['errors'][errcode] = {}
                    errsByUser[user]['errors'][errcode]['error'] = errcode
                    errsByUser[user]['errors'][errcode]['codename'] = err['error']
                    errsByUser[user]['errors'][errcode]['codeval'] = errnum
                    errsByUser[user]['errors'][errcode]['diag'] = errdiag
                    errsByUser[user]['errors'][errcode]['count'] = 0
                errsByUser[user]['errors'][errcode]['count'] += 1
                errsByUser[user]['toterrors'] += 1

                if site not in errsBySite:
                    errsBySite[site] = {}
                    errsBySite[site]['name'] = site
                    errsBySite[site]['errors'] = {}
                    errsBySite[site]['toterrors'] = 0
                    errsBySite[site]['toterrjobs'] = 0
                if errcode not in errsBySite[site]['errors']:
                    errsBySite[site]['errors'][errcode] = {}
                    errsBySite[site]['errors'][errcode]['error'] = errcode
                    errsBySite[site]['errors'][errcode]['codename'] = err['error']
                    errsBySite[site]['errors'][errcode]['codeval'] = errnum
                    errsBySite[site]['errors'][errcode]['diag'] = errdiag
                    errsBySite[site]['errors'][errcode]['count'] = 0
                errsBySite[site]['errors'][errcode]['count'] += 1
                errsBySite[site]['toterrors'] += 1
                
                if tasktype == 'jeditaskid' or taskid > 1000000 or 'produsername' in requestParams:
                    if taskid not in errsByTask:
                        errsByTask[taskid] = {}
                        errsByTask[taskid]['name'] = taskid
                        errsByTask[taskid]['longname'] = taskname
                        errsByTask[taskid]['errors'] = {}
                        errsByTask[taskid]['toterrors'] = 0
                        errsByTask[taskid]['toterrjobs'] = 0
                        errsByTask[taskid]['tasktype'] = tasktype
                    if errcode not in errsByTask[taskid]['errors']:
                        errsByTask[taskid]['errors'][errcode] = {}
                        errsByTask[taskid]['errors'][errcode]['error'] = errcode
                        errsByTask[taskid]['errors'][errcode]['codename'] = err['error']
                        errsByTask[taskid]['errors'][errcode]['codeval'] = errnum
                        errsByTask[taskid]['errors'][errcode]['diag'] = errdiag
                        errsByTask[taskid]['errors'][errcode]['count'] = 0
                    errsByTask[taskid]['errors'][errcode]['count'] += 1
                    errsByTask[taskid]['toterrors'] += 1
        if site in errsBySite: errsBySite[site]['toterrjobs'] += 1
        if taskid in errsByTask: errsByTask[taskid]['toterrjobs'] += 1

                
    ## reorganize as sorted lists
    errsByCountL = []
    errsBySiteL = []
    errsByUserL = []
    errsByTaskL = []
    
    kys = errsByCount.keys()
    kys.sort()
    for err in kys:
        errsByCountL.append(errsByCount[err])
    if 'sortby' in requestParams and requestParams['sortby'] == 'count':
        errsByCountL = sorted(errsByCountL, key=lambda x:-x['count'])

    kys = errsByUser.keys()
    kys.sort()
    for user in kys:
        errsByUser[user]['errorlist'] = []
        errkeys = errsByUser[user]['errors'].keys()
        errkeys.sort()
        for err in errkeys:
            errsByUser[user]['errorlist'].append(errsByUser[user]['errors'][err])
        errsByUserL.append(errsByUser[user])
    if 'sortby' in requestParams and requestParams['sortby'] == 'count':
        errsByUserL = sorted(errsByUserL, key=lambda x:-x['toterrors'])

    kys = errsBySite.keys()
    kys.sort()
    for site in kys:
        errsBySite[site]['errorlist'] = []
        errkeys = errsBySite[site]['errors'].keys()
        errkeys.sort()
        for err in errkeys:
            errsBySite[site]['errorlist'].append(errsBySite[site]['errors'][err])
        errsBySiteL.append(errsBySite[site])
    if 'sortby' in requestParams and requestParams['sortby'] == 'count':
        errsBySiteL = sorted(errsBySiteL, key=lambda x:-x['toterrors'])

    kys = errsByTask.keys()
    kys.sort()
    for taskid in kys:
        errsByTask[taskid]['errorlist'] = []
        errkeys = errsByTask[taskid]['errors'].keys()
        errkeys.sort()
        for err in errkeys:
            errsByTask[taskid]['errorlist'].append(errsByTask[taskid]['errors'][err])
        errsByTaskL.append(errsByTask[taskid])
    if 'sortby' in requestParams and requestParams['sortby'] == 'count':
        errsByTaskL = sorted(errsByTaskL, key=lambda x:-x['toterrors'])

    suml = []
    for f in sumd:
        itemd = {}
        itemd['field'] = f
        iteml = []
        kys = sumd[f].keys()
        kys.sort()
        for ky in kys:
            iteml.append({ 'kname' : ky, 'kvalue' : sumd[f][ky] })
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x:x['field'])

    if 'sortby' in requestParams and requestParams['sortby'] == 'count':
        for item in suml:
            item['list'] = sorted(item['list'], key=lambda x:-x['kvalue'])

    kys = errHist.keys()
    kys.sort()
    errHistL = []
    for k in kys:
        errHistL.append( [ k, errHist[k] ] )

    return errsByCountL, errsBySiteL, errsByUserL, errsByTaskL, suml, errHistL

def getTaskName(tasktype,taskid):
    taskname = ''
    if tasktype == 'taskid':
        taskname = ''
    elif tasktype == 'jeditaskid' and taskid and taskid != 'None' :
        tasks = JediTasks.objects.filter(jeditaskid=taskid).values('taskname')
        if len(tasks) > 0:
            taskname = tasks[0]['taskname']
    return taskname

def errorSummary(request):
    valid, response = initRequest(request)
    if not valid: return response

    testjobs = False
    if 'prodsourcelabel' in requestParams and requestParams['prodsourcelabel'].lower().find('test') >= 0:
        testjobs = True

    jobtype = ''
    if 'jobtype' in requestParams:
        jobtype = requestParams['jobtype']
    elif '/analysis' in request.path:
        jobtype = 'analysis'
    elif '/production' in request.path:
        jobtype = 'production'
    elif testjobs:
        jobtype = 'rc_test'

    if jobtype == '':
        hours = 3
        limit = 50000
    elif jobtype.startswith('anal'):
        hours = 6
        limit = 50000
    else:
        hours = 12
        limit = 50000
    query = setupView(request, hours=hours, limit=limit)

    if not testjobs: query['jobstatus__in'] = [ 'failed', 'holding' ]

    jobs = []
    values = 'produsername', 'pandaid', 'cloud','computingsite','cpuconsumptiontime','jobstatus','transformation','prodsourcelabel','specialhandling','vo','modificationtime', 'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid', 'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag', 'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode', 'destinationse', 'currentpriority', 'computingelement'
    jobs.extend(Jobsdefined4.objects.filter(**query)[:JOB_LIMIT].values(*values))
    jobs.extend(Jobsactive4.objects.filter(**query)[:JOB_LIMIT].values(*values))
    jobs.extend(Jobswaiting4.objects.filter(**query)[:JOB_LIMIT].values(*values))
    jobs.extend(Jobsarchived4.objects.filter(**query)[:JOB_LIMIT].values(*values))
    jobs.extend(Jobsarchived.objects.filter(**query)[:JOB_LIMIT].values(*values))
    jobs = cleanJobList(jobs, mode='nodrop')
    njobs = len(jobs)

    tasknamedict = taskNameDict(jobs)

    ## Build the error summary.
    errsByCount, errsBySite, errsByUser, errsByTask, sumd, errHist = errorSummaryDict(request,jobs, tasknamedict, testjobs)

    ## Build the state summary and add state info to site error summary
    #notime = True
    #if testjobs: notime = False
    notime = False #### behave as it used to before introducing notime for dashboards. Pull only 12hrs.
    statesummary = dashSummary(request, hours, limit=limit, view=jobtype, cloudview='region', notime=notime)
    sitestates = {}
    savestates = [ 'finished', 'failed', 'cancelled', 'holding', ]
    for cloud in statesummary:
        for site in cloud['sites']:
            sitename = cloud['sites'][site]['name']
            sitestates[sitename] = {}
            for s in savestates:
                sitestates[sitename][s] = cloud['sites'][site]['states'][s]['count']
            sitestates[sitename]['pctfail'] = cloud['sites'][site]['pctfail']
            
    for site in errsBySite:
        sitename = site['name']
        if sitename in sitestates:
            for s in savestates:
                if s in sitestates[sitename]: site[s] = sitestates[sitename][s]
            if 'pctfail' in sitestates[sitename]: site['pctfail'] = sitestates[sitename]['pctfail']

    taskname = ''
    if not testjobs:
        ## Build the task state summary and add task state info to task error summary
        taskstatesummary = dashTaskSummary(request, hours, limit=limit, view=jobtype)
        taskstates = {}
        for task in taskstatesummary:
            taskid = task['taskid']
            taskstates[taskid] = {}
            for s in savestates:
                taskstates[taskid][s] = task['states'][s]['count']
            if 'pctfail' in task: taskstates[taskid]['pctfail'] = task['pctfail']

        for task in errsByTask:
            taskid = task['name']
            if taskid in taskstates:
                for s in savestates:
                    if s in taskstates[taskid]: task[s] = taskstates[taskid][s]
                if 'pctfail' in taskstates[taskid]: task['pctfail'] = taskstates[taskid]['pctfail']

        if 'jeditaskid' in requestParams:
            taskname = getTaskName('jeditaskid',requestParams['jeditaskid'])

    if 'sortby' in requestParams:
        sortby = requestParams['sortby']
    else:
        sortby = 'alpha'

    flowstruct = buildGoogleFlowDiagram(jobs=jobs)

    request.session['max_age_minutes'] = 6
    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        nosorturl = removeParam(request.get_full_path(), 'sortby')
        xurl = extensibleURL(request)
        jobsurl = xurl.replace('/errors/','/jobs/')
        data = {
            'prefix': getPrefix(request),
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'requestString' : request.META['QUERY_STRING'],
            'jobtype' : jobtype,
            'njobs' : njobs,
            'hours' : LAST_N_HOURS_MAX,
            'limit' : JOB_LIMIT,
            'user' : None,
            'xurl' : xurl,
            'jobsurl' : jobsurl,
            'nosorturl' : nosorturl,
            'errsByCount' : errsByCount,
            'errsBySite' : errsBySite,
            'errsByUser' : errsByUser,
            'errsByTask' : errsByTask,
            'sumd' : sumd,
            'errHist' : errHist,
            'tfirst' : TFIRST,
            'tlast' : TLAST,
            'sortby' : sortby,
            'taskname' : taskname,
            'flowstruct' : flowstruct,
        }
        data.update(getContextVariables(request))
        response = render_to_response('errorSummary.html', data, RequestContext(request))
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes']*60)
        return response
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        resp = []
        for job in jobs:
            resp.append({ 'pandaid': job.pandaid, 'status': job.jobstatus, 'prodsourcelabel': job.prodsourcelabel, 'produserid' : job.produserid})
        return  HttpResponse(json.dumps(resp), mimetype='text/html')

def removeParam(urlquery, parname, mode='complete'):
    """Remove a parameter from current query"""
    urlquery = urlquery.replace('&&','&')
    urlquery = urlquery.replace('?&','?')
    pstr = '.*(%s=[a-zA-Z0-9\.\-]*).*' % parname
    pat = re.compile(pstr)
    mat = pat.match(urlquery)
    if mat:
        pstr = mat.group(1)
        urlquery = urlquery.replace(pstr,'')
        urlquery = urlquery.replace('&&','&')
        urlquery = urlquery.replace('?&','?')
        if mode != 'extensible':
            if urlquery.endswith('?') or urlquery.endswith('&'): urlquery = urlquery[:len(urlquery)-1]
    return urlquery

def incidentList(request):
    valid, response = initRequest(request)
    if not valid: return response
    if 'days' in requestParams:
        hours = int(requestParams['days'])*24
    else:
        if 'hours' not in requestParams:
            hours = 24*3
        else:
            hours = int(requestParams['hours'])
    setupView(request, hours=hours, limit=9999999)
    iquery = {}
    startdate = timezone.now() - timedelta(hours=hours)
    startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = timezone.now().strftime(defaultDatetimeFormat)
    iquery['at_time__range'] = [startdate, enddate]
    if 'site' in requestParams:
        iquery['description__contains'] = 'queue=%s' % requestParams['site']
    if 'category' in requestParams:
        iquery['description__startswith'] = '%s:' % requestParams['category']
    if 'comment' in requestParams:
        iquery['description__contains'] = '%s' % requestParams['comment']
    if 'notifier' in requestParams:
        iquery['description__contains'] = 'DN=%s' % requestParams['notifier']
    incidents = Incidents.objects.filter(**iquery).order_by('at_time').reverse().values()
    sumd = {}
    pars = {}
    incHist = {}
    for inc in incidents:
        desc = inc['description']
        desc = desc.replace('&nbsp;',' ')
        parsmat = re.match('^([a-z\s]+):\s+queue=([^\s]+)\s+DN=(.*)\s\s\s*([A-Za-z^ \.0-9]*)$',desc)
        tm = inc['at_time']
        tm = tm - timedelta(minutes=tm.minute % 30, seconds=tm.second, microseconds=tm.microsecond)
        if not tm in incHist: incHist[tm] = 0
        incHist[tm] += 1
        if parsmat:
            pars['category'] = parsmat.group(1)
            pars['site'] = parsmat.group(2)
            pars['notifier'] = parsmat.group(3)
            pars['type'] = inc['typekey']
            if parsmat.group(4): pars['comment'] = parsmat.group(4)
        else:
            parsmat = re.match('^([A-Za-z\s]+):.*$',desc)
            if parsmat:
                pars['category'] = parsmat.group(1)
            else:
                pars['category'] = desc[:10]
        for p in pars:
            if p not in sumd:
                sumd[p] = {}
                sumd[p]['param'] = p
                sumd[p]['vals'] = {}
            if pars[p] not in sumd[p]['vals']:
                sumd[p]['vals'][pars[p]] = {}
                sumd[p]['vals'][pars[p]]['name'] = pars[p]
                sumd[p]['vals'][pars[p]]['count'] = 0
            sumd[p]['vals'][pars[p]]['count'] += 1
        ## convert incident components to URLs. Easier here than in the template.
        if 'site' in pars:
            inc['description'] = re.sub('queue=[^\s]+','queue=<a href="%ssite=%s">%s</a>' % (extensibleURL(request), pars['site'], pars['site']), inc['description'])

    ## convert to ordered lists
    suml = []
    for p in sumd:
        itemd = {}
        itemd['param'] = p
        iteml = []
        kys = sumd[p]['vals'].keys()
        kys.sort(key=lambda y: y.lower())
        for ky in kys:
            iteml.append({ 'kname' : ky, 'kvalue' : sumd[p]['vals'][ky]['count'] })
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x:x['param'].lower())

    kys = incHist.keys()
    kys.sort()
    incHistL = []
    for k in kys:
        incHistL.append( [ k, incHist[k] ] )

    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'user' : None,
            'incidents': incidents,
            'sumd' : suml,
            'incHist' : incHistL,
            'xurl' : extensibleURL(request),
            'hours' : hours,
            'ninc' : len(incidents),
        }
        return render_to_response('incidents.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        resp = incidents
        return  HttpResponse(json.dumps(resp), mimetype='text/html')

def pandaLogger(request):
    valid, response = initRequest(request)
    if not valid: return response
    getrecs = False
    iquery = {}
    if 'category' in requestParams:
        iquery['name'] = requestParams['category']
        getrecs = True
    if 'type' in requestParams:
        iquery['type'] = requestParams['type']
        getrecs = True
    if 'level' in requestParams:
        iquery['levelname'] = requestParams['level'].upper()
        getrecs = True
    if 'taskid' in requestParams:
        iquery['message__startswith'] = requestParams['taskid']
        getrecs = True
    if 'jeditaskid' in requestParams:
        iquery['message__icontains'] = "jeditaskid=%s" % requestParams['jeditaskid']
        getrecs = True
    if 'site' in requestParams:
        iquery['message__startswith'] = requestParams['site']
        getrecs = True
    if 'pandaid' in requestParams:
        iquery['pid'] = requestParams['pandaid']
        getrecs = True
    if 'hours' not in requestParams:
        if getrecs:
            hours = 72
        else:
            hours = 24
    else:
        hours = int(requestParams['hours'])
    setupView(request, hours=hours, limit=9999999)
    startdate = timezone.now() - timedelta(hours=hours)
    startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = timezone.now().strftime(defaultDatetimeFormat)
    iquery['bintime__range'] = [startdate, enddate]
    counts = Pandalog.objects.filter(**iquery).values('name','type','levelname').annotate(Count('levelname')).order_by('name','type','levelname')
    if getrecs:
        records = Pandalog.objects.filter(**iquery).order_by('bintime').reverse()[:JOB_LIMIT].values()
        ## histogram of logs vs. time, for plotting
        logHist = {}
        for r in records:
            r['message'] = r['message'].replace('<','')
            r['message'] = r['message'].replace('>','')
            r['levelname'] = r['levelname'].lower()
            tm = r['bintime']
            tm = tm - timedelta(minutes=tm.minute % 30, seconds=tm.second, microseconds=tm.microsecond)
            if not tm in logHist: logHist[tm] = 0
            logHist[tm] += 1
        kys = logHist.keys()
        kys.sort()
        logHistL = []
        for k in kys:
            logHistL.append( [ k, logHist[k] ] )
    else:
        records = None
        logHistL = None
    logs = {}
    totcount = 0
    for inc in counts:
        name = inc['name']
        type = inc['type']
        level = inc['levelname']
        count = inc['levelname__count']
        totcount += count
        if name not in logs:
            logs[name] = {}
            logs[name]['name'] = name
            logs[name]['count'] = 0
            logs[name]['types'] = {}
        logs[name]['count'] += count
        if type not in logs[name]['types']:
            logs[name]['types'][type] = {}
            logs[name]['types'][type]['name'] = type
            logs[name]['types'][type]['count'] = 0
            logs[name]['types'][type]['levels'] = {}
        logs[name]['types'][type]['count'] += count
        if level not in logs[name]['types'][type]['levels']:
            logs[name]['types'][type]['levels'][level] = {}
            logs[name]['types'][type]['levels'][level]['name'] = level.lower()
            logs[name]['types'][type]['levels'][level]['count'] = 0
        logs[name]['types'][type]['levels'][level]['count'] += count

    ## convert to ordered lists
    logl = []
    for l in logs:
        itemd = {}
        itemd['name'] = logs[l]['name']
        itemd['types'] = []
        for t in logs[l]['types']:
            logs[l]['types'][t]['levellist'] = []
            for v in logs[l]['types'][t]['levels']:
                logs[l]['types'][t]['levellist'].append(logs[l]['types'][t]['levels'][v])
            logs[l]['types'][t]['levellist'] = sorted(logs[l]['types'][t]['levellist'], key=lambda x:x['name'])
            typed = {}
            typed['name'] = logs[l]['types'][t]['name']
            itemd['types'].append(logs[l]['types'][t])
        itemd['types'] = sorted(itemd['types'], key=lambda x:x['name'])
        logl.append(itemd)
    logl = sorted(logl, key=lambda x:x['name'])

    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'user' : None,
            'logl' : logl,
            'records' : records,
            'ninc' : totcount,
            'logHist' : logHistL,
            'xurl' : extensibleURL(request),
            'hours' : hours,
            'getrecs' : getrecs,
        }
        return render_to_response('pandaLogger.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        resp = incidents
        return  HttpResponse(json.dumps(resp), mimetype='text/html')

def workingGroups(request):
    valid, response = initRequest(request)
    if not valid: return response
    taskdays = 3
    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        VOMODE = 'atlas'
    else:
        VOMODE = ''
    if VOMODE != 'atlas':
        days = 30
    else:
        days = taskdays
    hours = days*24
    query = setupView(request,hours=hours,limit=999999)
    query['workinggroup__isnull'] = False

    ## WG task summary
    tasksummary = wgTaskSummary(request, view='working group', taskdays=taskdays)

    ## WG job summary
    wgsummarydata = wgSummary(query)
    wgs = {}
    for rec in wgsummarydata:
        wg = rec['workinggroup']
        if wg == None: continue
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        if wg not in wgs:
            wgs[wg] = {}
            wgs[wg]['name'] = wg
            wgs[wg]['count'] = 0
            wgs[wg]['states'] = {}
            wgs[wg]['statelist'] = []
            for state in statelist:
                wgs[wg]['states'][state] = {}
                wgs[wg]['states'][state]['name'] = state
                wgs[wg]['states'][state]['count'] = 0
        wgs[wg]['count'] += count
        wgs[wg]['states'][jobstatus]['count'] += count

    errthreshold = 15
    ## Convert dict to summary list
    wgkeys = wgs.keys()
    wgkeys.sort()
    wgsummary = []
    for wg in wgkeys:
        for state in statelist:
            wgs[wg]['statelist'].append(wgs[wg]['states'][state])
            if int(wgs[wg]['states']['finished']['count']) + int(wgs[wg]['states']['failed']['count']) > 0:
                wgs[wg]['pctfail'] = int(100.*float(wgs[wg]['states']['failed']['count'])/(wgs[wg]['states']['finished']['count']+wgs[wg]['states']['failed']['count']))

        wgsummary.append(wgs[wg])
    if len(wgsummary) == 0: wgsummary = None

    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        xurl = extensibleURL(request)
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'url' : request.path,
            'xurl' : xurl,
            'user' : None,
            'wgsummary' : wgsummary,
            'taskstates' : taskstatedict,
            'tasksummary' : tasksummary,
            'hours' : hours,
            'days' : days,
            'errthreshold' : errthreshold,
        }
        return render_to_response('workingGroups.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        resp = []
        return  HttpResponse(json.dumps(resp), mimetype='text/html')

def datasetInfo(request):
    valid, response = initRequest(request)
    if not valid: return response
    setupView(request, hours=365*24, limit=999999999)
    query = {}
    dsets = []
    dsrec = None
    colnames = []
    columns = []
    if 'datasetname' in requestParams:
        dataset = requestParams['datasetname']
        query['datasetname'] = requestParams['datasetname']
    elif 'datasetid' in requestParams:
        dataset = requestParams['datasetid']
        query['datasetid'] = requestParams['datasetid']
    else:
        dataset = None
    
    if dataset:
        dsets = JediDatasets.objects.filter(**query).values()
        if len(dsets) == 0:
            startdate = timezone.now() - timedelta(hours=30*24)
            startdate = startdate.strftime(defaultDatetimeFormat)
            enddate = timezone.now().strftime(defaultDatetimeFormat)
            query = { 'modificationdate__range' : [startdate, enddate] }
            if 'datasetname' in requestParams:
                query['name'] = requestParams['datasetname']
            elif 'datasetid' in requestParams:
                query['vuid'] = requestParams['datasetid']
            moredsets = Datasets.objects.filter(**query).values()
            if len(moredsets) > 0:
                dsets = moredsets
                for ds in dsets:
                    ds['datasetname'] = ds['name']
                    ds['creationtime'] = ds['creationdate']
                    ds['modificationtime'] = ds['modificationdate']
                    ds['nfiles'] = ds['numberfiles']
                    ds['datasetid'] = ds['vuid']
    if len(dsets) > 0:
        dsrec = dsets[0]
        dataset = dsrec['datasetname']
        colnames = dsrec.keys()
        colnames.sort()
        for k in colnames:
            val = dsrec[k]
            if dsrec[k] == None:
                val = ''
                continue
            pair = { 'name' : k, 'value' : val }
            columns.append(pair)

    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'dsrec' : dsrec,
            'datasetname' : dataset,
            'columns' : columns,
        }
        data.update(getContextVariables(request))
        return render_to_response('datasetInfo.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        return  HttpResponse(json.dumps(dsrec), mimetype='text/html')

def datasetList(request):
    valid, response = initRequest(request)
    if not valid: return response
    setupView(request, hours=365*24, limit=999999999)
    query = {}
    dsets = []
    for par in ( 'jeditaskid', 'containername' ):
        if par in requestParams:
            query[par] = requestParams[par]
    
    if len(query) > 0:
        dsets = JediDatasets.objects.filter(**query).values()
        dsets = sorted(dsets, key=lambda x:x['datasetname'].lower())

    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        data = {
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'datasets' : dsets,
        }
        data.update(getContextVariables(request))
        return render_to_response('datasetList.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        return  HttpResponse(json.dumps(dsrec), mimetype='text/html')

def fileInfo(request):
    valid, response = initRequest(request)
    if not valid: return response
    setupView(request, hours=365*24, limit=999999999)
    query = {}
    files = []
    frec = None
    colnames = []
    columns = []
    if 'filename' in requestParams:
        file = requestParams['filename']
        query['lfn'] = requestParams['filename']
    elif 'lfn' in requestParams:
        file = requestParams['lfn']
        query['lfn'] = requestParams['lfn']
    elif 'fileid' in requestParams:
        file = requestParams['fileid']
        query['fileid'] = requestParams['fileid']
    elif 'guid' in requestParams:
        file = requestParams['guid']
        query['guid'] = requestParams['guid']
    else:
        file = None
    if 'scope' in requestParams:
        query['scope'] = requestParams['scope']
    if 'pandaid' in requestParams and requestParams['pandaid'] != '':
        query['pandaid'] = requestParams['pandaid']
    
    if file:
        files = JediDatasetContents.objects.filter(**query).values()
        if len(files) == 0:
            morefiles = Filestable4.objects.filter(**query).values()
            if len(morefiles) == 0:
                morefiles.extend(FilestableArch.objects.filter(**query).values())
            if len(morefiles) > 0:
                files = morefiles
                for f in files:
                    f['creationdate'] = f['modificationtime']
                    f['fileid'] = f['row_id']
                    f['datasetname'] = f['dataset']
                    f['oldfiletable'] = 1

        for f in files:
            f['fsizemb'] = "%0.2f" % (f['fsize']/1000000.)
            dsets = JediDatasets.objects.filter(datasetid=f['datasetid']).values()
            if len(dsets) > 0:
                f['datasetname'] = dsets[0]['datasetname']

    if len(files) > 0:
        files = sorted(files, key=lambda x:x['pandaid'], reverse=True)
        frec = files[0]
        file = frec['lfn']
        colnames = frec.keys()
        colnames.sort()
        for k in colnames:
            val = frec[k]
            if frec[k] == None:
                val = ''
                continue
            pair = { 'name' : k, 'value' : val }
            columns.append(pair)

    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'frec' : frec,
            'files' : files,
            'filename' : file,
            'columns' : columns,
        }
        data.update(getContextVariables(request))
        return render_to_response('fileInfo.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        return  HttpResponse(json.dumps(dsrec), mimetype='text/html')

def fileList(request):
    valid, response = initRequest(request)
    if not valid: return response
    setupView(request, hours=365*24, limit=999999999)
    query = {}
    files = []
    frec = None
    colnames = []
    columns = []
    datasetname = ''
    datasetid = 0
    if 'datasetname' in requestParams:
        datasetname = requestParams['datasetname']
        dsets = JediDatasets.objects.filter(datasetname=datasetname).values()
        if len(dsets) > 0:
            datasetid = dsets[0]['datasetid']
    elif 'datasetid' in requestParams:
        datasetid = requestParams['datasetid']
        dsets = JediDatasets.objects.filter(datasetid=datasetid).values()
        if len(dsets) > 0:
            datasetname = dsets[0]['datasetname']

    files = []
    if datasetid > 0:
        query['datasetid'] = datasetid
        files = JediDatasetContents.objects.filter(**query).values()
        for f in files:
            f['fsizemb'] = "%0.2f" % (f['fsize']/1000000.)

    ## Count the number of distinct files
    filed = {}
    for f in files:
        filed[f['lfn']] = 1
    nfiles = len(filed)

    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'files' : files,
            'nfiles' : nfiles,
        }
        data.update(getContextVariables(request))
        return render_to_response('fileList.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        return  HttpResponse(json.dumps(files), mimetype='text/html')

def workQueues(request):
    valid, response = initRequest(request)
    if not valid: return response
    setupView(request, hours=180*24, limit=9999999)
    query = {}
    for param in requestParams:
        for field in JediWorkQueue._meta.get_all_field_names():
            if param == field:
                query[param] = requestParams[param]
    queues = JediWorkQueue.objects.filter(**query).order_by('queue_type','queue_order').values()
    #queues = sorted(queues, key=lambda x:x['queue_name'],reverse=True)
        
    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        data = {
            'request' : request,
            'viewParams' : viewParams,
            'requestParams' : requestParams,
            'queues': queues,
            'xurl' : extensibleURL(request),
        }
        return render_to_response('workQueues.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        return  HttpResponse(json.dumps(queues), mimetype='text/html')

def stateNotUpdated(request, state='transferring', hoursSinceUpdate=36, values = standard_fields, count = False):
    valid, response = initRequest(request)
    if not valid: return response
    query = setupView(request, opmode='notime', limit=99999999)
    if 'jobstatus' in requestParams: state = requestParams['jobstatus']
    if 'transferringnotupdated' in requestParams: hoursSinceUpdate = int(requestParams['transferringnotupdated'])
    if 'statenotupdated' in requestParams: hoursSinceUpdate = int(requestParams['statenotupdated'])
    moddate = timezone.now() - timedelta(hours=hoursSinceUpdate)
    moddate = moddate.strftime(defaultDatetimeFormat)
    mindate = timezone.now() - timedelta(hours=24*30)
    mindate = mindate.strftime(defaultDatetimeFormat)
    query['statechangetime__lte'] = moddate
    #query['statechangetime__gte'] = mindate
    query['jobstatus'] = state
    if count:
        jobs = []
        jobs.extend(Jobsactive4.objects.filter(**query).values('cloud','computingsite','jobstatus').annotate(Count('jobstatus')))
        jobs.extend(Jobsdefined4.objects.filter(**query).values('cloud','computingsite','jobstatus').annotate(Count('jobstatus')))
        jobs.extend(Jobswaiting4.objects.filter(**query).values('cloud','computingsite','jobstatus').annotate(Count('jobstatus')))
        ncount = 0
        perCloud = {}
        perRCloud = {}
        for cloud in cloudList:
            perCloud[cloud] = 0
            perRCloud[cloud] = 0
        for job in jobs:
            site = job['computingsite']
            if site in homeCloud:
                cloud = homeCloud[site]
                if not cloud in perCloud:
                    perCloud[cloud] = 0
                perCloud[cloud] += job['jobstatus__count']
            cloud = job['cloud']
            if not cloud in perRCloud:
                perRCloud[cloud] = 0
            perRCloud[cloud] += job['jobstatus__count']
            ncount += job['jobstatus__count']
        perCloudl = []
        for c in perCloud:
            pcd = { 'name' : c, 'count' : perCloud[c] }
            perCloudl.append(pcd)
        perCloudl = sorted(perCloudl, key=lambda x:x['name'])
        perRCloudl = []
        for c in perRCloud:
            pcd = { 'name' : c, 'count' : perRCloud[c] }
            perRCloudl.append(pcd)
        perRCloudl = sorted(perRCloudl, key=lambda x:x['name'])
        return ncount, perCloudl, perRCloudl
    else:
        jobs = []
        jobs.extend(Jobsactive4.objects.filter(**query).values(*values))
        jobs.extend(Jobsdefined4.objects.filter(**query).values(*values))
        jobs.extend(Jobswaiting4.objects.filter(**query).values(*values))
        return jobs

def taskNotUpdated(request, query, state='submitted', hoursSinceUpdate=36, values = [], count = False):
    valid, response = initRequest(request)
    if not valid: return response
    #query = setupView(request, opmode='notime', limit=99999999)
    if 'status' in requestParams: state = requestParams['status']
    if 'statenotupdated' in requestParams: hoursSinceUpdate = int(requestParams['statenotupdated'])
    moddate = timezone.now() - timedelta(hours=hoursSinceUpdate)
    moddate = moddate.strftime(defaultDatetimeFormat)
    mindate = timezone.now() - timedelta(hours=24*30)
    mindate = mindate.strftime(defaultDatetimeFormat)
    query['statechangetime__lte'] = moddate
    #query['statechangetime__gte'] = mindate
    query['status'] = state

    if count:
        tasks = JediTasks.objects.filter(**query).values('name','status').annotate(Count('status'))
        statecounts = {}
        for s in taskstatelist:
            statecounts[s] = {}
            statecounts[s]['count'] = 0
            statecounts[s]['name'] = s
        ncount = 0
        for task in tasks:
            state = task['status']
            statecounts[state]['count'] += task['status__count']
            ncount += job['status__count']
        return ncount, statecounts
    else:
        tasks = JediTasks.objects.filter(**query).values()
        return tasks

def getErrorDescription(job, mode='html'):
    txt = ''
    if 'metastruct' in job and job['metastruct']['exitCode'] != 0:
        meta = job['metastruct']
        txt += "%s: %s" % (meta['exitAcronym'], meta['exitMsg'])
        return txt

    for errcode in errorCodes.keys():
        errval = 0
        if job.has_key(errcode):
            errval = job[errcode]
            if errval != 0 and errval != '0' and errval != None and errval != '':
                try:
                    errval = int(errval)                                                                                                                                                      
                except:
                    pass # errval = -1
                errdiag = errcode.replace('errorcode','errordiag')
                if errcode.find('errorcode') > 0:
                    diagtxt = job[errdiag]
                else:
                    diagtxt = ''
                if len(diagtxt) > 0:
                    desc = diagtxt
                elif errval in errorCodes[errcode]:
                    desc = errorCodes[errcode][errval]
                else:
                    desc = "Unknown %s error code %s" % ( errcode, errval )
                errname = errcode.replace('errorcode','')
                errname = errname.replace('exitcode','')
                if mode == 'html':
                    txt += " <b>%s:</b> %s" % ( errname, desc )                                                                                                                                                                                                                               
                else:
                    txt = "%s: %s" % ( errname, desc ) 
    return txt

def getPilotCounts(view):
    query = {}
    query['flag'] = view
    query['hours'] = 3
    rows = Sitedata.objects.filter(**query).values()
    pilotd = {}
    for r in rows:
        site = r['site']
        if not site in pilotd: pilotd[site] = {}
        pilotd[site]['count'] = r['getjob'] + r['updatejob']
        pilotd[site]['time'] = r['lastmod']
    return pilotd

def taskNameDict(jobs):
    ## Translate IDs to names. Awkward because models don't provide foreign keys to task records.
    taskids = {}
    jeditaskids = {}
    for job in jobs:
        if 'taskid' in job and job['taskid'] and job['taskid'] > 0:
            taskids[job['taskid']] = 1
        if 'jeditaskid' in job and job['jeditaskid'] and job['jeditaskid'] > 0: jeditaskids[job['jeditaskid']] = 1
    taskidl = taskids.keys()
    jeditaskidl = jeditaskids.keys()
    tasknamedict = {}
    if len(jeditaskidl) > 0:
        tq = { 'jeditaskid__in' : jeditaskidl }
        jeditasks = JediTasks.objects.filter(**tq).values('taskname', 'jeditaskid')
        for t in jeditasks:
            tasknamedict[t['jeditaskid']] = t['taskname']
    #if len(taskidl) > 0:
    #    tq = { 'taskid__in' : taskidl }
    #    oldtasks = Etask.objects.filter(**tq).values('taskname', 'taskid')
    #    for t in oldtasks:
    #        tasknamedict[t['taskid']] = t['taskname']
    return tasknamedict

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            return str(obj)
        return json.JSONEncoder.default(self, obj)

def getFilePathForObjectStore(objectstore, filetype="logs"):
    """ Return a proper file path in the object store """

    # For single object stores                                                                                                                                                                                                                          
    # root://atlas-objectstore.cern.ch/|eventservice^/atlas/eventservice|logs^/atlas/logs                                                                                                                                                               
    # For multiple object stores                                                                                                                                                                                                                        
    # eventservice^root://atlas-objectstore.cern.ch//atlas/eventservice|logs^root://atlas-objectstore.bnl.gov//atlas/logs                                                                                                                               

    basepath = ""

    # Which form of the schedconfig.objectstore field do we currently have?                                                                                                                                                                             
    if objectstore != "":
        _objectstore = objectstore.split("|")
        if "^" in _objectstore[0]:
            for obj in _objectstore:
                if obj[:len(filetype)] == filetype:
                    basepath = obj.split("^")[1]
                    break
        else:
            _objectstore = objectstore.split("|")
            url = _objectstore[0]
            for obj in _objectstore:
                if obj[:len(filetype)] == filetype:
                    basepath = obj.split("^")[1]
                    break
            if basepath != "":
                if url.endswith('/') and basepath.startswith('/'):
                    basepath = url + basepath[1:]
                else:
                    basepath = url + basepath

        if basepath == "":
            print "Object store path could not be extracted using file type \'%s\' from objectstore=\'%s\'" % (filetype, objectstore)

    else:
        print "Object store not defined in queuedata"

    return basepath

def buildGoogleFlowDiagram(jobs=[], tasks=[]):
    ## set up google flow diagram
    if 'flow' not in requestParams: return None
    flowstruct = {}
    if len(jobs) > 0:
        flowstruct['maxweight'] = len(jobs)
        flowrows = buildGoogleJobFlow(jobs)
    elif len(tasks) > 0:
        flowstruct['maxweight'] = len(tasks)
        flowrows = buildGoogleTaskFlow(tasks)
    else:
        return None
    flowstruct['columns'] = [ ['string', 'From'], ['string', 'To'], ['number', 'Weight'] ]
    flowstruct['rows'] = flowrows[:3000]
    return flowstruct

def buildGoogleJobFlow(jobs):
    cloudd = {}
    mcpcloudd = {}
    mcpshownd = {}
    errd = {}
    errshownd = {}
    sited = {}
    statd = {}
    errcountd = {}
    sitecountd = {}
    siteshownd = {}
    ptyped = {}
    ptypecountd = {}
    ptypeshownd = {}
    for job in jobs:
        errinfo = errorInfo(job,nchars=40,mode='string')
        jobstatus = job['jobstatus']
        for js in ( 'finished', 'holding', 'merging', 'running', 'cancelled', 'transferring', 'starting' ):
            if jobstatus == js: errinfo = js
        if errinfo not in errcountd: errcountd[errinfo] = 0
        errcountd[errinfo] += 1
        cloud = job['homecloud']
        mcpcloud = job['cloud']
        ptype = job['processingtype']
        if ptype not in ptypecountd: ptypecountd[ptype] = 0
        ptypecountd[ptype] += 1
        site = job['computingsite']
        if site not in sitecountd: sitecountd[site] = 0
        sitecountd[site] += 1

        if cloud not in cloudd: cloudd[cloud] = {}
        if site not in cloudd[cloud]: cloudd[cloud][site] = 0
        cloudd[cloud][site] += 1

        if mcpcloud not in mcpcloudd: mcpcloudd[mcpcloud] = {}
        if cloud not in mcpcloudd[mcpcloud]: mcpcloudd[mcpcloud][cloud] = 0
        mcpcloudd[mcpcloud][cloud] += 1

        if jobstatus not in errd: errd[jobstatus] = {}
        if errinfo not in errd[jobstatus]: errd[jobstatus][errinfo] = 0
        errd[jobstatus][errinfo] += 1

        if site not in sited: sited[site] = {}
        if errinfo not in sited[site]: sited[site][errinfo] = 0
        sited[site][errinfo] += 1

        if jobstatus not in statd: statd[jobstatus] = {}
        if errinfo not in statd[jobstatus]: statd[jobstatus][errinfo] = 0
        statd[jobstatus][errinfo] += 1


        if ptype not in ptyped: ptyped[ptype] = {}
        if errinfo not in ptyped[ptype]: ptyped[ptype][errinfo] = 0
        ptyped[ptype][errinfo] += 1

    flowrows = []

    for mcpcloud in mcpcloudd:
        for cloud in mcpcloudd[mcpcloud]:
            n = mcpcloudd[mcpcloud][cloud]
            if float(n)/len(jobs)>0.0:
                mcpshownd[mcpcloud] = 1
                flowrows.append( [ "%s MCP" % mcpcloud, cloud, n ] )

    othersited = {}
    othersiteErrd = {}

    for cloud in cloudd:
        if cloud not in mcpshownd: continue
        for e in cloudd[cloud]:
            n = cloudd[cloud][e]
            if float(sitecountd[e])/len(jobs)>.01:
                siteshownd[e] = 1
                flowrows.append( [ cloud, e, n ] )
            else:
                flowrows.append( [ cloud, 'Other sites', n ] )
                othersited[e] = n
    #for jobstatus in errd:
    #    for errinfo in errd[jobstatus]:
    #        flowrows.append( [ errinfo, jobstatus, errd[jobstatus][errinfo] ] )
    for e in errcountd:
        if float(errcountd[e])/len(jobs)>.01:
            errshownd[e] = 1

    for site in sited:
        nother = 0
        for e in sited[site]:
            n = sited[site][e]
            if site in siteshownd:
                sitename = site
            else:
                sitename = "Other sites"
            if e in errshownd:
                errname = e
            else:
                errname = 'Other errors'
            flowrows.append( [ sitename, errname, n ] )
            if errname not in othersiteErrd: othersiteErrd[errname] = 0
            othersiteErrd[errname] += n

    #for e in othersiteErrd:
    #    if e in errshownd:
    #        flowrows.append( [ 'Other sites', e, othersiteErrd[e] ] )

    for ptype in ptyped:
        if float(ptypecountd[ptype])/len(jobs)>.05:
            ptypeshownd[ptype] = 1
            ptname = ptype
        else:
            ptname = "Other processing types"
        for e in ptyped[ptype]:
            n = ptyped[ptype][e]
            if e in errshownd:
                flowrows.append( [ e, ptname, n ] )
            else:
                flowrows.append( [ 'Other errors', ptname, n ] )

    return flowrows

def buildGoogleTaskFlow(tasks):
    analysis = 'tasktype' in requestParams and requestParams['tasktype'].startswith('anal')
    ptyped = {}
    reqd = {}
    statd = {}
    substatd = {}
    trfd = {}
    filestatd = {}
    cloudd = {}
    reqsized = {}
    reqokd = {}
    ## count the reqid's. Use only the biggest (in file count) if too many.
    for task in tasks:
        if not analysis and 'deftreqid' not in task: continue
        req = int(task['reqid'])
        dsinfo = task['dsinfo']
        nfiles = dsinfo['nfiles']
        if req not in reqsized: reqsized[req] = 0
        reqsized[req] += nfiles
        ## Veto requests that are all done etc.
        if task['superstatus'] != 'done': reqokd[req] = 1

    if not analysis:
        for req in reqsized:
            # de-prioritize requests not specifically OK'd for inclusion
            if req not in reqokd: reqsized[req] = 0

        nmaxreq = 10
        if len(reqsized) > nmaxreq:
            reqkeys = reqsized.keys()
            reqsortl = sorted(reqkeys, key=reqsized.__getitem__, reverse=True)
            reqsortl = reqsortl[:nmaxreq-1]
        else:
            reqsortl = reqsized.keys()

    for task in tasks:
        ptype = task['processingtype']
        #if 'jedireqid' not in task: continue
        req = int(task['reqid'])
        if not analysis and req not in reqsortl: continue
        stat = task['superstatus']
        substat = task['status']
        #trf = task['transpath']
        trf = task['taskname']
        cloud = task['cloud']
        if cloud == '': cloud = 'No cloud assigned'
        dsinfo = task['dsinfo']
        nfailed = dsinfo['nfilesfailed']
        nfinished = dsinfo['nfilesfinished']
        nfiles = dsinfo['nfiles']
        npending = nfiles - nfailed - nfinished

        if ptype not in ptyped: ptyped[ptype] = {}
        if req not in ptyped[ptype]: ptyped[ptype][req] = 0
        ptyped[ptype][req] += nfiles

        if req not in reqd: reqd[req] = {}
        if stat not in reqd[req]: reqd[req][stat] = 0
        reqd[req][stat] += nfiles

        if trf not in trfd: trfd[trf] = {}
        if stat not in trfd[trf]: trfd[trf][stat] = 0
        trfd[trf][stat] += nfiles

        if stat not in statd: statd[stat] = {}
        if substat not in statd[stat]: statd[stat][substat] = 0
        statd[stat][substat] += nfiles

        if substat not in substatd: substatd[substat] = {}
        if 'finished' not in substatd[substat]:
            for filestat in ('finished', 'failed', 'pending'):
                substatd[substat][filestat] = 0
        substatd[substat]['finished'] += nfinished
        substatd[substat]['failed'] += nfailed
        substatd[substat]['pending'] += npending

        if cloud not in cloudd: cloudd[cloud] = {}
        if 'finished' not in cloudd[cloud]:
            for filestat in ('finished', 'failed', 'pending'):
                cloudd[cloud][filestat] = 0
        cloudd[cloud]['finished'] += nfinished
        cloudd[cloud]['failed'] += nfailed
        cloudd[cloud]['pending'] += npending

    flowrows = []

    if analysis:
        ## Don't include request, task for analysis
        for trf in trfd:
            for stat in trfd[trf]:
                n = trfd[trf][stat]
                flowrows.append( [ trf, 'Task %s' % stat, n ] )
    else:        
        for ptype in ptyped:
            for req in ptyped[ptype]:
                n = ptyped[ptype][req]
                flowrows.append( [ ptype, 'Request %s' % req, n ] )

        for req in reqd:
            for stat in reqd[req]:
                n = reqd[req][stat]
                flowrows.append( [ 'Request %s' % req, 'Task %s' % stat, n ] )

    for stat in statd:
        for substat in statd[stat]:
            n = statd[stat][substat]
            flowrows.append( [ 'Task %s' % stat, 'Substatus %s' % substat, n ] )

    for substat in substatd:
        for filestat in substatd[substat]:
            if filestat not in substatd[substat]: continue
            n = substatd[substat][filestat]
            flowrows.append( [ 'Substatus %s' % substat, 'File status %s' % filestat, n ] )

    for cloud in cloudd:
        for filestat in cloudd[cloud]:
            if filestat not in cloudd[cloud]: continue
            n = cloudd[cloud][filestat]
            flowrows.append( [ 'File status %s' % filestat, cloud, n ] )

    return flowrows

def addJobMetadata(jobs):
    print 'adding metadata'
    pids = []
    for job in jobs:
        if job['jobstatus'] == 'failed': pids.append(job['pandaid'])
    query = {}
    query['pandaid__in'] = pids

    mdict = {}
    ## Get job metadata
    mrecs = Metatable.objects.filter(**query).values()
    print 'got metadata'
    for m in mrecs:
        try:
            mdict[m['pandaid']] = m['metadata']
        except:
            pass

    for job in jobs:
        if job['pandaid'] in mdict:
            try:
                job['metastruct'] = json.loads(mdict[job['pandaid']])
            except:
                pass
                #job['metadata'] = mdict[job['pandaid']]
    print 'added metadata'

    return jobs

