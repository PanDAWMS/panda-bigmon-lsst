"""
    pandajob.utils
"""
import pytz
from datetime import datetime, timedelta

from django.conf import settings

# from ..common.settings import defaultDatetimeFormat
from core.common.settings import defaultDatetimeFormat

from core.resource.models import Schedconfig
from core.common.models import JediJobRetryHistory
from core.common.models import Users
from core.pandajob.models import Jobsactive4, Jobsdefined4, Jobswaiting4, \
    Jobsarchived4


homeCloud = {}
statelist = [ 'defined', 'waiting', 'assigned', 'activated', 'sent', \
             'running', 'holding', 'finished', 'failed', 'cancelled', \
             'transferring', 'starting', 'pending' ]
sitestatelist = [ 'assigned', 'activated', 'sent', 'starting', 'running', \
                 'holding', 'transferring', 'finished', 'failed', 'cancelled' ]
viewParams = {}
VOLIST = [ 'atlas', 'bigpanda', 'htcondor', 'lsst' ]
VONAME = { 'atlas' : 'ATLAS', \
           'bigpanda' : 'BigPanDA', \
           'htcondor' : 'HTCondor', \
           'lsst' : 'LSST', \
           '' : '' \
}
VOMODE = ' '
standard_fields = [ 'processingtype', 'computingsite', 'destinationse', \
                   'jobstatus', 'prodsourcelabel', 'produsername', \
                   'jeditaskid', 'taskid', 'workinggroup', 'transformation', \
                   'vo', 'cloud']
standard_sitefields = [ 'region', 'gocname', 'status', 'tier', '\
                        comment_field', 'cloud' ]
standard_taskfields = [ 'tasktype', 'status', 'corecount', 'taskpriority', \
                       'username', 'transuses', 'transpath', 'workinggroup', \
                       'processingtype', 'cloud', ]
LAST_N_HOURS_MAX = 0
#JOB_LIMIT = 0
JOB_LIMIT = 1000


def setupHomeCloud():
    global homeCloud
    if len(homeCloud) > 0:
        return
    sites = Schedconfig.objects.filter().exclude(cloud='CMS').values()
    for site in sites:
        homeCloud[site['siteid']] = site['cloud']


def setupView(request, opmode='', hours=0, limit=-99):
    global VOMODE
    global viewParams
    global LAST_N_HOURS_MAX, JOB_LIMIT
    global standard_fields

    print ':59 LAST_N_HOURS_MAX=', LAST_N_HOURS_MAX
    setupHomeCloud()
    settings.ENV['MON_VO'] = ''
    viewParams['MON_VO'] = ''
    VOMODE = ''
    for vo in VOLIST:
        if request.META['HTTP_HOST'].startswith(vo):
            VOMODE = vo
    ## If DB is Oracle, set vomode to atlas
#    if dbaccess['default']['ENGINE'].find('oracle') >= 0: VOMODE = 'atlas'
    if settings.DATABASES['default']['ENGINE'].find('oracle') >= 0: VOMODE = 'atlas'
    settings.ENV['MON_VO'] = VONAME[VOMODE]
    viewParams['MON_VO'] = settings.ENV['MON_VO']
    fields = standard_fields
    if VOMODE == 'atlas':
        LAST_N_HOURS_MAX = 12
        if 'hours' not in request.GET:
            JOB_LIMIT = 1000
        else:
            JOB_LIMIT = 1000
        if 'cloud' not in fields: fields.append('cloud')
        if 'atlasrelease' not in fields: fields.append('atlasrelease')
        if 'produsername' in request.GET or 'jeditaskid' in request.GET:
            if 'jobsetid' not in fields: fields.append('jobsetid')
            if 'hours' not in request.GET and ('jobsetid' in request.GET or 'taskid' in request.GET or 'jeditaskid' in request.GET):
                LAST_N_HOURS_MAX = 180 * 24
        else:
            if 'jobsetid' in fields: fields.remove('jobsetid')
    else:
        LAST_N_HOURS_MAX = 7 * 24
        JOB_LIMIT = 1000
    if hours > 0:
        ## Call param overrides default hours, but not a param on the URL
        LAST_N_HOURS_MAX = hours
    ## For site-specific queries, allow longer time window
    if 'computingsite' in request.GET:
        LAST_N_HOURS_MAX = 72
    ## hours specified in the URL takes priority over the above
    if 'hours' in request.GET:
        LAST_N_HOURS_MAX = int(request.GET['hours'])
    if limit != -99 and limit >= 0:
        ## Call param overrides default, but not a param on the URL
        JOB_LIMIT = limit
    if 'limit' in request.GET:
        JOB_LIMIT = int(request.GET['limit'])
    ## Exempt single-job, single-task etc queries from time constraint
    deepquery = False
    if 'jeditaskid' in request.GET: deepquery = True
    if 'taskid' in request.GET: deepquery = True
    if 'pandaid' in request.GET: deepquery = True
    if 'batchid' in request.GET: deepquery = True
    if deepquery:
        opmode = 'notime'
        LAST_N_HOURS_MAX = 24 * 365
    if opmode != 'notime':
        if LAST_N_HOURS_MAX <= 72 :
            viewParams['selection'] = ", last %s hours" % LAST_N_HOURS_MAX
        else:
            viewParams['selection'] = ", last %d days" % (float(LAST_N_HOURS_MAX) / 24.)
        if JOB_LIMIT < 100000 and JOB_LIMIT > 0:
            viewParams['selection'] += " (limit %s per table)" % JOB_LIMIT
        viewParams['selection'] += ". Query params: hours=%s" % LAST_N_HOURS_MAX
        if JOB_LIMIT < 100000 and JOB_LIMIT > 0:
            viewParams['selection'] += ", limit=%s" % JOB_LIMIT
    else:
        viewParams['selection'] = ""
    for param in request.GET:
        viewParams['selection'] += ", %s=%s " % (param, request.GET[param])
    print ':127 LAST_N_HOURS_MAX=', LAST_N_HOURS_MAX
    startdate = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(hours=LAST_N_HOURS_MAX)
    startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = datetime.utcnow().replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
    query = { 'modificationtime__range' : [startdate, enddate] }
    print ':134 query=', query
    ### Add any extensions to the query determined from the URL
    for vo in [ 'atlas', 'lsst' ]:
        if request.META['HTTP_HOST'].startswith(vo):
            query['vo'] = vo
    for param in request.GET:
        if param == 'cloud' and request.GET[param] == 'All': continue
        for field in Jobsactive4._meta.get_all_field_names():
            if param == field:
                if param == 'transformation' or param == 'transpath':
                    query['%s__endswith' % param] = request.GET[param]
                else:
                    query[param] = request.GET[param]
    if 'jobtype' in request.GET:
        jobtype = request.GET['jobtype']
    else:
        jobtype = opmode
    if jobtype == 'analysis':
        query['prodsourcelabel__in'] = ['panda', 'user']
    elif jobtype == 'production':
        query['prodsourcelabel'] = 'managed'
    elif jobtype == 'test':
        query['prodsourcelabel__contains'] = 'test'
    print ':157 query=', query
    return query


def cleanJobList(jobs, mode='drop'):
    for job in jobs:
        if not job['produsername']:
            if job['produserid']:
                job['produsername'] = job['produserid']
            else:
                job['produsername'] = 'Unknown'
        if job['transformation']: job['transformation'] = job['transformation'].split('/')[-1]
        if job['jobstatus'] == 'failed':
            job['errorinfo'] = errorInfo(job, nchars=50)
        else:
            job['errorinfo'] = ''
        job['jobinfo'] = ''
        if isEventService(job): job['jobinfo'] = 'Event service job'

    if mode == 'nodrop': return jobs
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
                if retry['oldpandaid'] == pandaid and retry['newpandaid'] != pandaid:
                    ## there is a retry for this job. Drop it.
                    print 'dropping', pandaid
                    dropJob = retry['newpandaid']
            if dropJob == 0:
                newjobs.append(job)
            else:
                droplist.append({ 'pandaid' : pandaid, 'newpandaid' : dropJob })
        droplist = sorted(droplist, key=lambda x:-x['pandaid'])
        jobs = newjobs
    jobs = sorted(jobs, key=lambda x:-x['pandaid'])
    return jobs


def cleanTaskList(tasks):
    for task in tasks:
        if task['transpath']: task['transpath'] = task['transpath'].split('/')[-1]
    return tasks


def siteSummaryDict(sites):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    global standard_sitefields
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
        if (site['multicloud'] is not None) and (site['multicloud'] != 'None') and (re.match('[A-Z]+', site['multicloud'])):
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
                sumd[user]['n' + state] = 0
            sumd[user]['nsites'] = 0
            sumd[user]['sites'] = {}
            sumd[user]['nclouds'] = 0
            sumd[user]['clouds'] = {}
            sumd[user]['nqueued'] = 0
#            sumd[user]['latest'] = timezone.now() - timedelta(hours=2400)
            sumd[user]['latest'] = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(hours=2400)
            sumd[user]['pandaid'] = 0
        cloud = job['cloud']
        site = job['computingsite']
        cpu = float(job['cpuconsumptiontime']) / 1.
        state = job['jobstatus']
        if job['modificationtime'] > sumd[user]['latest']: sumd[user]['latest'] = job['modificationtime']
        if job['pandaid'] > sumd[user]['pandaid']: sumd[user]['pandaid'] = job['pandaid']
        sumd[user]['cputime'] += cpu
        sumd[user]['njobs'] += 1
        sumd[user]['n' + state] += 1
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


def extensibleURL(request):
    """ Return a URL that is ready for p=v query extension(s) to be appended """
    xurl = request.get_full_path()
    if xurl.endswith('/'): xurl = xurl[0:len(xurl) - 1]
    if xurl.find('?') > 0:
        xurl += '&'
    else:
        xurl += '?'
    if 'jobtype' in request.GET:
        xurl += "jobtype=%s&" % request.GET['jobtype']
    return xurl


def errorInfo(job, nchars=300):
    errtxt = ''
    if int(job['brokerageerrorcode']) != 0:
        errtxt += 'Brokerage error %s: %s <br>' % (job['brokerageerrorcode'], job['brokerageerrordiag'])
    if int(job['ddmerrorcode']) != 0:
        errtxt += 'DDM error %s: %s <br>' % (job['ddmerrorcode'], job['ddmerrordiag'])
    if int(job['exeerrorcode']) != 0:
        errtxt += 'Executable error %s: %s <br>' % (job['exeerrorcode'], job['exeerrordiag'])
    if int(job['jobdispatchererrorcode']) != 0:
        errtxt += 'Dispatcher error %s: %s <br>' % (job['jobdispatchererrorcode'], job['jobdispatchererrordiag'])
    if int(job['piloterrorcode']) != 0:
        errtxt += 'Pilot error %s: %s <br>' % (job['piloterrorcode'], job['piloterrordiag'])
    if int(job['superrorcode']) != 0:
        errtxt += 'Sup error %s: %s <br>' % (job['superrorcode'], job['superrordiag'])
    if int(job['taskbuffererrorcode']) != 0:
        errtxt += 'Task buffer error %s: %s <br>' % (job['taskbuffererrorcode'], job['taskbuffererrordiag'])
    if job['transexitcode'] != '' and job['transexitcode'] is not None and int(job['transexitcode']) > 0:
        errtxt += 'Transformation exit code %s' % job['transexitcode']
    if len(errtxt) > nchars:
        ret = errtxt[:nchars] + '...'
    else:
        ret = errtxt[:nchars]
    return ret


def isEventService(job):
    if 'specialhandling' in job and job['specialhandling'].find('eventservice') >= 0:
        return True
    else:
        return False


def userList(request):
    nhours = 90 * 24
    query = setupView(request, hours=nhours, limit=-99)
    if VOMODE == 'atlas':
        view = 'database'
    else:
        view = 'dynamic'
    if 'view' in request.GET:
        view = request.GET['view']
    sumd = []
    jobsumd = []
    userdb = []
    userdbl = []
    userstats = {}
    if view == 'database':
        startdate = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(hours=nhours)
        startdate = startdate.strftime(defaultDatetimeFormat)
        enddate = datetime.utcnow().replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
        query = { 'latestjob__range' : [startdate, enddate] }
        #viewParams['selection'] = ", last %d days" % (float(nhours)/24.)
        ## Use the users table
        userdb = Users.objects.filter(**query).order_by('name')
        if 'sortby' in request.GET:
            sortby = request.GET['sortby']
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
            if u.cpua1: udict['cpua1'] = "%0.1f" % (int(u.cpua1) / 3600.)
            if u.cpua7: udict['cpua7'] = "%0.1f" % (int(u.cpua7) / 3600.)
            if u.cpup1: udict['cpup1'] = "%0.1f" % (int(u.cpup1) / 3600.)
            if u.cpup7: udict['cpup7'] = "%0.1f" % (int(u.cpup7) / 3600.)
            udict['latestjob'] = u.latestjob
            userdbl.append(udict)

            if u.njobsa > 0: anajobs += u.njobsa
            if u.njobsa >= 1000: n1000 += 1
            if u.njobsa >= 10000: n10k += 1
            if u.latestjob != None:
#                latest = datetime.utcnow() - u.latestjob.replace(tzinfo=None)
                latest = datetime.utcnow().replace(tzinfo=pytz.utc) - u.latestjob.replace(tzinfo=pytz.utc)
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
            nhours = 7 * 24
        query = setupView(request, hours=nhours, limit=3000)
        ## dynamically assemble user summary info
        values = 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime', 'pandaid'
        jobs = QuerySetChain(\
                        Jobsdefined4.objects.filter(**query).order_by('-modificationtime')[:JOB_LIMIT].values(*values),
                        Jobsactive4.objects.filter(**query).order_by('-modificationtime')[:JOB_LIMIT].values(*values),
                        Jobswaiting4.objects.filter(**query).order_by('-modificationtime')[:JOB_LIMIT].values(*values),
                        Jobsarchived4.objects.filter(**query).order_by('-modificationtime')[:JOB_LIMIT].values(*values),
        )
        for job in jobs:
            if job['transformation']: job['transformation'] = job['transformation'].split('/')[-1]
        sumd = userSummaryDict(jobs)
        jobsumd = jobSummaryDict(request, jobs, [ 'jobstatus', 'prodsourcelabel', 'specialhandling', 'vo', 'transformation', ])
    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        data = {
            'viewParams' : viewParams,
            'requestParams' : request.GET,
            'xurl' : extensibleURL(request),
            'url' : request.path,
            'sumd' : sumd,
            'jobsumd' : jobsumd,
            'userdb' : userdbl,
            'userstats' : userstats,
        }
        data.update(getContextVariables(request))
        return render_to_response('userList.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        resp = sumd
        return  HttpResponse(json_dumps(resp), mimetype='text/html')


def siteSummary(query):
    summary = []
    summary.extend(Jobsactive4.objects.filter(**query).values('cloud', 'computingsite', 'jobstatus').annotate(Count('jobstatus')).order_by('cloud', 'computingsite', 'jobstatus'))
    summary.extend(Jobsarchived4.objects.filter(**query).values('cloud', 'computingsite', 'jobstatus').annotate(Count('jobstatus')).order_by('cloud', 'computingsite', 'jobstatus'))
    return summary


def voSummary(query):
    summary = []
    summary.extend(Jobsactive4.objects.filter(**query).values('vo', 'jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobsarchived4.objects.filter(**query).values('vo', 'jobstatus').annotate(Count('jobstatus')))
    return summary


def wnSummary(query):
    summary = []
    summary.extend(Jobsactive4.objects.filter(**query).values('cloud', 'computingsite', 'modificationhost', 'jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobsarchived4.objects.filter(**query).values('cloud', 'computingsite', 'modificationhost', 'jobstatus').annotate(Count('jobstatus')))
    return summary


def jobStateSummary(jobs):
    global statelist
    statecount = {}
    for state in statelist:
        statecount[state] = 0
    for job in jobs:
        statecount[job['jobstatus']] += 1
    return statecount


