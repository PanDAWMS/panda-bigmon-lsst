
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.shortcuts import render_to_response
from django.template import RequestContext
from core.pandajob.models import MonitorUsers
from django.core.cache import cache
import logging
import subprocess

userToRunVoms = 'atlpan'

class processAuth(object):
    
    def process_request(self, request):
        #logging.error('Something went wrong!')
        data = {'hi':'hi1'}

        if 'SSL_CLIENT_S_DN' in request.META or 'HTTP_X_SSL_CLIENT_S_DN' in request.META:
            if 'SSL_CLIENT_S_DN' in request.META:
               userdn = request.META['SSL_CLIENT_S_DN']
            else:
               userdn = request.META['HTTP_X_SSL_CLIENT_S_DN']
            userrec = MonitorUsers.objects.filter(dname__startswith=userdn, isactive=1).values()
            if len(userrec) > 0:
                logging.error(userrec)
                return None
            else:
                theListOfVMUsers = cache.get('voms-users-list')
                if (theListOfVMUsers is None) or (len(theListOfVMUsers) == 0):
                    proc = subprocess.Popen('sudo -u atlpan /usr/bin/voms-admin --host lcg-voms2.cern.ch --vo atlas list-users', shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    theListOfVMUsers, stderr = proc.communicate()
                    if len(theListOfVMUsers) < 20 :
                        logging.error('Error of getting list of users (voms-admin). stderr:' + theListOfVMUsers +" "+ stderr)
                        return render_to_response('errorAuth.html', data, RequestContext(request))
                    cache.set('voms-users-list', theListOfVMUsers, 1800)
                    if theListOfVMUsers.find(userdn) > 0:
                        newUser = MonitorUsers(dname=userdn, isactive=1)
                        newUser.save()
                        return None
                    else:
                        return render_to_response('errorAuth.html', data, RequestContext(request))
                
                
                return render_to_response('errorAuth.html', data, RequestContext(request))
#        else:
#            return render_to_response('errorAuth.html', RequestContext(request))
