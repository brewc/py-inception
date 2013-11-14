import pika
import uuid
import json
import requests
from operator import itemgetter
import time

import logging
logging.getLogger('pika').setLevel(logging.DEBUG)

class InceptionClient(object):

    def __init__(self):

        queues = {'build','monitor','post-config','talkback'}
        exchange_queues = {'build', 'monitor', 'post-config'}
        
        # open the connection, build the channel, and declare the queue
        credentials = pika.PlainCredentials('inception','pass*word')
        parameters = pika.ConnectionParameters('app.chris.breu.org',5672,'/inception',credentials)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='inception', type='direct')
    
        # declare the queue,
        # bind to the cloud_worker exchange,
    
        for what_queue in queues:
            self.channel.queue_declare(queue=what_queue)
            
        for what_queue in exchange_queues:
            self.channel.queue_bind(exchange='inception',
                               routing_key=what_queue,
                               queue=what_queue)
        
        self.channel.basic_consume(self.on_response, no_ack=True,
                               queue='talkback')
 
        #set some defaults
        
        self.endpoints={
            'cloudFilesCDN':{'ORD':None, 'DFW':None},'cloudServers':{'rax':None}, 
            'cloudFiles':{'ORD':None, 'DFW':None}, 'cloudDatabases':{'ORD':None, 'DFW':None}, 
            'cloudDNS':{'rax':None}, 'cloudServersOpenStack':{'ORD':None, 'DFW':None, 'SYD':None, 'IAD':None, 'HKG':None}, 
            'cloudLoadBalancers':{'ORD':None,'DFW':None}, 'cloudBlockStorage':{'ORD':None, 'DFW':None},
            'cloudMonitoring':{'rax':None}
            } 
        
        self.access={'token':None,'token_expires':None,'ddi':0,'ddi':None}
        
        self.adminPass =''
        self.uuid =''
        self.ip=''
        self.role=''

    def auth(self, apiuser, apikey):
        auth_url='https://identity.api.rackspacecloud.com/v2.0/tokens'
        auth_headers={"Content-Type":"application/json"}

        auth_json=\
            """
                {"auth":{
                    "RAX-KSKEY:apiKeyCredentials":{
                        "username": "%s",
                        "apiKey": "%s"}
                        }
                }
            """ % (apiuser,apikey)
        auth_response=requests.post(auth_url, data=auth_json, headers=auth_headers)
        if auth_response.ok:
            
            # some defaults
            # [TODO]
            # since the api does not always return the serviceCatalog in the exact same order,
            # find the list index for cloudServersOpenStack
            # thanks Jordan!  Could also use list comprehension to find the value
            # [x['endpoints'][0]['publicURL'] for x in auth.json['access']['serviceCatalog'] if x['name'] == 'cloudServersOpenStack'] 
            #either way wrap it all in a wrapper for readability

            servers_index=map(itemgetter('name'), auth_response.json()['access']['serviceCatalog']).index('cloudServersOpenStack')
            self.auth_default_dc=auth_response.json()['access']['user']['RAX-AUTH:defaultRegion']

            auth_response_json = auth_response.json()
            
            for key in self.endpoints:
                find_index=map(itemgetter('name'), auth_response_json['access']['serviceCatalog']).index(key)
                for  key1, value in self.endpoints[key].iteritems():
                    if auth_response_json['access']['serviceCatalog'][find_index]['endpoints'][0].has_key('region'):
                        if auth_response_json['access']['serviceCatalog'][find_index]['endpoints'][0]['region'] == key1:
                            self.endpoints[key][key1]=auth_response.json()['access']['serviceCatalog'][find_index]['endpoints'][0]['publicURL']
                        else:
                            self.endpoints[key][key1]=auth_response.json()['access']['serviceCatalog'][find_index]['endpoints'][1]['publicURL'] 
                    else:
                        self.endpoints[key][key1]=auth_response.json()['access']['serviceCatalog'][find_index]['endpoints'][0]['publicURL']
  
  #          if self.auth_default_dc =='ORD':
  #              self.auth_server_url=auth_response.json['access']['serviceCatalog'][servers_index]['endpoints'][1]['publicURL']
  #          else:
  #              self.auth_server_url=auth_response.json['access']['serviceCatalog'][servers_index]['endpoints'][0]['publicURL']

            self.access['ddi']=auth_response_json['access']['token']['tenant']['id']
            self.access['token']=auth_response_json['access']['token']['id']
            self.access['token_expires']=auth_response_json['access']['token']['expires']
            
        return auth_response.ok

    # delete servers that have failed in the build
    def do_cleanup(self, server_url, token):
        delete_headers={"X-Auth-Token":token}
        delete_response = requests.delete(server_url, headers=delete_headers)
        if delete_response.ok:
            return delete_response.ok
        else:
            return delete_response.status_code

    # catch response from rabbitmq
    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    # all the things needed to build a server
    def build(self, server_url, token, flavor, image, name, meta, personality):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='inception',
                                   routing_key='build',
                                   properties=pika.BasicProperties(
                                         reply_to = 'talkback',
                                         correlation_id = self.corr_id,
                                         content_type="application/json",
                                         ),
                                   # set action to 'build' to build a server
                                   body=json.dumps({'action':'build','endpoint':server_url, 'token':token, 'flavor':flavor, 'image':image, 'name':name, 'role':meta, 'personality':personality}))
        while self.response is None:
            self.connection.process_data_events()
        return self.response
    
    # send monitor reuqe
    def monitor(self, url, token):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='inception',
                                   routing_key='monitor',
                                   properties=pika.BasicProperties(
                                       reply_to = 'talkback',
                                       correlation_id = self.corr_id,
                                       content_type='application/json',
                                       ),
                                   body=json.dumps({'action':'monitor','url':url,'token':token}))
        while self.response is None:
            self.connection.process_data_events()
        return self.response

print " [x] Starting client"
server = InceptionClient()

auth = server.auth('rackerchris','ba56a317b503fa6261183a262e7e9676')

if auth:
    print " [x] Valid Auth.  Using Token: %s, ddi: %s" % (server.access['token'], server.access['ddi'])
    
    print " [x] Calling server build"
    
    ### define the status dict
    
    def monitor_build(url, token):
        monitor = server.monitor(build_response['url'],server.access['token'])
        monitor_response= json.loads(monitor)
        server.ip=monitor_response['ip']
        is_building = monitor_response['status']
        print "    [x] Server Status: %s, Server Public IP: %s. Sleeping for 10 seconds" % (monitor_response['status'], server.ip)
        time.sleep(10)
        
    response = server.build(server.endpoints['cloudServersOpenStack']['ORD'],server.access['token'],2,'c195ef3b-9195-4474-b6f7-16e5bd86acd0','testrabbitbuild','webhead','')

    build_response = json.loads(response)

    if build_response['valid'] == 'true':
        print "    [x] Successfull call to the API for a build."
        print "    [x] Starting [MONITOR]."
        server.adminPass = build_response['adminPass']
        server.uuid = build_response['body']
        server.role = build_response['role']

        is_building ='BUILD'
        while is_building =='BUILD':
            monitor = server.monitor(build_response['url'],server.access['token'])
            monitor_response= json.loads(monitor)
            server.ip=monitor_response['ip']
            is_building = monitor_response['status']
            print "    [x] Server Status: %s, Server Public IP: %s. Sleeping for 10 seconds" % (monitor_response['status'], server.ip)
            time.sleep(10)
        
        monitor_response= json.loads(monitor)
        if monitor_response =='ERROR':
            delete_server=server.do_cleanup(build_response['url'], server.auth_token)
            if delete_server =='True':
                print "    [x] Server has been sucessfully scheduled for delete"
            else:
                print "    [x] Server delete api called returned: %s" % (delete_server,)
        
        if monitor =='ACTIVE':
            print """
            Server has been built.  Here's the info.
            adminPass : %s
            uuid      : %s
            public_ip : %s
            role      : %s
            """ % (server.adminPass, server.uuid, server.ip, server.role)
            print "    [x] Moving to configure with knife for the selected role"
            pass
    else:
        print build_response['body']
else:
    print " [x] Invalid Auth Attempt."
