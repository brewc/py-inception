import pika
import requests
import json
import re
from operator import itemgetter
import logging
logging.getLogger('pika').setLevel(logging.DEBUG)

def do_rq_connect(host, port, queue):

    print "[x] Building the queue: %s" % (queue)

    try:
        credentials = pika.PlainCredentials('inception','pass*word')
        parameters = pika.ConnectionParameters('app.chris.breu.org',5672,'/inceptions',credentials)

        connection = pika.BlockingConnection(pika.ConnectionParameters(parameters))

        channel = connection.channel()
    
        channel.queue_declare(queue=queue)
        print "[.] Connection sucessfully established to rabbitMQ, queues and exchanges built."
        return channel
    except Exception:
        print "[x] Unable to create the connection to the rabbitMQ server using host:%s, port: %d" % (host, port)
        pass

# do_build: returns json formated: valid:[t/f], body:[uuid], url:[href], adminPass, and role
def do_build(server_url, token, flavor, image, name, role, personality):
    print "    [x] API Call Values: server_url:%s token:%s flavor:%d image:%s name:%s role:%s personality:%s" % (server_url, token, int(flavor), image, name, role, personality)
    headers = {"X-Auth-Token":token,"Content-Type":"application/json"}
    server_json = \
            """
                {
                "server": {
                "name": "%s",
                "min_count": 1,
                "max_count": 1,
                "imageRef": "%s",
                "flavorRef": %d,
                "metadata": {
                    "role" : "%s"
                },
                "personality": [{
                    "path": "/etc/banner.txt",
                    "contents": "VEhJUyBJUyBBIFBFUlNPTkFMSVRZIFRFU1Q="
                }]
                }
                }
            """ % (name, image, int(flavor), role)

    api_call=requests.post(server_url+'/servers', data=server_json, headers=headers)
    api_call_json = api_call.json()
    
    if re.match('^[23]', str(api_call.status_code)):
        print "    [x] Status of api_call is %s uuid:%s" % (api_call.status_code,api_call_json['server']['id'])
        return json.dumps({'valid':'true', 'body' : api_call_json['server']['id'], 'url' : api_call_json['server']['links'][0]['href'],'adminPass':api_call_json['server']['adminPass'],'role':role})
    else:
        body = "     [x] API CALL [FAILED] with status code:%s!" % (api_call.status_code,)
        return json.dumps({'valid':'false', 'body' : body})

def get_status(url,token):
    headers = {'X-Auth-Token':token}
    api_call=requests.get(url, headers=headers)
    api_call_json = api_call.json()
    
    server_ip=''

    if re.match('^[23]', str(api_call.status_code)):
        if api_call_json['server']['addresses']:
            server_ip_index=map(itemgetter('version'), api_call_json['server']['addresses']['public']).index(4)
            server_ip=api_call_json['server']['addresses']['public'][server_ip_index]['addr']

        return json.dumps({'valid':'true', 'status': api_call_json['server']['status'], 'ip': server_ip, 'body':''})
    else:
        body="     [x] API CALL [FAILED] with status code:%s!" % (api_call.status_code,)
        return json.dumps({'valid':'false', 'body': body})
   

def on_build_request(ch,method,props,body):

    request = json.loads(body)

    if method.routing_key=='build':
    #if request['action'] == 'build':
        print "    [.] Calling do_build"
        response = do_build(request['endpoint'],request['token'],request['flavor'],request['image'],request['name'],request['role'],request['personality'])

        ch.basic_publish(exchange='',
                        routing_key=props.reply_to,
                        properties=pika.BasicProperties(correlation_id = \
                                                        props.correlation_id,
                                                        reply_to = 'talkback'),
                        body=str(response))
        ch.basic_ack(delivery_tag = method.delivery_tag)
        return

def on_monitor_request(ch,method,props,body):
    request = json.loads(body)

    if method.routing_key == 'monitor':
    #if request['action'] == 'monitor':
        print "    [.] Calling do_monitor"
        response = get_status(request['url'],request['token'])

        ch.basic_publish(exchange='',
                        routing_key=props.reply_to,
                        properties=pika.BasicProperties(correlation_id = \
                                                        props.correlation_id,
                                                        reply_to = 'talkback'),
                        body=str(response))
        ch.basic_ack(delivery_tag = method.delivery_tag)
        return
def on_post_config_request(ch,method,props,body):
    pass

if __name__ =='__main__':

    queues = {'build','monitor','post-config','talkback'}
    exchange_queues = {'build','monitor','post-config'}
        
    # open the connection, build the channel, and declare the queue

    credentials = pika.PlainCredentials('inception', 'pass*word')
    parameters = pika.ConnectionParameters('app.chris.breu.org',
                                           5672,
                                           '/inception',
                                           credentials)

    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()
    channel.exchange_declare(exchange='inception', type='direct')
    
        # declare the queue,
        # bind to the cloud_worker exchange,
    
    for what_queue in queues:
        channel.queue_declare(queue=what_queue)
    
    for what_queue in exchange_queues:
        channel.queue_bind(exchange='inception',
                            routing_key=what_queue,
                            queue=what_queue)
        
    #channel.queue_delcare(queue='callback')
          
    if channel.is_open:
        channel.basic_consume(on_build_request, queue='build')
        channel.basic_consume(on_monitor_request, queue='monitor')
        channel.basic_consume(on_post_config_request, queue='post-config')

try:
    print "[x] Awaiting Requests"
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
connection.close()

