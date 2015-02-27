#!/usr/bin/env python2.7

"""Load balancer application for OpenCache."""

import json
import random
import urllib
import optparse
import time
import os

import sys
from datetime import datetime
from pprint import pprint

class Node(object):
    """Represents a single nodes capacity and load."""
    def __init__(self, id_, capacity, expr=None, load={}):
	self.id_ = id_
	self.capacity = capacity
	self.expr = expr
	self.load = load

def _load_file(file):
    """Load the JSON configuration file."""
    file_handle = open(file)
    return json.load(file_handle)

def _parse_config(config):
    """Parse the JSON configuration into objects."""
    nodes = {}
    for id_, capacity in config['capacity'].items():
        nodes[id_] = Node(id_=id_, capacity=capacity)
    return nodes

def _get_failover_nodes(config):
    nodes_to_watch = []
    for id_, value in config['failover'].items():
	for item in value:
	    nodes_to_watch.append(item)
    return nodes_to_watch

def _update_load(options, nodes):
    """Update node data from the nodes themselves."""
    for id_, node in nodes.iteritems():
	load = {}
        result = _do_opencache_call('stat', options, id_, '*')['result']
        load['cache_miss'] = int(result['total_cache_miss'])
        load['cache_miss_size'] = int(result['total_cache_miss_size'])
        load['cache_hit'] = int(result['total_cache_hit'])
        load['cache_hit_size'] = int(result['total_cache_hit_size'])
        load['cache_object'] = int(result['total_cache_object'])
        load['cache_object_size'] = int(result['total_cache_object_size'])
        #node.expr = list(result['expr_seen'])
	node.expr = ['192.168.0.7']
	node.load = load

    #for id_, node in nodes.iteritems():
#	pass
#	print 'node %s' % id_
#	pprint(vars(node))

def _check_thresholds(nodes):
    """Check to see which metrics are exceeding their thresholds."""
    overloaded = []
    for node in nodes.values():
        for metric in node.capacity.keys():
            if node.load[metric] > node.capacity[metric]:
                overloaded.append(({'node': node, 'metric': metric}))
    return overloaded

def _find_node_to_move_to_old(nodes, metric, load):
    """
    Find the most appropriate node to move the load to.

    List of nodes is randomised to create a 'round-robin' effect.

    """
    keys = list(nodes.keys())
    random.shuffle(keys)
    nodes = [(key, nodes[key]) for key in keys]
    for _, node in nodes:
	#print 'this nodes ' + metric + ' is ' + str(node.load[metric])
        if node.load[metric] < node.capacity[metric] + int(load):
            return node
    return None, None

def _find_node_to_move_to(nodes, metric, load):
    list_of_nodes = []
    for node in nodes.itervalues():
	list_of_nodes.append(node)
    random.shuffle(list_of_nodes)

    for node in list_of_nodes:
	if int(load) < node.capacity[metric]:
	    return node

    return None, None

def _find_expr_to_move(options, node, metric):
    """ Find the most appropriate expression to move on the given node."""
    load = []
    key = 'total_' + str(metric)
    for expr in node.expr:
        result = _do_opencache_call('stat', options, node.id_, expr)['result']
        load.append({'expr': expr, 'load': int(result[key])})
    load.sort(key=lambda tup: tup['load'])
    to_move = node.load[metric] - node.capacity[metric]
    for item in load:
        if item['load'] > to_move:
            return item['expr'], item['load']
    return None, None

def _ask_for_refresh(options, nodes):
    for id_, node in nodes.items():
        _do_opencache_call('refresh', options, id_)

def _do_opencache_call(method, options, node, expr='192.168.0.7', call_id=None):
    """Make a JSON-RPC call to the OpenCache controller."""
    if call_id is None:
        call_id = random.randint(1, 999)
    params = {'node-id': str(node), 'expr': str(expr)}
    url = "http://%s:%s" % (options.host, options.port)
    try:
        post_data = json.dumps({"id": call_id, "method": str(method),
                               "params": params, "jsonrpc": "2.0"})
    except Exception as exception:
        print "[ERROR] Could not encode JSON: %s" % exception
    try:
        response_data = urllib.urlopen(url, post_data).read()
        print "[INFO] Sent request: %s" % post_data
        try:
            response_json = json.loads(response_data)
            if response_json['id'] == str(call_id):
                print "[INFO] Received response: %s" % response_json
                return response_json
            else:
                print "[ERROR] Mismatched call ID for response: %s" % response_json
                raise IOError("Mismatched call ID for response: %s" % response_json)
        except Exception as exception:
            print "[ERROR] Could not decode JSON from OpenCache node response: %s" % exception
    except IOError as exception:
        print "[ERROR] Could not connect to OpenCache instance: %s" % exception


def _move_expr(options, expr, overloaded, target):
    """Call the OpenCache API to move the content between nodes."""
    print '-' * 25 + ' moving from ' + str(overloaded) + ' to ' + str(target) + ' ' + '-' * 25
    _do_opencache_call('stop', options, overloaded, expr)
    _do_opencache_call('start', options, overloaded, expr)
    _do_opencache_call('stop', options, overloaded, expr)

    _do_opencache_call('start', options, target, expr)
    print str(datetime.now())
    print '-' * 71

def _parse_options():
    """Parse the command line options given."""
    parser = optparse.OptionParser()
    parser.add_option("-i", "--hostname", dest="host", default='127.0.0.1',
                      help="hostname of OpenCache controller")
    parser.add_option("-p", "--port", dest="port", default='49001',
                      help="port number of the OpenCache JSON-RPC interface")
    parser.add_option("-c", "--config", dest="config",
                      help="path of load balancer configuration")
    parser.add_option("-d", "--delay", dest="delay", default=10,
                      help="delay between load balancing operations")
    return parser.parse_args()

if __name__ == '__main__':
    options, _ = _parse_options()
    if options.config:
        config = _load_file(options.config)
    else:
        print "[ERROR] No configuration file given."
        exit()
    nodes = _parse_config(config)
    failover_nodes = _get_failover_nodes(config)
    _do_opencache_call('stat', options, '*', '*')
    print '\n\n\n'
    while True:
	_ask_for_refresh(options, nodes)
        _update_load(options, nodes)
        overloaded = _check_thresholds(nodes)
        for item in overloaded:
            expr_to_move, load = _find_expr_to_move(options, item['node'], item['metric'])
            if not expr_to_move:
                print "[ERROR] No expression found to move from overloaded node."
                break
            node_to_move_to = _find_node_to_move_to(nodes, item['metric'], load)
            if not node_to_move_to:
                print "[ERROR] No node found to move load to."
                break
            _move_expr(options, expr_to_move, item['node'].id_,
                       node_to_move_to.id_)
        time.sleep(float(options.delay))
