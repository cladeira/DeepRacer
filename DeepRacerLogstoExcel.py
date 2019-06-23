#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import boto3
import sys
import dateutil.parser


# In[2]:


'''
Get a particular log given stream ID
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs.html#CloudWatchLogs.Client.filter_log_events
'''

def get_log_events(log_group, stream_name=None, stream_prefix=None, start_time=None, end_time=None):
    #client = boto3.client('logs')
    client = boto3.client(
    'logs',
    aws_access_key_id='xxxxxxxx', #Enter the access key here
    aws_secret_access_key='xxxxxxxx', #Enter the secret key here
    region_name='us-east-1')
    
    if stream_name is None and stream_prefix is None:
        print("both stream name and prefix can't be None")
        return

    kwargs = {
        'logGroupName': log_group,
        'logStreamNames': [stream_name],
        'limit': 10000,
    }

    if stream_prefix:
        kwargs = {
            'logGroupName': log_group,
            'logStreamNamePrefix': stream_prefix,
            'limit': 10000,
        }

    kwargs['startTime'] = start_time
    kwargs['endTime'] = end_time

    while True:
        resp = client.filter_log_events(**kwargs)
        yield from resp['events']
        try:
            kwargs['nextToken'] = resp['nextToken']
        except KeyError:
            break


def download_log(fname, stream_name=None, stream_prefix=None,
                 log_group=None, start_time=None, end_time=None):
    if start_time is None:
        start_time = 1451490400000  # 2018
    if end_time is None:
        end_time = 2000000000000  # 2033 #arbitrary future date
    if log_group is None:
        log_group = "/aws/robomaker/SimulationJobs"

    with open(fname, 'w') as f:
        logs = get_log_events(
            log_group=log_group,
            stream_name=stream_name,
            stream_prefix=stream_prefix,
            start_time=start_time,
            end_time=end_time
        )
        for event in logs:
            f.write(event['message'].rstrip())
            f.write("\n")


def download_all_logs(pathprefix, log_group, not_older_than=None, older_than=None):
    client = boto3.client('logs')

    lower_timestamp = iso_to_timestamp(not_older_than)
    upper_timestamp = iso_to_timestamp(older_than)

    fetched_files = []
    next_token = None

    while next_token is not 'theEnd':
        streams = describe_log_streams(client, log_group, next_token)

        next_token = streams.get('nextToken', 'theEnd')

        for stream in streams['logStreams']:
            if lower_timestamp and stream['lastEventTimestamp'] < lower_timestamp:
                return fetched_files  # we're done, next logs will be even older
            if upper_timestamp and stream['firstEventTimestamp'] > upper_timestamp:
                continue
            stream_prefix = stream['logStreamName'].split("/")[0]
            file_name = "%s%s.log" % (pathprefix, stream_prefix)
            download_log(file_name, stream_prefix=stream_prefix, log_group=log_group)
            fetched_files.append(
                (file_name, stream_prefix, stream['firstEventTimestamp'], stream['lastEventTimestamp']))

    return fetched_files


def describe_log_streams(client, log_group, next_token):
    if next_token:
        streams = client.describe_log_streams(logGroupName=log_group, orderBy='LastEventTime',
                                              descending=True, nextToken=next_token)
    else:
        streams = client.describe_log_streams(logGroupName=log_group, orderBy='LastEventTime',
                                              descending=True)
    return streams


def iso_to_timestamp(iso_date):
    return dateutil.parser.parse(iso_date).timestamp() * 1000 if iso_date else None


# In[3]:


'''
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''

import numpy as np
import pandas as pd
import gzip
import glob
import math
from datetime import datetime


EPISODE_PER_ITER = 20

def load_data(fname):
    data = []
    with open(fname, 'r') as f:
        for line in f.readlines():
            if "SIM_TRACE_LOG" in line:
                parts = line.split("SIM_TRACE_LOG:")[1].split('\t')[0].split(",")
                data.append(",".join(parts))
    return data

def convert_to_pandas(data, wpts=None):

    """
    stdout_ = 'SIM_TRACE_LOG:%d,%d,%.4f,%.4f,%.4f,%.2f,%.2f,%d,%.4f,%s,%s,%.4f,%d,%.2f,%s\n' % (
            self.episodes, self.steps, model_location[0], model_location[1], model_heading,
            self.steering_angle,
            self.speed,
            self.action_taken,
            self.reward,
            self.done,
            all_wheels_on_track,
            current_progress,
            closest_waypoint_index,
            self.track_length,
            time.time())
        print(stdout_)
    """        

    df_list = list()
    
    #ignore the first two dummy values that coach throws at the start.
    for d in data[2:]:
        parts = d.rstrip().split(",")
        episode = int(parts[0])
        steps = int(parts[1])
        x = 100*float(parts[2])
        y = 100*float(parts[3])
        ##cWp = get_closest_waypoint(x, y, wpts)
        yaw = float(parts[4])
        steer = float(parts[5])
        throttle = float(parts[6])
        action = float(parts[7])
        reward = float(parts[8])
        done = 0 if 'False' in parts[9] else 1
        all_wheels_on_track = parts[10]
        progress = float(parts[11])
        closest_waypoint = int(parts[12])
        track_len = float(parts[13])
        tstamp = parts[14]
        
        #desired_action = int(parts[10])
        #on_track = 0 if 'False' in parts[12] else 1
        
        iteration = int(episode / EPISODE_PER_ITER) +1
        df_list.append((iteration, episode, steps, x, y, yaw, steer, throttle, action, reward, done, all_wheels_on_track, progress,
                        closest_waypoint, track_len, tstamp))

    header = ['iteration', 'episode', 'steps', 'x', 'y', 'yaw', 'steer', 'throttle', 'action', 'reward', 'done', 'on_track', 'progress', 'closest_waypoint', 'track_len', 'timestamp']
    
    df = pd.DataFrame(df_list, columns=header)
    return df

def episode_parser(data, action_map=True, episode_map=True):
    '''
    Arrange data per episode
    '''
    action_map = {} # Action => [x,y,reward] 
    episode_map = {} # Episode number => [x,y,action,reward] 

 
    for d in data[:]:
        parts = d.rstrip().split("SIM_TRACE_LOG:")[-1].split(",")
        e = int(parts[0])
        x = float(parts[2]) 
        y = float(parts[3])
        angle = float(parts[5])
        ttl = float(parts[6])
        action = int(parts[7])
        reward = float(parts[8])

        try:
            episode_map[e]
        except KeyError:
            episode_map[e] = np.array([0,0,0,0,0,0]) #dummy
        episode_map[e] = np.vstack((episode_map[e], np.array([x,y,action,reward,angle,ttl])))

        try:
            action_map[action]
        except KeyError:
            action_map[action] = []
        action_map[action].append([x, y, reward])
                
    # top laps
    total_rewards = {}
    for x in episode_map.keys():
        arr = episode_map[x]
        total_rewards[x] = np.sum(arr[:,3])

    import operator
    top_idx = dict(sorted(total_rewards.items(), key=operator.itemgetter(1), reverse=True)[:])
    sorted_idx = list(top_idx.keys())

    return action_map, episode_map, sorted_idx


# In[4]:


stream_name = 'sim-d8s7z5nm54t6'
fname = 'logs/deepracer-%s.log' %stream_name
download_log(fname, stream_prefix=stream_name)
data = load_data(fname)
df = convert_to_pandas(data, None)
export = 'logs/export-%s.xlsx' %stream_name
df.to_excel(export)


# In[ ]:




