#!/usr/bin/python

import requests

def getCounter(groupName, counterName, host = 'localhost'):
    # get job list       
    getJobs = 'http://%s:19888/ws/v1/history/mapreduce/jobs' %host
    jobs = requests.get(getJobs).json()['jobs']['job'] 
    # get counters
    ts = max([job['finishTime'] for job in jobs])
    id = [job['id'] for job in jobs if job['finishTime'] == ts][0]
    getCounters = 'http://%s:19888/ws/v1/history/mapreduce/jobs/%s/counters' %(host, id)
    counterGroups = requests.get(getCounters).json()['jobCounters']['counterGroup']
    # loop through to counters to return value
    counters = [g['counter'] for g in counterGroups if g['counterGroupName']==groupName][0]
    totalValues = [c['totalCounterValue'] for c in counters if c['name']==counterName]
    return totalValues[0] if len(totalValues)==1 else None

def getCounters(groupName, host = 'localhost'):
    # get job list       
    getJobs = 'http://%s:19888/ws/v1/history/mapreduce/jobs' %host
    jobs = requests.get(getJobs).json()['jobs']['job'] 
    # get counters
    ts = max([job['finishTime'] for job in jobs])
    id = [job['id'] for job in jobs if job['finishTime'] == ts][0]
    getCounters = 'http://%s:19888/ws/v1/history/mapreduce/jobs/%s/counters' %(host, id)
    counterGroups = requests.get(getCounters).json()['jobCounters']['counterGroup']
    # loop through to counters to return value
    counters = [g['counter'] for g in counterGroups if g['counterGroupName']==groupName]    
    return {c['name']:c['totalCounterValue'] for c in counters[0]} if len(counters)==1 else []
    