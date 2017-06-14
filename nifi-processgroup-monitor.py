import requests
import json
import time

# define variables
url = "http://192.168.5.166:9090"
processgroupname = "Test Group"

# define functions
def log(message):
	print("[" + time.strftime("%c") + "]: " + message)

def GetNifiGroup(groupname):	
	# Get and return nifi groups matching the group name
	groupsearchrequest = requests.get(url + "/nifi-api/flow/search-results?q=" + groupname)
	log("Get Nifi Group HTTP Status Code: " + str(groupsearchrequest.status_code) + "\r")
	#DEBUG log("Get Nifi Group Body: " + groupsearchrequest.text + "\r")
	return groupsearchrequest.text

def GetProcessorList(searchdata):
	# Get a list of processors to be restarted
	groupdata = json.loads(searchdata)
	groupid = groupdata['searchResultsDTO']['processGroupResults'][0]['id']
	groupname = groupdata['searchResultsDTO']['processGroupResults'][0]['name']
	log ("Group ID: " + groupid +  " Group Name: " + groupname + "\r")
	processordata = requests.get(url + "/nifi-api/process-groups/" + groupid + "/processors")
	log("Get Processor Data HTTP Status Code: " + str(processordata.status_code) + "\r")
	#DEBUG log("Get Processor Data Body: " + processordata.text + "\r")
	processors = json.loads(processordata.text)
	processorlist = []
	for processor in processors['processors']:
		log("Processor ID: " + processor['id'] + " Processor Name: " + processor['status']['name'] +"\r")	
		processorlist.append(processor['id'])
	return(processorlist)

def StartProcessor(id, revision):
		requestbody = '{"status":{"runStatus":"RUNNING"},"component":{"state":"RUNNING","id":"' + str(id) + '"},"id":" '+ str(id) +' ","revision":{"version":' + str(revision) + '}}'
		requestheaders = {'Content-Type' : 'application/json'}
		restartrequest = requests.put(url + "/nifi-api/processors/" + id, data=requestbody, headers=requestheaders)
		log("Restart request for " + id + " Status: " + str(restartrequest.status_code))
			
def RestartProcessors(list):
	# Restart all processors in the array if they're stopped
	for processor in list:
		processjson = requests.get(url + "/nifi-api/processors/" + processor)
		log("Get Processor HTTP Status Code: " + str(processjson.status_code) + "\r")
		#DEBUG log("Get Processor Body: " + processjson.text + "\r")
		processordata = json.loads(processjson.text)
		version = processordata['revision']['version']
		log("Processor version is: " + str(version))
		if processordata['component']['state'] != "RUNNING":
			log(processor + " " + processordata['component']['name'] + " is stopped, starting...")
			StartProcessor(processor, version)
		else:
			log(processor + " is started, doing nothing...")

# main script	
print("#############################################################################")
log("                               Starting...")
groupoutput = GetNifiGroup(processgroupname)
processlist = GetProcessorList(groupoutput)
RestartProcessors(processlist)