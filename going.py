from __future__ import print_function

execfile("rp_import.py")

def going_handler(event):
    key = unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    bucket = unquote_plus(event['Records'][0]['s3']['bucket']['name'].encode('utf8'))
    
    print ("Parsing " + bucket + ":" + key)
    payload = {"eventType": "ParseRPGoingFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Starting " + key }
    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
    newsString = None
    

    try:
#    if (1==1):
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        # Store XML in dictionary
        races=parse(body);
        # print(newspapers)
        if (type(races["Going"]["Meeting"]) not in (tuple, list)):
            races["Going"]["Meeting"] = [ races["Going"]["Meeting"] ]
        for meeting in races["Going"]["Meeting"]:
            if (type(meeting["Race"]) not in (tuple, list)):
                meeting["Race"] = [ meeting["Race"]]
            for race in meeting["Race"]:
                try:
                    response = requests.get(esURL+"rp_race/race/"+race["@raceID"], headers=esHeaders)
                    if (response.ok == False or response.json()["found"] == False):
                        print ('Race Not Found: '+race["@uid"])
                        payload = {"eventType": "ParseRPGoingFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Race not found " + race["@uid"] }
                        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                        continue
                    print ("Updating race "+race["@raceID"])
                    raceInfo=response.json()["_source"]
                    raceInfo["going"]=race["@going"]
                    raceInfo["goingCode"] = race["@going_code"]
                    requests.post(esURL+"rp_race/race/"+race["@raceID"], headers=esHeaders, json = raceInfo)
            
                except Exception, e:
                    payload = {"eventType": "ParseRPGoingFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Exception" + str(e) }
                    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                    print(e)
     
        newkey=key.replace(environ['S3_TO_PROCESS_DIR'],environ['S3_PROCESSED_DIR'])
        try:
            s3.copy_object(
                ACL='private',
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': key},
                Key=newkey
              )
            
            s3.delete_object(Bucket=bucket, Key=key)
        except:
            print ("Failed to copy")
        payload = {"eventType": "ParseRPGoingFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Finished" + key }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
        
    except Exception, e:
#    else:
        print (e)
        payload = {"eventType": "ParseRPGoingFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Exception" + str(e) }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
