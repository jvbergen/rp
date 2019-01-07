from __future__ import print_function

execfile("rp_import.py")

def non_runner_handler(event):
    
    key = unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    bucket = unquote_plus(event['Records'][0]['s3']['bucket']['name'].encode('utf8'))
    
    print ("Parsing " + bucket + ":" + key)
    payload = {"eventType": "ParseRPNonRunnerFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Starting " + key }
    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
    newsString = None

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        # Store XML in dictionary
        nonRunners=parse(body);
        # print(newspapers)
        if (not "Meeting" in nonRunners["NonRunners"]):
            # No non runners in races
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
            return
        if (type(nonRunners["NonRunners"]["Meeting"]) not in (tuple, list)):
            # Only a single meeting, turn it into an array
            nonRunners["NonRunners"]["Meeting"] = [ nonRunners["NonRunners"]["Meeting"]]
        for meeting in nonRunners["NonRunners"]["Meeting"]:
            if (type(meeting["Race"]) not in (tuple, list)):
                # Only a single race with non runners in the meeting, turn it into an array
                meeting["Race"] = [ meeting["Race"]]
            for race in meeting["Race"]:
                response = requests.get(esURL+"rp_race/race/"+race["@race_uid"], headers=esHeaders)
                if (response.ok == False or response.json()["found"] == False):
                    print ('Race Not Found: '+race["@race_uid"])
                    payload = {"eventType": "ParseRPNonRunnerFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Race not found " + race["@race_uid"] }
                    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                    continue
                raceInfo=response.json()["_source"]
                if (type(race["Runner"]) not in (tuple,list)):
                    # Only a single non runner in race, turn ir into an array
                    race["Runner"] = [ race["Runner"]]
                for runner in race["Runner"]:
                    offset = raceInfo["horseMap"][runner["@saddle_no"]]
                    raceInfo["horses"][offset]["nonRunner"]=True
                
                requests.post(esURL+"rp_race/race/"+race["@race_uid"], headers=esHeaders, json = raceInfo)   
     
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
        payload = {"eventType": "ParseRPNonRunnerFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Finished " + key }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
        
    except Exception, e:
        print (e)
        payload = {"eventType": "ParseRPNonRunnerFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Exception " + str(e) }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)