from __future__ import print_function

execfile("rp_import.py")

def predictor_dog_handler(event):
    
    key = unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    bucket = unquote_plus(event['Records'][0]['s3']['bucket']['name'].encode('utf8'))
    
    payload = {"eventType": "ParseRPPredictorDogFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Starting " + key }
    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
    newsString = None
    
#    try:
    if True:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        # Store XML in dictionary
        predictor=parse(body);
        if (not "Meeting"  in predictor["Declaration"]):
            # No meetings
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
        if (type(predictor["Declaration"]["Meeting"]) not in (tuple, list)):
            # Only one meeting, not in an array
            predictor["Declaration"]["Meeting"] = [ predictor["Declaration"]["Meeting"]]
        for meeting in predictor["Declaration"]["Meeting"]:
            if (type(meeting["Race"]) not in (tuple, list)):
                meeting["Race"] = [ meeting["Race"]]
            for race in meeting["Race"]:
                print ("Race: " + race["@id"])
                response = requests.get(esURL+"rp_dogs/race/"+race["@id"], headers=esHeaders)
                if (response.ok == False or response.json()["found"] == False):
                    print ('Race Not Found: '+race["@id"])
                    print (response)
                    print (response.text)
                    payload = {"eventType": "ParseRPPredictorDogFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Race not found " + race["@id"] }
                    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                    continue
                raceInfo=response.json()["_source"]
                if "Runner" not in race:
                    #No runner data from RP
                    continue
                for runner in race["Runner"]:
                    predictor = runner.get("@prediction","0")
                    if (predictor == "") :
                        predictor = "0"
                    raceInfo["runners"][int(runner["@trap"])-1]["predictor"] = float(predictor)
                    
                requests.post(esURL+"rp_dogs/race/"+race["@id"], headers=esHeaders, json = raceInfo)   
     
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
        payload = {"eventType": "ParseRPPredictorDogFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Finished " + key }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
    else :       
#    except Exception, e:
        print (e)
        payload = {"eventType": "ParseRPPredictorDogFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Exception " + str(e) }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)