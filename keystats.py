from __future__ import print_function

execfile("rp_import.py")

def keystats_handler(event):
    
    key = unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    bucket = unquote_plus(event['Records'][0]['s3']['bucket']['name'].encode('utf8'))
    
    payload = {"eventType": "ParseRPKeystatFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Starting " + key }
    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
    newsString = None
    
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        # Store XML in dictionary
        keyStats=parse(body);
        # print(newspapers)
        if (not "Meeting"  in keyStats["Keystat"]):
            # No resulted races
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
        if (type(keyStats["Keystat"]["Meeting"]) not in (tuple, list)):
            keyStats["Keystat"]["Meeting"] = [ keyStats["KeyStat"]["Meeting"]]
        for meeting in keyStats["Keystat"]["Meeting"]:
            if (type(meeting["Race"]) not in (tuple, list)):
                meeting["Race"] = [ meeting["Race"]]
            for race in meeting["Race"]:
                if "Keystat" not in race:
                    continue
                response = requests.get(esURL+"rp_race/race/"+race["@uid"], headers=esHeaders)
                if (response.ok == False or response.json()["found"] == False):
                    print ('Race Not Found: '+race["@uid"])
                    payload = {"eventType": "ParseRPKeystatFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Race not found " + race["@uid"] }
                    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                    continue
                raceInfo=response.json()["_source"] 
                raceInfo["keyStats"] = { "runner" : race["Keystat"]["@runner"], 
                    "KSweight" : race["Keystat"]["@KSweight"],
                    "comment": race["Keystat"]["@comment"]}
                requests.post(esURL+"rp_race/race/"+race["@uid"], headers=esHeaders, json = raceInfo)   
     
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
        payload = {"eventType": "ParseRPKeystatFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Finished " + key }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
        
    except Exception, e:
        print (e)
        payload = {"eventType": "ParseRPKeystatFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Exception " + str(e) }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
       