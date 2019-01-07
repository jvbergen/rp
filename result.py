from __future__ import print_function

execfile("rp_import.py")

def result_handler(event):
    
    key = unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    bucket = unquote_plus(event['Records'][0]['s3']['bucket']['name'].encode('utf8'))
    
    print ("Parsing " + bucket + ":" + key)
    payload = {"eventType": "ParseRPResultsFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Starting " + key }
    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
    newsString = None
    
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        # Store XML in dictionary
        results=parse(body);
        # print(newspapers)
        if (not "Meeting"  in results["RPResults"]):
            # No resulted races
            requests.delete(esURL+"rp_lock/lock/lock", headers=esHeaders)
            newkey=key.replace(environ['S3_TO_PROCESS_DIR'],environ['S3_PROCESSED_DIR'])
            try:
                s3.copy_object(
                    ACL='public-read',
                    Bucket=bucket,
                    CopySource={'Bucket': bucket, 'Key': key},
                    Key=newkey
                )
                s3.delete_object(Bucket=bucket, Key=key)
            except:
                print ("Failed to copy")
            return
        if (type(results["RPResults"]["Meeting"]) not in (tuple, list)):
            results["RPResults"]["Meeting"] = [ results["RPResults"]["Meeting"]]
        for meeting in results["RPResults"]["Meeting"]:
            if (type(meeting["Race"]) not in (tuple, list)):
                meeting["Race"] = [ meeting["Race"]]
            for race in meeting["Race"]:
                response = requests.get(esURL+"rp_race/race/"+race["@uid"], headers=esHeaders)
                if (response.ok == False or response.json()["found"] == False):
                    print ('Race Not Found: '+race["@uid"])
                    payload = {"eventType": "ParseRPResultsFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Race not found " + race["@uid"] }
                    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                    continue
                raceInfo=response.json()["_source"]
                results={}
                raceInfo["hasResults"] = True
                results["winSecs"]=race.get("@win_secs")
                results["offtime"]=race.get("@offtime").replace("-",":")
                results["tote"]=race.get("Tote")
                results["toteSwinger"]=race.get("ToteSwinger")
                results["abandonReason"]=race.get("AbandonReason")
                results["longHandicap"]=race.get("LongHandicap")
                results["nonRunners"]=race.get("NonRunners")
                results["raceComments"]=race.get("RaceComments")
                results["runners"] = []
                if "Runner" not in race:
                    continue
                if (type(race["Runner"]) not in (tuple, list)):
                    race["Runner"] = [ race["Runner"] ]
                for runner in race["Runner"]:
                    horse = {}
                    horse["position"] = runner.get("@position")
                    if ("@horse_uid" in runner):
                        horse["rpHorseId"] = int(runner["@horse_uid"])
                    horse["horseName"]=runner.get("@horsename")
                    horse["jockeyName"]=runner.get("@jockeyname")
                    horse["saddle"]=runner.get("@saddle")
                    horse["distanceHif"]=runner.get("@distance_hif")
                    horse["distanceToWinner"]=runner.get("@distance_to_winner")
                    horse["comment"]=runner.get("Commentinrun")
                    results["runners"].append(horse)
                raceInfo["results"] = results
                requests.post(esURL+"rp_race/race/"+race["@uid"], headers=esHeaders, json = raceInfo)   
     
        # release lock
        requests.delete(esURL+"rp_lock/lock/lock", headers=esHeaders)
        newkey=key.replace(environ['S3_TO_PROCESS_DIR'],environ['S3_PROCESSED_DIR'])
        try:
            s3.copy_object(
                ACL='public-read',
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': key},
                Key=newkey
              )
            
            s3.delete_object(Bucket=bucket, Key=key)
        except:
            print ("Failed to copy")
        payload = {"eventType": "ParseRPResultsFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Finished " + key }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
        
    except Exception, e:
        print (e)
        # release lock
        payload = {"eventType": "ParseRPResultsFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Exception " + str(e) }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
        requests.delete(esURL+"rp_lock/lock/lock", headers=esHeaders)
