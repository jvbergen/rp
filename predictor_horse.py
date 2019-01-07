from __future__ import print_function

execfile("rp_import.py")

def predictor_horse_handler(event):
    
    key = unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    bucket = unquote_plus(event['Records'][0]['s3']['bucket']['name'].encode('utf8'))
    
    payload = {"eventType": "ParseRPPredictorFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Starting " + key }
    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
    newsString = None
    
    try:
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
                response = requests.get(esURL+"rp_race/race/"+race["@raceid"], headers=esHeaders)
                if (response.ok == False or response.json()["found"] == False):
                    print ('Race Not Found: '+race["@raceid"])
                    payload = {"eventType": "ParseRPPredictorFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Race not found " + race["@raceid"] }
                    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                    continue
                raceInfo=response.json()["_source"]
                if "horseMap" not in raceInfo:
                    #Mapping data not saved
                    continue
                if "Runner" not in race:
                    #No runner data from RP
                    continue
                for runner in race["Runner"]:
                    offset = raceInfo["horseMap"][runner["@saddle_no"]]
                    raceInfo["horses"][offset]["predictor"] = {}
                    raceInfo["horses"][offset]["predictor"]["prediction"]          = float(runner.get("@prediction"))
                    raceInfo["horses"][offset]["predictor"]["abilityPoints"]       = float(runner.get("@ability_points"))
                    raceInfo["horses"][offset]["predictor"]["recentFormPoints"]    = float(runner.get("@recent_form_points"))
                    raceInfo["horses"][offset]["predictor"]["trainerFormPoints"]   = float(runner.get("@trainer_form_points"))
                    raceInfo["horses"][offset]["predictor"]["trainerRecordPoints"] = float(runner.get("@trainer_record_points"))
                    raceInfo["horses"][offset]["predictor"]["goingPoints"]         = float(runner.get("@going_points"))
                    raceInfo["horses"][offset]["predictor"]["distancePoints"]      = float(runner.get("@distance_points"))
                    raceInfo["horses"][offset]["predictor"]["coursePoints"]        = float(runner.get("@course_points"))
                    raceInfo["horses"][offset]["predictor"]["drawPoints"]          = float(runner.get("@draw_points"))
                    raceInfo["horses"][offset]["predictor"]["groupPoints"]         = float(runner.get("@group_points"))
                    
                    
                requests.post(esURL+"rp_race/race/"+race["@raceid"], headers=esHeaders, json = raceInfo)   
     
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
        payload = {"eventType": "ParseRPPredictorFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Finished " + key }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
        
    except Exception, e:
        print (e)
        payload = {"eventType": "ParseRPPredictorFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Exception " + str(e) }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)