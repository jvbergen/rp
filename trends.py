from __future__ import print_function

execfile("rp_import.py")

def trends_handler(event):
    
    key = unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    bucket = unquote_plus(event['Records'][0]['s3']['bucket']['name'].encode('utf8'))
    
    print ("Parsing " + bucket + ":" + key)
    payload = {"eventType": "ParseRPTrendsFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Starting " + key }
    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
    newsString = None
    
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        # Store XML in dictionary
        trends=parse(body);
        # print(newspapers)
        if (not "Race" in trends["Trends"]):
            # No trends 
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
        if (type(trends["Trends"]["Race"]) not in (tuple, list)):
            # only one race, not a list
            trends["Trends"]["Race"] = [ trends["Trends"]["Race"]]
        for race in trends["Trends"]["Race"]:
            if (not "Trend" in race):
                # No trend info for this race, skip
                continue;
                
            if (type(race["Trend"]) not in (tuple, list)):
                # Only one trend data element, not a list
                race["Trend"] = [ race["Trend"]]
            # Load race data
            response = requests.get(esURL+"rp_race/race/"+race["@uid"], headers=esHeaders)
            if (response.ok == False or response.json()["found"] == False):
                print ('Race Not Found: '+race["@uid"])
                payload = {"eventType": "ParseRPTrendsFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Race not found " + race["@uid"] }
                requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                continue
            raceInfo=response.json()["_source"]
            raceInfo["trends"] = []
            for trend in race["Trend"]:
                info={ "year": None, 
                        "weightLbs": None,
                        "trainer" : None,
                        "jockey" : None,
                        "rpr" : None,
                        "draw" : None,
                        "age" : None,
                        "winner" : None,
                        "sp" : None,
                        "topspeed" : None }
                if "@year" in trend:
                    info["year"] = int(trend["@year"])
                if "@weight_lbs" in trend:
                    info["weightLbs"] = int(trend["@weight_lbs"])
                if "@trainer" in trend:
                    info["trainer"] = trend["@trainer"]
                if "@jockey" in trend:
                    info["jockey"] = trend["@jockey"]
                if "@rpr" in trend:
                    info["rpr"] = int(trend["@rpr"])
                if "@draw" in trend:
                    info["draw"] = int(trend["@draw"])
                if "@year" in trend:
                    info["year"] = int(trend["@year"])
                if "@age" in trend:
                    info["age"] = int(trend["@age"])
                if "@winner" in trend:
                    info["winner"] = trend["@winner"]
                if "@sp" in trend:
                    info["sp"] = trend["@sp"]
                if "@topspeed" in trend:
                    info["topspeed"] = int(trend["@topspeed"])
                    
                raceInfo["trends"].append(info)
                requests.post(esURL+"rp_race/race/"+race["@uid"], headers=esHeaders, json = raceInfo)   
     
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
        payload = {"eventType": "ParseRPTrendsFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Finished " + key }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
        
    except Exception, e:
        print (e)
        payload = {"eventType": "ParseRPTrendsFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Exception " + str(e) }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
