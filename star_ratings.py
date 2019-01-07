from __future__ import print_function

execfile("rp_import.py")

def star_ratings_handler(event ):
    
    key = unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    bucket = unquote_plus(event['Records'][0]['s3']['bucket']['name'].encode('utf8'))
    
    payload = {"eventType": "ParseRPStarFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Starting " + key }
    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
    newsString = None
    
    try:
#    if (1==1):
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        # Store XML in dictionary
        races=parse(body);

        for horse in races["StarRating"]["Horse"]:
            h = requests.get(esURL+"rp_horse/horse/"+str(horse["@horse_id"]), headers=esHeaders)
            if (h.ok and h.json()["found"]==True):
                horseJSON=h.json()["_source"]
                horseJSON["starRating"]=horse["@star_rating"]
                raceJSON = requests.get(esURL+"rp_race/race/"+str(horseJSON["lastRace"]["raceId"]), headers=esHeaders).json()["_source"] 
                raceJSON["horses"][horseJSON["lastRace"]["offset"]]["starRating"]=horse["@star_rating"]
                r = requests.put(esURL+"rp_race/race/"+str(horseJSON["lastRace"]["raceId"]), headers=esHeaders, json=raceJSON)
                if (not r.ok):
                    payload = {"eventType": "ParseRPStarFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Failed to save race " + str(horseJSON["lastRace"]["raceId"]) }
                    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                    print (r.text)
                r = requests.put(esURL+"rp_horse/horse/"+str(horse["@horse_id"]), headers=esHeaders, json=horseJSON)
                if (not r.ok):
                    payload = {"eventType": "ParseRPStarFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Failed to save horse " + str(horse["@horse_id"]) }
                    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                    print (r.text)
        newkey=key.replace(environ['S3_TO_PROCESS_DIR'],environ['S3_PROCESSED_DIR'])
        try:
            s3.copy_object(
                ACL='private',
                CopySource={'Bucket': bucket, 'Key': key},
                Bucket=bucket,
                Key=newkey
              )
            
            s3.delete_object(Bucket=bucket, Key=key)
        except:
            print ("Failed to copy")
        payload = {"eventType": "ParseRPStarFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Finished" + key }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
        
    except Exception, e:
#    else:
        print (e)
        payload = {"eventType": "ParseRPStarFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Exception" + str(e) }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)