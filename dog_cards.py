from __future__ import print_function

execfile("rp_import.py")
execfile("event_match.py")

ladDigitalMap = []
coralDigitalMap = []
coralRetailMap = []

def dog_cards_handler(event):
    key = unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    bucket = unquote_plus(event['Records'][0]['s3']['bucket']['name'].encode('utf8'))
    print ("Parsing " + key)
    
    payload = {"eventType": "ParseRPDogCardFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Starting parsing " + key }
    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
    
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj['Body'].read()
    declaration = parse(body)
    
    if ("Meeting" not in declaration["Declaration_Dogs"]):
        # Empty file, nothing to do
        print ("empty file" + dumps(declaration))
        Finish(bucket,key);
        return 0
    
    needRetry = False
            
    print ("Parsing " + bucket+":"+key)
  
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('Europe/London')
    
    # if only a single meeting (no array), create an array with a single element
    if (type(declaration["Declaration_Dogs"]["Meeting"] ) not in (tuple, list)):
        declaration["Declaration_Dogs"]["Meeting"]=[ declaration["Declaration_Dogs"]["Meeting"] ]
    # Step through the meetings in the declaration file
    for meeting in declaration["Declaration_Dogs"]["Meeting"] :
        courseId = meeting["@track_uid"]
        meetingDate = meeting["@date"][0:10]
        abandoned =  meeting.get("@abandoned")
        print ("Parsing " + meeting["@track"] + " (" +str(courseId) + ")")
        
        ladDigitalMap = []
        coralDigitalMap = []
        coralRetailMap = []

        # Find the possible event type IDs for the OB instances
        response = requests.get(esURL+"rp_mapping/rp_mapping/dogs"+courseId, headers=esHeaders)
        if (response.ok ) :
            responseJSON=response.json()
            if (responseJSON["found"] == True) :
                ladDigitalMap   = responseJSON["_source"]["LadDigital"]
                coralDigitalMap = responseJSON["_source"]["CoralDigital"]
                coralRetailMap  = responseJSON["_source"]["CoralRetail"]
        if (len(ladDigitalMap) == 0 and len(coralDigitalMap) and len (coralRetailMap)):
            payload = {"eventType": "ParseRPDogCardFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Mapping not found for RP track uid" + str(courseid)}
            requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
            print ("mapping not found for " + courseId)
        
        # if only a single meeting (no array), create an array with a single element
        if (type(meeting["Race"]) not in (tuple, list)):
            meeting["Race"]=[ meeting["Race"] ]
        # Step through the races in the racing post meeting
        
        for race in meeting["Race"] :
            
            # try to find the mapping for Coral Digital
            # get RP start time, this is a 12 hour time with a '-' between hours and minuts
            startTime = race["@time"]
            if (startTime[1:2] == ':') :
                # time functions have a leading 0 for times <10. Add to match
                startTime = "0" + startTime
            print (meeting["@track"] + " race " + startTime)
            hour = int(startTime[0:2])+12
            minute = startTime[3:]
            startTime2 = str(hour) + ":" + minute
            raceId = race["@race_uid"]
            contents = ""
            obStartTime = ""

# Find the Coral Digital event IDs
            coralDigitalId, obStartTime = match_event (coralDigitalMap, environ['CD_SPORTS_API_URL'], environ['CD_SPORTS_API_KEY'],
                meetingDate, startTime,startTime2)
             # We checked the event type(s) now see if we fount the event ID
            if (len(coralDigitalId) == 0) :
                payload = {"eventType": "ParseRPDogCardFile", "fileTitle": key, "errorExists": "true", 
                        "errorMessage": "Failed to find Coral digital event id for rp race " + 
                          " ("+ raceId + ")" }
                requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                print ("Event ID not found in Coral Digital OB")
                print (coralDigitalMap)
                needRetry = True
# END OF CORAL DIGITAL


# Find the Coral Retail event IDs
            coralRetailId, obStartTime2 = match_event (coralRetailMap, environ['CR_SPORTS_API_URL'], environ['CR_SPORTS_API_KEY'],
                meetingDate, startTime,startTime2)
            if obStartTime == "" and obStartTime2 != "":
                obStartTime=obStartTime2
                
             # We checked the event type(s) now see if we fount the event ID
            if (len(coralRetailId) == 0) :
                payload = {"eventType": "ParseRPDogCardFile", "fileTitle": key, "errorExists": "true", 
                        "errorMessage": "Failed to find Coral retail event id for rp race " + 
                        " ("+ raceId + ")" }
                requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                print ("Event ID not found in Coral Retail OB")
                print (coralRetailMap)
                needRetry = True
# END OF CORAL RETAIL

# Find the Ladbrokes Digital event IDs
            ladbrokesDigitalId, obStartTime2 = match_event (ladDigitalMap, environ['LD_SPORTS_API_URL'], environ['LD_SPORTS_API_KEY'],
                meetingDate, startTime,startTime2)
            if obStartTime == "" and obStartTime2 != "":
                obStartTime=obStartTime2    
            
             # We checked the event type(s) now see if we fount the event ID
            if (len(ladbrokesDigitalId) == 0) :
                payload = {"eventType": "ParseRPDogCardFile", "fileTitle": key, 
                        "errorExists": "true", "errorMessage": "Failed to find Ladbrokes digital event id for rp race "  + 
                        " ("+ raceId + ")" }
                requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                print ("Event ID not found in Ladbrokes Digital OB")
                print (ladDigitalMap)
                needRetry = True
# END OF LADBROKES DIGITAL
        
            try:
                raceJSON = requests.get(esURL+"rp_dogs/race/"+raceId, headers=esHeaders).json()["_source"]
            except:
                raceJSON = {}  
            try:
#           if (1==1):
                if (len(coralDigitalId) != 0):
                    raceJSON["coralDigitalEventId"] = coralDigitalId
                if (len(coralRetailId) != 0):
                    raceJSON["coralRetailEventId"] = coralRetailId
                if (len(ladbrokesDigitalId) != 0):
                    raceJSON["ladbrokesDigitalEventId"] = ladbrokesDigitalId
                if obStartTime != "" :
                    raceJSON["obStartTime"] = obStartTime 
                raceJSON["rpRaceId"] = int(raceId)
                raceJSON["rpTrackId"] = int(courseId)
                raceJSON["trackName"] = meeting.get("@track") 
                raceJSON["raceNo"] = int(race.get("@race_number"))
                raceJSON["time"] = startTime
                raceJSON["Comment"] = race.get("Comment")
                raceJSON["grade"] = race.get("@grade")
                raceJSON["prize"] = race.get("@prize")
                raceJSON["distance"] = race.get("@distance_meters")
                raceJSON["bags"] = race.get("@bags")
                raceJSON["postPick"] = race.get("@post_pick")
                raceJSON["raceType"] = race.get("@race_type")
                print ("OB 1: " + str(coralDigitalId) + " OB 2: " + str(coralRetailId) + " OB 3: " + str(ladbrokesDigitalId))
            except Exception as e:
#    else:
                print ("Got exception in race " + raceId)
                print(e)
                payload = {"eventType": "ParseRPDogCardFile", "fileTitle": key, 
                    "errorExists": "true", 
                    "errorMessage": "Exception in parsing race if " + raceId + dumps(str(e))}
                requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                
            raceJSON["runners"] = []
           
            # Assuming more than 1 dog in a race so no check for it being an array
            for runner in race["Runner"]:
                if (runner.get("@name") == "VACANT"):
                    raceJSON["runners"].append({"rpDogId" : 0, "dogName" : "VACANT"})
                    continue
                dog = {}
                try: 
 #               if (1==1):
                    dog["rpDogId"] = int(runner["@dog_uid"])
                    dog["dogName"] = runner.get("@name")
                    dog["trainerName"] = runner.get("@trainer_name")
                    
                    if "@bestrecenttime" in runner and runner.get("@bestrecenttime") != "":
                        dog["bestRecentTime"] = float(runner.get("@bestrecenttime"))
                    
                    dog["trap"] = runner.get("@trap")
                    if "@rating" in runner and runner["@rating"] != "":
                        dog["rating"] = int(runner["@rating"])
                    dog["last5Runs"] = runner.get("@last5runs")
                    if "@last_run_time" in runner and runner.get("@last_run_time") != "":
                        dog["lastRunTime"] = float(runner["@last_run_time"])
                    dog["dogcolour"] = runner.get("@colour")
                    dog["dogSex"] = runner.get("@sex")
                    dog["damName"] = runner.get("@dam_name")
                    dog["sireName"] = runner.get("@sire_name")
                    dog["whelp"] = runner.get("@whelp")
                    dog["bitchSeason"] = runner.get("@bitch_season")
                    dog["seeding"] = runner.get("@seeding")
                    dog["comment"] = runner.get("Comment")
                    dog["form"] = []
                    if ("Form" in runner):
                        if (type(runner["Form"]) not in (tuple, list)):
                           runner["Form"]=[ runner["Form"] ]
                        for form in runner["Form"]:
                            f={}
                            try:
                                f["date"] =  form.get("@date")
                                f["position"] =  form.get("@position")
                                if "@weight" in form and form["@weight"] != "" :
                                    f["weight"] =  float(form["@weight"])
                                if "@winners_time" in form and form["@winners_time"] != "" :
                                    f["winnersTime"] =  float(form["@winners_time"])
                                if "@calc_time" in form and form["@calc_time"] != "" :
                                    f["calcTime"] =  float(form["@calc_time"])
                                if "@dist"  in form and form ["@dist"] != "" : 
                                    f["distance"] =  int(form["@dist"])
                                if "@trap" in form and form["@trap"] !="" :
                                    f["trap"] =  int(form["@trap"])
                                f["distanceBeaten"] =  form.get("@dist_beaten")
                                f["winner"] =  form.get("@winner")
                                f["going"] =  form.get("@going")
                                f["grade"] =  form.get("@grade")
                                f["comment"] =  form.get("@comment")
                                f["bend"] = form.get("@bend")
                                if "@sectional_time" in form and form["@sectional_time"] != "" :
                                    f["sectionalTime"] = float(form["@sectional_time"])
                            except Exception as e:
                                print ("Got exception in form")
                                print(e)
                            dog["form"].append(f)  
                            
#                else:
                    
                except Exception as e:
                    print ("Got exception in dog")
                    print (e)
                
           
                raceJSON["runners"].append(dog)
      
            print ("Saving race " + raceId)
            response = requests.put(esURL+"rp_dogs/race/"+raceId, headers=esHeaders, json=raceJSON)
            if not response.ok:
                payload = {"eventType": "ParseRPDogCardFile", "fileTitle": key, 
                    "errorExists": "true", 
                    "errorMessage": "failed to save " + str(raceId)}  
                print ("HTTP status code: " + str(response.status_code))
                print (response.text)
                print ("obStartTime >" + obStartTime + "<")
                needRetry=True

    if needRetry :
        # Not all races found, retry as OB can still be parsing it
        payload = {"eventType": "ParseRPDogCardFile", "fileTitle": key, 
                "errorExists": "false", 
                "errorMessage": "Not all races found, requesting retry"}
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
        # Copy file to processed dir too as the races are in ES even though
        # might not be mapped. Horse Declarations can be processed!
        # but do not delete in to_process as we're still retrying
        newkey=key.replace(environ['S3_TO_PROCESS_DIR'],environ['S3_PROCESSED_DIR'])
        print ("Copying from " + key + " to " + newkey)
        s3.copy_object(
                ACL='private',
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': key},
                Key=newkey
            )
        raise MyMustRetry(key)
    
    Finish(bucket,key)
        
def Finish(bucket,key):    
    
    newkey=key.replace(environ['S3_TO_PROCESS_DIR'],environ['S3_PROCESSED_DIR'])
    print ("moving from " + key + " to " + newkey)
    try:
        s3.copy_object(
                ACL='private',
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': key},
                Key=newkey
            )
        s3.delete_object(Bucket=bucket, Key=key)
    except:
        print ("copy failed")
    
    payload = {"eventType": "ParseRPDogCardFile", "fileTitle": key, 
                "errorExists": "false", 
                "errorMessage": "completed"}
    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
