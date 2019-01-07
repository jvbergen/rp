from __future__ import print_function

execfile("rp_import.py")
execfile("event_match.py")

silkCoralUrl = environ['SILK_URL']
silkLadbrokesUrl= environ['SILK_LADBROKES_URL']
raceGraphicsLadbrokesUrl = environ['RACE_IMAGES_LADBROKES_URL']
raceGraphicsCoralUrl = environ['RACE_IMAGES_URL']

ladDigitalMap = []
coralDigitalMap = []
coralRetailMap = []

def horse_declaration_handler(event):
    key = unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    bucket = unquote_plus(event['Records'][0]['s3']['bucket']['name'].encode('utf8'))
    
    payload = {"eventType": "ParseRPDeclarationFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Starting parsing " + key }
    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
    
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj['Body'].read()
    declaration = parse(body)
    
    if ("Meeting" not in declaration["Declaration"]):
        # Empty file, nothing to do
        print ("empty file" + dumps(declaration))
        Finish(bucket,key);
        return 0
    
    needRetry = False
    usaRace = "_USA" in key
  
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('Europe/London')
    
    # if only a single meeting (no array), create an array with a single element
    if (type(declaration["Declaration"]["Meeting"] ) not in (tuple, list)):
        declaration["Declaration"]["Meeting"]=[ declaration["Declaration"]["Meeting"] ]
    # Step through the meetings in the declaration file
    for meeting in declaration["Declaration"]["Meeting"] :
        courseId = meeting["@courseid"]
        going = meeting.get("officialgoing")
        meetingDate = meeting["@date"][0:10]
        print ("Parsing " + meeting["@course"] + " (" +str(courseId) + ")")
        
        ladDigitalMap = []
        coralDigitalMap = []
        coralRetailMap = []

        # Find the possible event type IDs for the OB instances
        response = requests.get(esURL+"rp_mapping/rp_mapping/"+courseId, headers=esHeaders)
        if (response.ok ) :
            responseJSON=response.json()
            if (responseJSON["found"] == True) :
                ladDigitalMap   = responseJSON["_source"]["LadDigital"]
                coralDigitalMap = responseJSON["_source"]["CoralDigital"]
                coralRetailMap  = responseJSON["_source"]["CoralRetail"]
        if (usaRace):
            courseId2=str(int(courseId)+2000)
            response = requests.get(esURL+"rp_mapping/rp_mapping/"+courseId2, headers=esHeaders)
            if (response.ok):
                responseJSON=response.json()
                if (responseJSON["found"] == True) :
                    ladDigitalMap.extend(responseJSON["_source"]["LadDigital"])
                    coralDigitalMap.extend(responseJSON["_source"]["CoralDigital"])
                    coralRetailMap.extend(responseJSON["_source"]["CoralRetail"])

        if (len(ladDigitalMap) == 0 and len(coralDigitalMap) and len (coralRetailMap)):
            payload = {"eventType": "ParseRPDeclarationFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Mapping not found for RP course id" + str(courseid)}
            requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
            print ("mapping not found for " + courseId)
        
        # if only a single meeting (no array), create an array with a single element
        if (type(meeting["Race"]) not in (tuple, list)):
            meeting["Race"]=[ meeting["Race"] ]
        # Step through the races in the racing post meeting
        
        for race in meeting["Race"] :
            
            # try to find the mapping for Coral Digital
            # get RP start time, this is a 12 hour time with a '-' between hours and minuts
            startTime = race["@time"].replace("-",":")
            if (startTime[1:2] == ':') :
                # time functions have a leading 0 for times <10. Add to match
                startTime = "0" + startTime
            print ("race: " + race["@title"] + " " + startTime)
            hour = int(startTime[0:2])+12
            minute = startTime[3:]
            startTime2 = str(hour) + ":" + minute
            raceId = race["@uid"]
            contents = ""
            obStartTime = ""

# Find the Coral Digital event IDs
            coralDigitalId, obStartTime = match_event (coralDigitalMap, environ['CD_SPORTS_API_URL'], environ['CD_SPORTS_API_KEY'],
                meetingDate, startTime, startTime2, race.get("@time_24",""))
  
             # We checked the event type(s) now see if we fount the event ID
            if (len(coralDigitalId) == 0) :
                payload = {"eventType": "ParseRPDeclarationFile", "fileTitle": key, "errorExists": "true", 
                        "errorMessage": "Failed to find Coral digital event id for rp race " + 
                        race["@title"] + " ("+ raceId + ")" }
                requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                print ("Event ID not found in Coral Digital OB")
                print (coralDigitalMap)
                needRetry = True
# END OF CORAL DIGITAL


# Find the Coral Retail event IDs
            coralRetailId, obStartTime2 = match_event (coralRetailMap, environ['CR_SPORTS_API_URL'], environ['CR_SPORTS_API_KEY'],
                meetingDate, startTime, startTime2, race.get("@time_24",""))
  
            if obStartTime == "" and obStartTime2 !="":
                obStartTime = obStartTime2
             # We checked the event type(s) now see if we fount the event ID
            if (len(coralRetailId) == 0) :
                payload = {"eventType": "ParseRPDeclarationFile", "fileTitle": key, "errorExists": "true", 
                        "errorMessage": "Failed to find Coral retail event id for rp race " + 
                        race["@title"] + " ("+ raceId + ")" }
                requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                print ("Event ID not found in Coral Retail OB")
                print (coralRetailMap)
                needRetry = True
# END OF CORAL RETAIL

# Find the Ladbrokes Digital event IDs
            ladbrokesDigitalId, obStartTime2 = match_event (ladDigitalMap, environ['LD_SPORTS_API_URL'], environ['LD_SPORTS_API_KEY'],
                meetingDate, startTime, startTime2, race.get("@time_24",""))
            
            if obStartTime == "" and obStartTime2 !="":
                obStartTime = obStartTime2
             # We checked the event type(s) now see if we fount the event ID
            if (len(ladbrokesDigitalId) == 0) :
                payload = {"eventType": "ParseRPDeclarationFile", "fileTitle": key, 
                        "errorExists": "true", "errorMessage": "Failed to find Ladbrokes digital event id for rp race "  + 
                        race["@title"] + " ("+ raceId + ")" }
                requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                print ("Event ID not found in Ladbrokes Digital OB")
                print (ladDigitalMap)
                needRetry = True
# END OF LADBROKES DIGITAL
        
            try:
                raceJSON = requests.get(esURL+"rp_race/race/"+raceId, headers=esHeaders).json()["_source"]
            except:
                raceJSON = {}  
            try:
#           if (1==1):
                print ("OB 1: " + str(coralDigitalId) + " OB 2: " + str(coralRetailId) + " OB 3: " + str(ladbrokesDigitalId))
                if len(coralDigitalId) != 0 :
                    raceJSON["coralDigitalEventId"] = coralDigitalId
                if len(coralRetailId) != 0 :
                    raceJSON["coralRetailEventId"] = coralRetailId
                if len(ladbrokesDigitalId) != 0 :
                    raceJSON["ladbrokesDigitalEventId"] = ladbrokesDigitalId
                if obStartTime != "" :
                    raceJSON["obStartTime"] = obStartTime 
                raceJSON["rpRaceId"] = int(raceId)
                raceJSON["rpCourseId"] = int(meeting.get("@courseid"))
                raceJSON["raceName"] = race.get("@title")
                if "@yards in race":
                    raceJSON["yards"] = int(race["@yards"])
                if "runners" in race:
                    raceJSON["runners"] = int(race["@runners"])
                if "@crsgraphic" in race:
                    raceJSON["courseGraphicsCoral"] = raceGraphicsCoralUrl + race.get("@crsgraphic")
                    raceJSON["courseGraphicsLadbrokes"] = raceGraphicsLadbrokesUrl + race.get("@crsgraphic")
                raceJSON["courseName"] = meeting.get("@course") 
                raceJSON["raceNo"] = int(race.get("@race_no"))
                raceJSON["time"] = startTime
                # Don't want to overwrite with null if it is missing in a newer file
                if "@distance_mfy" in race:
                    raceJSON["distance"]=race["@distance_mfy"]
                raceJSON["going"]=going
                if "@going" in race:
                    raceJSON["goingCode"]=race["@going"]
                if "Verdict" in race:
                    raceJSON["verdict"]=race["Verdict"]
                if "Diomed" in race:
                    raceJSON["diomed"] = race["Diomed"]
                
            except Exception as e:
#    else:
                print ("Got exception in race " + raceId)
                print(e)
                payload = {"eventType": "ParseRPDeclarationFile", "fileTitle": key, 
                    "errorExists": "true", 
                    "errorMessage": "Exception in parsing race if " + raceId + dumps(str(e))}
                requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                
            if ("horses" not in raceJSON):
                raceJSON["horses"] = []
           
            # Assuming more than 1 horse in a race so no check for it being an array
            for runner in race["Runner"]:
                horse = {}
                rpHorseId = runner["@horseid"]
                response = requests.get(esURL+"rp_horse/horse/"+rpHorseId, headers=esHeaders)
                updateES = False # update ES with the horse informantion
                updateRace = False
                if (response.ok and response.json()["found"]==True):
#                    print ("found horse " + rpHorseId )
                    updateES = True
                    responseJSON = response.json()["_source"]
                    if ("lastRace" in responseJSON and "raceId" in responseJSON["lastRace"] and 
                        responseJSON["lastRace"]["raceId"] == raceId and responseJSON["lastRace"]["offset"] < len(raceJSON["horses"])):
                        offset = responseJSON["lastRace"]["offset"]
                        updateRace = True
                        try:
                            horse = raceJSON["horses"][offset]
                        except Exception as e:
                            print (e)

#                        print ("use offset " + offset)
                try:        
                    horse["rpHorseId"] = int(rpHorseId)
                    
                    horse["saddle"] = runner.get("@saddle")
                    if "@jockey" in runner:
                        horse["jockey"] = runner["@jockey"]
                    if "@headgear" in runner:
                        horse["headgear"] = runner["@headgear"]
                    horse["horseName"] = runner["@horsename"]
                    horse["trainer"] = runner["@trainer"]
                    if "@silkname" in runner:
                        horse["silkCoral"] = silkCoralUrl+runner["@silkname"]
                        horse["silkLadbrokes"] = silkLadbrokesUrl + runner["@silkname"]
                    if "@weight_text" in runner:
                        horse["weight"] = runner["@weight_text"]
                    if "@weight_lbs" in runner:
                        horse["weightLbs"] = int(runner["@weight_lbs"])
                    if ("@formfigs" in runner and runner["@formfigs"] != "") :
                        horse["formfigs"] = runner["@formfigs"]
                    if "@rp_rating" in runner:
                        horse["rating"] =  runner["@rp_rating"]
                    if "@unadjusted_master_rating" in runner:
                        horse["unAdjustedMasterRating"] = runner["@unadjusted_master_rating"]
                    if "@adjusted_master_rating" in runner:
                        horse["adjustedMasterRating"] = runner["@adjusted_master_rating"]
                    if "@horse_age" in runner:
                        horse["horseAge"] = int(runner["@horse_age"])
                    if "@trainer_uid" in runner:
                        horse["rpTrainerId"] = int(runner["@trainer_uid"])
                    if "@jockey_uid" in runner:
                        horse["rpJockeyId"] = int(runner["@jockey_uid"])
                    if "@dayssincerun" in runner:
                        horse["daysSinceRun"] = runner["@dayssincerun"]
                    if "@draw" in runner:
                        horse["draw"] = runner["@draw"]
                    if "@course_distance_winner" in runner:
                        horse["courseDistanceWinner"] = runner ["@course_distance_winner"]
                except Exception as e:
                    print ("Got exception in horse")
                    print (e)
                
                if (updateRace):
                    raceJSON["horses"][offset] = horse
                    # No need to update /horse info
                    #delete lines below later. Needed now as horsemap is new
                    if ("horseMap" not in raceJSON):
                        raceJSON["horseMap"] = {}
                    raceJSON["horseMap"][horse["saddle"]]=offset
                    
                else:
                    offset=len(raceJSON["horses"])
                    raceJSON["horses"].append(horse)
                    if ("horseMap" not in raceJSON):
                        raceJSON["horseMap"] = {}
                    if (horse["saddle"]) :
                        raceJSON["horseMap"][horse["saddle"]]=offset
                    lastRace = {}
                    lastRace["lastRace"] = {}
                    lastRace["lastRace"]["raceId"] = raceId
                    lastRace["lastRace"]["offset"] = offset
                    if (updateES):
                        reponse = requests.post(esURL+"rp_horse/horse/"+rpHorseId+"/_update", headers=esHeaders,
                            json = { "doc" : lastRace })
                    else:
                        reponse = requests.post(esURL+"rp_horse/horse/"+rpHorseId, headers=esHeaders,
                            json = lastRace )
                   
#                raceJSON["horses"].append(horse)
#                horseInRace[runner["@horseid"]]=raceId
            if (not "hasResults" in raceJSON):
                raceJSON["hasResults"] = False
            print ("Saving race " + raceId)
            response = requests.put(esURL+"rp_race/race/"+raceId, headers=esHeaders, json=raceJSON)
            if not response.ok:
                payload = {"eventType": "ParseRPDeclarationFile", "fileTitle": key, 
                    "errorExists": "true", 
                    "errorMessage": "failed to save " + str(raceId)}  
                print ("HTTP status code: " + str(response.status_code))
                print (response.text)
                print ("obStartTime >" + obStartTime + "<")
                needRetry=True
#            redisclient.set("race:"+raceId,dumps(raceJSON))

    if needRetry :
        # Not all races found, retry as OB can still be parsing it
        payload = {"eventType": "ParseRPDeclarationFile", "fileTitle": key, 
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
    
    payload = {"eventType": "ParseRPDeclarationFile", "fileTitle": key, 
                "errorExists": "false", 
                "errorMessage": "completed"}
    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
    # HorseDetailsDecs_USA_20181120_Overnight.xml
    # Declarations_USA_20181120_Overnight.xml
    s3bucket = resource('s3').Bucket(bucket)
    testkey = key.replace(environ['S3_TO_PROCESS_DIR']+"/Declarations_",
        environ['S3_TO_PROCESS_DIR']+"/HorseDetailsDecs_")
    objs = list(s3bucket.objects.filter(Prefix=testkey))
    print ("testing " + testkey)
    if len(objs) > 0:
        event = {"Records": [{"s3": {"object": {"key": key},"bucket":
                {"arn": "arn:aws:s3:::"+bucket,"name": bucket}}}]}
        sqs = {}
        sqs["Records"]=[]
        sqs["Records"]
        sqs["Records"].append({"body": dumps(event), "move" : key.startswith(environ['S3_TO_PROCESS_DIR'])})
        
        invoke_response = lambda_client.invoke(FunctionName="lcg-dev1-rp",
                                InvocationType='Event',
                                Payload=dumps(sqs))