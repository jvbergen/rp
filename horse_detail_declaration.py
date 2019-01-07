from __future__ import print_function

execfile("rp_import.py")

def horse_detail_declaration_handler(event):
    raceid=0
    oldraceid=0
    key = unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    bucket = unquote_plus(event['Records'][0]['s3']['bucket']['name'].encode('utf8'))
    s3bucket = resource('s3').Bucket(bucket)
    
    payload = {"eventType": "ParseRPHorsesFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Starting parsing " + key }
    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)

    usaRace = "_USA" in key
    
    testkey = key.replace("to_process/HorseDetailsDecs_","processed/DeclarationsEarly_")
    objs = list(s3bucket.objects.filter(Prefix=testkey))
    print ("testing " + testkey)
    if len(objs) > 0 and objs[0].key == testkey:
        print (*objs)
        print("Exists!")
    else:
        testkey = key.replace("to_process/HorseDetailsDecs_","processed/Declarations_")
        print ("testing " + testkey)
        objs = list(s3bucket.objects.filter(Prefix=testkey))
        if len(objs) > 0 and objs[0].key == testkey:
            print("Exists!")
        else:
            testkey = key.replace("to_process/HorseDetailsDecs_","processed/Declarations_Foreign_")
            print ("testing " + testkey)
            objs = list(s3bucket.objects.filter(Prefix=testkey))
            if len(objs) > 0 and objs[0].key == testkey:
                print("Exists!")
            else:
                payload = {"eventType": "ParseRPHorsesFile", "fileTitle": key, "errorExists": "false", "errorMessage": "No matching declaration file (yet)" + key + ""}
                requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                raise MyMustRetry (key)
    
   
    try:
#    if (1==1):
        # Read the XML file
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        # Store XML in dictionary
        horses=parse(body);
        
        if "Horse" not in horses["HorseDetails"]:
            # Empty file
            Finish(bucket, key)
            return 0
            
        # # Step through the horse elements 1 by 1
        # fileDate=horses["HorseDetails"]["@filename"][0:8]
        # raceDate=fileDate[0:4]+"-"+fileDate[4:6]+"-"+fileDate[6:8]
        # nextRaceDate=(datetime.strptime(raceDate,"%Y-%m-%d")+timedelta(days=1)).strftime("%Y-%m-%d")
        # secondNextRaceDate=(datetime.strptime(raceDate,"%Y-%m-%d")+timedelta(days=2)).strftime("%Y-%m-%d")
        for horse in horses["HorseDetails"]["Horse"]:
            
            if horse["@type"] != "Runner":
                #Sir and Dam etc info, skip
                continue
            
            response = requests.get(esURL+"rp_horse/horse/"+horse["@id"], headers=esHeaders)
            if (response.ok and response.json()["found"]==True and "lastRace" in response.json()["_source"]):
                horseJSON = response.json()["_source"]
              
                raceid = str(horseJSON["lastRace"]["raceId"])
                raceOffset = horseJSON["lastRace"]["offset"]
            else :
                print ("Can't find "+ horse["@id"] + " in today's races" )
                payload = {"eventType": "ParseRPHorsesFile", "fileTitle": key, 
                        "errorExists": "true", 
                        "errorMessage": "Not able to find race for " + horse["@name"] + " (" + str(horse["@id"]) + ")" }
                requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                continue
            if (not "form" in horseJSON):
                horseJSON["form"]=[]
            # Don't save the document every horse, only once we moved to a new race.
            if oldraceid != raceid:
                # moved on to new race, save the old race info in Redis
                if ((oldraceid != 0) and (race != None)):
                    try:
                        response = requests.post(esURL+"rp_race/race/"+oldraceid+"/_update", headers=esHeaders, json = { "doc" : race})
                        print ("Saved race " + oldraceid)
                        oldraceid = 0
                    except Exception as e:
                        print ("Failed to save Form for race " + oldraceid)
                        print (e)
                         # SEND ERROR TO NEW RELIC
                        payload = {"eventType": "ParseRPHorsesFile", "fileTitle": key, 
                            "errorExists": "true", 
                            "errorMessage": "failed to save race:" + oldraceid +" " + dumps(str(e)) }
                        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                # Get the race document.
                print ("loading race " + raceid)
                response = requests.get(esURL+"rp_race/race/"+raceid, headers=esHeaders)
                if (response.ok == False or response.json()["found"] == False):
                    print ("failed to load race " + raceid)
                    payload = {"eventType": "ParseRPHorsesFile", "fileTitle": key, 
                            "errorExists": "true", 
                            "errorMessage": "failed to load race:" + raceid }
                    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                    continue 
                
                race=response.json()["_source"]
                # Save the race id to compare against in later steps
                oldraceid=raceid
            
            # Check if we managed to find the race
         
            if "Spotlight" in horse:
                race["horses"][raceOffset]["spotlight"] = horse["Spotlight"]
            if "DiomedComment" in horse:
                race["horses"][raceOffset]["diomedComment"] = horse["DiomedComment"]
            if "BreedingComment" in horse:
                race["horses"][raceOffset]["breedingComment"] = horse["BreedingComment"]
            if (not usaRace):
                # Create empty form, any form will be overwritten.
                if raceOffset != -1:
                    race["horses"][raceOffset]["form"]=[]
                #No form in the USA Horse Details XML
                savedHorseForm=horseJSON["form"]
                if (not "Form" in horse ):
                    #No form in XML, try to find old form
                    length = len(savedHorseForm)
                    if (length)>2:
                        race["horses"][raceOffset]["form"].append(savedHorseForm[length-1])
                        race["horses"][raceOffset]["form"].append(savedHorseForm[length-2])
                        race["horses"][raceOffset]["form"].append(savedHorseForm[length-3])
                #Not a USA race and there is a Form object, 
                elif (type(horse["Form"]) in (tuple, list)):
                    # we have an array with form, step through it
                    for form in horse["Form"]:
                        # start with new empty Form element f
                        formJSON = parse_form(form)
                        savedHorseForm.append(formJSON)
                        if (raceOffset != -1):
                            race["horses"][raceOffset]["form"].append(formJSON)
                else:
                    # Single form element
                    formJSON = parse_form(horse["Form"])
                    savedHorseForm.append(formJSON)

                    if (raceOffset != -1):
                        race["horses"][raceOffset]["form"].append(formJSON)
                        
                if savedHorseForm is not None :
                    response = requests.post(esURL+"rp_horse/horse/"+ horse["@id"] +"/_update", headers=esHeaders, json = { "doc" : {"form" : savedHorseForm }})
            
        # Last race parses. Save it 
        try:
            response = requests.post(esURL+"rp_race/race/"+oldraceid+"/_update", headers=esHeaders, json = { "doc" : race})
            print ("Saved race " + str(oldraceid))
        except Exception as e:
            
            print ("Failed to save Form for race " + str(oldraceid))
            print (e)
              # SEND ERROR TO NEW RELIC
            payload = {"eventType": "ParseRPHorsesFile", "fileTitle": key, 
                            "errorExists": "true", 
                            "errorMessage": "failed to save race:" + oldraceid +" " + dumps(str(e)) }
            requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
            print(r)
        Finish(bucket, key)
    
#    else :
    except Exception as e:
        print ("Got Exception")
        print(e)
        # SEND ERROR TO NEW RELIC
        payload = {"eventType": "ParseRPHorsesFile", "fileTitle": key, 
                            "errorExists": "true", 
                            "errorMessage": "got exception" + dumps(str(e)) }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
        
def Finish(bucket, key):
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
    except Exception as e:
        print ("Failed to copy")
        print (e)
    
    payload = {"eventType": "ParseRPHorsesFile", "fileTitle": key, 
                        "errorExists": "false", 
                        "errorMessage": "completed" }
    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)


def parse_form (form):
    formJSON = {}
    formJSON["raceid"]=int(form["@raceid"])
    formJSON["date"]=form["@date"]
    formJSON["course"]=form["@course"]
    if ("@weight_lbs" in form) :
        weight = int(form["@weight_lbs"])
        stone = int(weight/14)
        formJSON["weightLbs"]=weight
        formJSON["weight"]=str(stone)+"-"+str(weight-stone*14)
    if ("@jockey" in form) :
        formJSON["jockey"]=form["@jockey"]
    if ("@conditions" in form) :
        formJSON["condition"]=form["@conditions"]
    if ("@race_outcome" in form) :
        formJSON["outcome"]=form["@race_outcome"]
    if ("@topspeed" in form) :
        formJSON["topspeed"]=form["@topspeed"]
    if ("@rpr" in form) :
        formJSON["rpr"]=form["@rpr"]
    if ("@official_rating" in form) :
        formJSON["officialRating"]=form["@official_rating"]
    return formJSON
