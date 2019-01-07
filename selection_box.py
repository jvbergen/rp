from __future__ import print_function

execfile("rp_import.py")

def selection_box_handler(event):
    
    key = unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    bucket = unquote_plus(event['Records'][0]['s3']['bucket']['name'].encode('utf8'))
    
    print ("Parsing " + bucket + ":" + key)
    payload = {"eventType": "ParseRPNewsFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Starting " + key }
    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
    newsString = None
    
    try:
#    if (1==1):
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        # Store XML in dictionary
        races=parse(body);
        # print(newspapers)
        if (type(races["SelectionBox"]["Race"]) not in (tuple, list)):
            races["SelectionBox"]["Race"] = [ races["SelectionBox"]["Race"] ]
        for race in races["SelectionBox"]["Race"]:
            if ("Newspaper" not in race):
                # RP not giving any NewsPapers for this race
                print ("No Newspaper for race " + race["@uid"])
                continue
            try:
                response = requests.get(esURL+"rp_race/race/"+race["@uid"], headers=esHeaders)
                if (response.ok == False or response.json()["found"] == False):
                    print ('Race Not Found: '+race["@uid"])
                    payload = {"eventType": "ParseRPNewsFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Race not found " + race["@uid"] }
                    requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                    continue
                raceInfo=response.json()["_source"]
                raceInfo["newspapers"]=[]
                     
                if (type(race["Newspaper"]) not in (tuple, list)):
                    race["Newspaper"] = [ race["Newspaper"]]
                for newspaper in race["Newspaper"]:
                    newsJSON = parse_news(newspaper)
                    raceInfo["newspapers"].append(newsJSON)

                requests.post(esURL+"rp_race/race/"+race["@uid"], headers=esHeaders, json = raceInfo)
            
            except Exception, e:
                payload = {"eventType": "ParseRPNewsFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Exception" + str(e) }
                requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
                print(e)
     
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
        payload = {"eventType": "ParseRPNewsFile", "fileTitle": key, "errorExists": "false", "errorMessage": "Finished" + key }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)
        
    except Exception, e:
#    else:
        print (e)

        payload = {"eventType": "ParseRPNewsFile", "fileTitle": key, "errorExists": "true", "errorMessage": "Exception" + str(e) }
        requests.post(nrURL, data=dumps(payload), headers=nrHeaders)


def parse_news (news):
    newsJSON = {}
    newsJSON["name"]=news["@name"]
    if ("@tipster" in news) :
        newsJSON["tipster"]=news["@tipster"]
    if ("@selection" in news) :
        newsJSON["selection"]=news["@selection"]
    if ("@rp_tip" in news) :
        newsJSON["rpTip"]=news["@rp_tip"]
    if ("@tips" in news) :
        newsJSON["tips"]=news["@tips"]
    if ("@flag" in news) :
        newsJSON["flag"]=news["@flag"]
    if ("@selection_uid" in news) :
        newsJSON["rpSelectionUid"]=int(news["@selection_uid"])
    return newsJSON