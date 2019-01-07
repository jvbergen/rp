def match_event (typeMap, url, apiKey, meetingDate, startTime, startTime2="", time_24=""):
    returnId = []
    obStartTime = ""
    for obId in typeMap :
        # Don't try to request SS data if either we don't have an event type ID or we already have found the event
        if (obId != "" and obId != -1 and len(returnId) == 0):
            contents = urlopen(url + "types/" + obId + "/events?api-key=" + apiKey).read()
            sAPIResponse = loads(contents)
            if ("events" in sAPIResponse and "event"  in sAPIResponse["events"]):
                if (type(sAPIResponse["events"]["event"] ) not in (tuple, list)):
                    sAPIResponse["events"]["event"]=[ sAPIResponse["events"]["event"] ]
                for obRace in sAPIResponse["events"]["event"]:
                    if ("eventDateTime" in obRace):
                        utc = datetime.strptime(obRace["eventDateTime"],"%Y-%m-%dT%H:%M:%SZ")
                        utc = utc.replace(tzinfo=from_zone)
                        if (time_24 != ""):
                            # We have a 24 time format in the race, compare agaist that 
                            london = utc.astimezone(to_zone).strftime("%Y-%m-%dT%H:%M:00")
                            if (london == time_24 ):
                                returnId.append(obRace["eventKey"])
                                obStartTime = obRace["eventDateTime"]
                        else:
                            # Convert time zone
                            london = utc.astimezone(to_zone).strftime("%H:%M %Y-%m-%d")
                            startDateTime = startTime + " " + meetingDate
                            startDateTime2 = startTime2 + " " + meetingDate
                            if (london == startDateTime or london == startDateTime2):
                                returnId.append(obRace["eventKey"])
                                obStartTime = obRace["eventDateTime"]
    return returnId,obStartTime