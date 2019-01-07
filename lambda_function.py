from __future__ import print_function

execfile("rp_import.py")

from horse_declaration import horse_declaration_handler
from horse_detail_declaration import horse_detail_declaration_handler
from going import going_handler
from selection_box import selection_box_handler
from result import result_handler
from keystats import keystats_handler
from predictor_horse import predictor_horse_handler
from predictor_dog import predictor_dog_handler
from non_runner import non_runner_handler
from dog_cards import dog_cards_handler
from star_ratings import star_ratings_handler
from trends import trends_handler

# This function is called from the SQS queue
# It needs to have a reserved concurrency of 1 to prevent multiple files causing updated on the same
# document in ES as the same time and therefor overwriting eachother

functionMap = { 
    "Declarations" : horse_declaration_handler,
    "DeclarationsEarly": horse_declaration_handler,
    "DeclarationsJoc" : horse_declaration_handler,
    "DeclarationsEx" : horse_declaration_handler,
    "HorseDetailsDecsJocEx" : horse_detail_declaration_handler,
    "HorseDetailsDecsEx" : horse_detail_declaration_handler,
    "HorseDetailsDecs": horse_detail_declaration_handler,
    "Going": going_handler,
    "SelectionBox" : selection_box_handler,
    "Full_Hrs_Result" : result_handler,
    "Fast_Hrs_Result" : result_handler,
    "KeyStats" : keystats_handler,
    "Trends" : trends_handler,
    "Predictor_horseRAW" : predictor_horse_handler,
    "Predictor_Dog" : predictor_dog_handler,
    "Non_Runner" : non_runner_handler,
    "DogCards" : dog_cards_handler,
    "StarRatings" : star_ratings_handler
    }

def lambda_handler(event, context):
    
    for Record in event["Records"]:
        event = loads(Record["body"])
        
        key = unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
        bucket = unquote_plus(event['Records'][0]['s3']['bucket']['name']).encode('utf8')
        print ("Parsing s3://" + bucket+":"+key)

        s3bucket = resource('s3').Bucket(bucket)        
        objs = list(s3bucket.objects.filter(Prefix=key))
        if len(objs) == 0 or objs[0].key != key:
            print ("No such file")
            return
        
        fileStart = key.find("/")+1
        file = key[fileStart:key.find("_",fileStart)]
    
        if file == "Predictor" or file == "Full" or file == "Fast":
            file = key[fileStart:key.find("_",fileStart+10)]
        elif file == "Non" :
            file = key[fileStart:key.find("_",fileStart+4)]

        functionMap.get(file,defaultMap)(event)
        if "move" in event and event["move"]:
            newkey=key.replace(environ['S3_TO_PROCESS_DIR'],environ['S3_PROCESSED_DIR'])
            print ("moving from " + key + " to " + newkey)
            s3.copy_object(
                ACL='private',
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': key},
                Key=newkey
            )
            s3.delete_object(Bucket=bucket, Key=key)
            
    return 
    
def defaultMap(event):
    key = unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    bucket = unquote_plus(event['Records'][0]['s3']['bucket']['name']).encode('utf8')
    
    print ("Known ignored file")
    fileStart = key.find("/")+1
    file = key[fileStart:key.find("_",fileStart)]
    
    if file == "Courses" or file == "DogResults" or file == "Predictor" or file == "Signposts" or file == "StatisticsEx" :
        # known files to ignore
        newkey=key.replace(environ['S3_TO_PROCESS_DIR'],environ['S3_IGNORED_DIR'])
        print ("moving from " + key + " to " + newkey)
        s3.copy_object(
                ACL='private',
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': key},
                Key=newkey
            )
        s3.delete_object(Bucket=bucket, Key=key)
        
    else:
        print ("Unknown XML " + key)
        #TODO ADD NR
