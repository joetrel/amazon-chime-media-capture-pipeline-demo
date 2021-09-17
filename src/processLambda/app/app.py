import boto3
import os
import subprocess
import shlex
from boto3.dynamodb.conditions import Key
import datetime

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

now = datetime.datetime.now()
print(now.year, now.month, now.day, now.hour, now.minute, now.second)

SOURCE_BUCKET = os.environ['MEDIA_CAPTURE_BUCKET']
SOURCE_PREFIX = 'captures'
MEETING_TABLE = os.environ['MEETINGS_TABLE_NAME']
STORAGE_BUCKET = os.environ['STORAGE_BUCKET']
STORAGE_TABLE = os.environ['STORAGE_TABLE_NAME']

def get_attendees(MEETING_ID):
    table = dynamodb.Table(MEETING_TABLE)
    meetingInfo = table.query(
        IndexName='meetingIdIndex',
        KeyConditionExpression=Key('meetingId').eq(MEETING_ID))
    return meetingInfo['Items'][0]

def upload_artifact(file_name, meetingInfo):
    prefix_path = "year=" + str(now.year) + "/month=" + str(now.month) + "/day=" + str(now.day) + "/hour=" + str(now.hour) + "/minute=" + str(now.minute) + "/"
    s3_path = prefix_path + meetingInfo['MeetingInfo']['ExternalMeetingId'] + '-' + file_name
    s3.upload_file('/tmp/' + file_name, STORAGE_BUCKET,  s3_path)

    return s3_path

def update_storage_db(audio_object, event_object, meetingInfo, STORAGE_BUCKET):
    table = dynamodb.Table(STORAGE_TABLE)
    response = table.put_item(
       Item={
            'ExternalMeetingId': meetingInfo['MeetingInfo']['ExternalMeetingId'],
            'Bucket': STORAGE_BUCKET,
            'AudioObject': audio_object,
            'EventObject': event_object
        }
    )

    return response

def process_files(objs_keys, MEETING_ID, file_type, *attendee):
    if file_type == "audio":
        with open('/tmp/' + file_type + '_list.txt', 'w') as f:
            for k in objs_keys:
                basename = os.path.splitext(k)[0]
                ffmpeg_cmd = "ffmpeg -i /tmp/" + k + " -bsf:v h264_mp4toannexb -f mpegts -framerate 15 -c copy /tmp/" + basename + "-" + file_type + ".ts -y"
                command1 = shlex.split(ffmpeg_cmd)
                p1 = subprocess.run(command1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                f.write(f'file \'/tmp/{basename}-{file_type}.ts\'\n')
        
        file_name = file_type + '.wav'
        ffmpeg_cmd = "ffmpeg -f concat -safe 0 -i /tmp/" + file_type + "_list.txt  -c copy /tmp/" + file_name + " -y"
        subprocess.run(shlex.split(ffmpeg_cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if file_type == 'events':
        events = []
        with open('/tmp/' + file_type + '_list.txt', 'w') as f:
            for k in objs_keys:
                with open('/tmp/' + k) as event:
                    events.append(event.read())
                        
        file_name = file_type + '.txt'
        with open('/tmp/' + file_name, 'w') as event_file:
            for event in events:
                event_file.write(str(event)+"\n")

    return file_name
    
    
def handler(event, context):
    print(event)
    MEETING_ID = event.get('detail').get('meetingId')
    print(MEETING_ID)

    meetingInfo = get_attendees(MEETING_ID)

    audioPrefix = SOURCE_PREFIX + '/' + MEETING_ID + '/audio'
    eventPrefix = SOURCE_PREFIX + '/' + MEETING_ID + '/meeting-events'

    audioList = s3.list_objects_v2(
        Bucket=SOURCE_BUCKET,
        Delimiter='string',
        MaxKeys=1000,
        Prefix=audioPrefix
    )
    audioObjects = audioList.get('Contents', [])
    print(audioObjects)

    file_list=[]
    file_type = 'audio'
    for object in audioObjects:
        path, filename = os.path.split(object['Key'])
        s3.download_file(SOURCE_BUCKET, object['Key'], '/tmp/' + filename)
        file_list.append(filename)
    
    objs_keys = [file for file in file_list if file.endswith('.mp4')]    
    audio_file = process_files(objs_keys, MEETING_ID, file_type)
    audio_object = upload_artifact(audio_file, meetingInfo)

    eventList = s3.list_objects_v2(
        Bucket=SOURCE_BUCKET,
        Delimiter='string',
        MaxKeys=1000,
        Prefix=eventPrefix
    )
    eventObjects = eventList.get('Contents', [])
    print(eventObjects)

    file_list=[]
    file_type = 'events'
    for object in eventObjects:
        path, filename = os.path.split(object['Key'])
        s3.download_file(SOURCE_BUCKET, object['Key'], '/tmp/' + filename)
        file_list.append(filename)
    
    objs_keys = [file for file in file_list if file.endswith('.txt')]    
    event_file = process_files(objs_keys, MEETING_ID, file_type)
    event_object = upload_artifact(event_file, meetingInfo)

    update_storage_db(audio_object, event_object, meetingInfo, STORAGE_BUCKET)
    
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST'            
        }
    }