const {"v4": uuidv4} = require('uuid');
var AWS = require('aws-sdk');
const chime = new AWS.Chime({ region: 'us-east-1' });
chime.endpoint = new AWS.Endpoint('https://service.chime.aws.amazon.com/console');
const region = 'us-east-1'

const mediaCaptureBucket = process.env['MEDIA_CAPTURE_BUCKET']
const aws_account_id = process.env['ACCOUNT_ID']

const startTranscribe = (meetingId) => {
    var params = {
        MeetingId: meetingId, /* required */
        TranscriptionConfiguration: { /* required */
          EngineTranscribeSettings: {
            LanguageCode: 'en-US',
            Region: 'us-east-1',
          }
        }
      };
      
      chime.startMeetingTranscription(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else     console.log(data);           // successful response
      });
}

exports.handler = async (event, context) => {
    const body =  JSON.parse(event.body)
    console.log(body) 
    const setRecording = body.setRecording

    if (setRecording) {
        const deleteRequest = {
            "MediaPipelineId": body.mediaPipeLine
        }
        const deleteInfo = await chime.deleteMediaCapturePipeline(deleteRequest).promise()
        console.log(deleteInfo)
                    const response = {
                statusCode: 200,
                body: JSON.stringify(deleteInfo),
                headers: {
                    'Access-Control-Allow-Origin':'*',
                    'Access-Control-Allow-Headers':'*',
                    'Access-Control-Allow-Methods': 'POST, OPTIONS',
                    'Content-Type':'application/json'
                }
            }
            return response
    } else {
        const meetingId = body.meetingId
        startTranscribe(meetingId)
        const captureRequest = {
            "SourceType": "ChimeSdkMeeting",
            "SourceArn": "arn:aws:chime::" + aws_account_id + ":meeting:" + meetingId,
            "SinkType": "S3Bucket",
            "SinkArn": "arn:aws:s3:::" + mediaCaptureBucket + "/captures/" + meetingId    
        }
        try {
            const captureInfo = await chime.createMediaCapturePipeline(captureRequest).promise()
            console.log(captureInfo)
            const response = {
                statusCode: 200,
                body: JSON.stringify(captureInfo),
                headers: {
                    'Access-Control-Allow-Origin':'*',
                    'Access-Control-Allow-Headers':'*',
                    'Access-Control-Allow-Methods': 'POST, OPTIONS',
                    'Content-Type':'application/json'
                }
            }
            return response
        } catch (err) {
           console.log(err)
           return err
        }
    }
}


