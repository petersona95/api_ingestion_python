import json
import boto3
import requests as req
import datetime
import pytz #used for converting to central time

s3 = boto3.client('s3') #assumes boto.cfg is set up
#boto3 is used to create a connection to S3. If you want to run this locally, it requires you create a boto.cfg
#https://www.youtube.com/watch?v=VSpshSNY0vA - this link walks you through the process of creating a boto.cfg
def lambda_handler(event, context):
    Bookmark = "test"
    #loop until no bookmark is returned. we hardcode the first bookmark to ensure the first file is always returned
    while((Bookmark is not None) and (Bookmark is not '')):
        bucket = 'rwl-datavault'
        
        #get chicago timezone, pass into datetime to get chicago time and convert to a string, then remove last 6 characters
        chi = pytz.timezone('US/Central')
        chi_time = str(datetime.datetime.now(chi))
        #remove the location at the end, aka drop the last 6 characters
        chi_time_no_loc = chi_time[:len(chi_time) - 6]
        
        fileName = 'upsapi/To_Be_Processed/' + 'json_' + chi_time_no_loc + '.json'
        _jsonRequest ={
            "AccessRequest": {
                "AccessLicenseNumber":"<ENTER LICENSE NUMBER HERE>",
                "UserId":"ENTER USER ID HERE",
                "Password":"ENTER PASSWORD HERE" 
            },
            "QuantumViewRequest":{
                "Request":{
                    "TransactionReference":{
                        "CustomerContext":"Customer Context"  ,
                        "XpciVersion":"1.0007" 
                    },
                    "RequestAction":"QVEvents",
                    "RequestOption":"01",
                },        
                "SubscriptionRequest":{
                    "Name": "RWDSFOutbound"
                }
            }
        }
        _request = json.dumps(_jsonRequest)
        print('REQUEST STRING: ' , _request)
        print('\n')
        #wwwcie is dev environment, onlinetools is prod
        #_host='https://wwwcie.ups.com/rest/QVEvents'
        _host= 'https://onlinetools.ups.com/rest/QVEvents'
        #request a response
        res = req.post(_host,_request)
        ResponseDict= json.loads(res.content)
        try:  
            Bookmark = ResponseDict["QuantumViewResponse"]["Bookmark"]
        #if no bookmark, the loop will not repeat
        except:
            Bookmark = ''
            #break
        #translate to a usable format
        transactionToUpload = res.content.decode("utf-8").replace('\n','')
        
        #if you want to test locally, use this instead of s3 upload..
        #desktop = os.path.join(os.path.join(os.path.join(os.environ['USERPROFILE']),'Desktop'),'Json.json')
        #desktop = "E:/Aptitive/Redwood - Frito Lay/Redwood-FL/UPS_API/Json-new2.json"
        #with open(desktop, 'w') as json_file:
        #   json_file.write(res.content.decode("utf-8").replace('\n',''))
        #   json_file.close()
        #upload to s3
        uploadByteStream = bytes(json.dumps(transactionToUpload).encode('UTF-8'))
        #send file to s3
        s3.put_object(Bucket=bucket, Key=fileName, Body=uploadByteStream)
        print('Put Complete')