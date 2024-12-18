
#example curl
# curl  -H "Authorization: Bearer $(gcloud auth print-identity-token)"   "[cloud run url]" -X POST 
# --data '{"verb": "[GET/PUT]", "expiry":"[30s/5m]", "bucket":"[BUCKET_NAME]","objects":[ {"id": "[IMAGE_ID]", "gcsUri": "[STORAGE_FILE_PATH]"}] }' -H "content-type: application/json"


import json
import os
import urllib.parse
from datetime import datetime, timedelta


import google.auth
from google.auth.transport import requests    
from google.cloud import storage
import functions_framework


def sign_object(method, bucket_name, object_uri, expires, service_account_email, access_token):

    # Create storage object to sign
    client = storage.Client()

     # get filename from uri string - gs://[bucket_name]/[file_name]
    bucket_len = len(bucket_name) + 6
    filename = object_uri[bucket_len:]

    if method == 'PUT':
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(filename)
    else:
        bucket = client.get_bucket(bucket_name)
        blob = bucket.get_blob(filename)

    url = blob.generate_signed_url(
        version="v4",
        expiration=expires,
        service_account_email=service_account_email, 
        access_token=access_token,
        method=method
        )

    return url


@functions_framework.http
def get_url(request):
    # read POST json, expected format { "bucket": "x", "expiry": "20s", "objects": []}
    path = request.path.lower()
    request_json = request.get_json(silent=True)
    bucket_name = request_json["bucket"]
    expiry = request_json["expiry"]
   
    #print("OK")
    #print(path)
    #print(bucket_name)

    expiry_unit = expiry[-1]
    expiry_val = expiry[0:-1]
    
    delta_unit = {"s" : "seconds", "m": "minutes", "h" : "hours", "d" : "days"}.get(expiry_unit, "seconds")

    #print(f'expiry  {expiry_val} {delta_unit}')

    if expiry_unit == "s":
        expires = datetime.utcnow() + timedelta( seconds = int(expiry_val))
    elif expiry_unit == "m":
        expires = datetime.utcnow() + timedelta( minutes = int(expiry_val))
    elif expiry_unit == "h":
        expires = datetime.utcnow() + timedelta( hours = int(expiry_val))
    elif expiry_unit == "d":
        expires = datetime.utcnow() + timedelta( days = int(expiry_val))
    else:
        expires = datetime.utcnow() + timedelta( seconds = 60)


    # Get the default credential on the current environment
    credentials, project_id = google.auth.default()
    # Refresh request to get the access token 
    req = requests.Request()
    credentials.refresh(req)

    # specify service account only for local development, deployment will use
    #   the assigned service account
    service_account_email = os.environ.get('SVC_ACCT', None)
    if hasattr(credentials, 'service_account_email'):
        service_account_email = credentials.service_account_email
        
    access_token=credentials.token    


    # signedUrl request for general objects
    if path.startswith("/objects"):
        verb = (request_json["verb"]).upper()
        objects = request_json["objects"]   

        if verb == "PUT":
            for object in objects:
                object_uri = object["gcsUri"]

                url = sign_object('PUT', bucket_name, object_uri, expires, service_account_email, access_token)
                object["signedurl"] = url;  
        else:            
            for object in objects:
                object_uri = object["gcsUri"]
                
                url = sign_object('GET', bucket_name, object_uri, expires, service_account_email, access_token)
                object["signedurl"] = url;

        http_status = 200

    # signedUrl request for imagen predictions   
    elif path.startswith("/imagen"):
        predictions = request_json["predictions"]

        for object in predictions:
            object_uri = object["gcsUri"]
        
            url = sign_object('GET', bucket_name, object_uri, expires, service_account_email, access_token)
            object["signedurl"] = url;

        http_status = 200
    
    else:
        response_string = '{"fault": {"faultstring" : "this request was unknown in Cloud Run."} }'
        request_json = json.loads(response_string)
        http_status = 404


    return json.dumps(request_json, indent=3), http_status


# curl -X GET -H 'Content-Type: image/png' "[SIGNED_URL]"
# curl -X PUT -H 'Content-Type: image/png' --upload-file  [IMAGE_FILE_PATH]   "[SIGNED_URL]"
