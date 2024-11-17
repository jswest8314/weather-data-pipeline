import boto3
from config import Configs

config = Configs()  # initialize the configuration object

def lambda_handler(event, context):
    bucket = config.BUCKET
    s3 = boto3.client('s3')

    # FIND THE MOST RECENT FILE FOR THE STATION:
    object_list = s3.list_objects_v2(Bucket=bucket, Prefix="maps/")["Contents"]  # return all objects and metadata
    count = len(object_list)
    filenames = [x['Key'] for x in object_list]
    modified_datetime = [x['LastModified'] for x in object_list]  # last time the file was modified
    files = dict(zip(filenames, modified_datetime))
    # storage_size = [x['Size'] for x in object_list]  # storage size in Bytes
    most_recent_data = max(modified_datetime)
    most_recent_map = list(files.keys())[list(files.values()).index(most_recent_data)]

    # PULL THE DATA FROM THE MOST RECENT FILE FOR THE STATION:
    response = s3.get_object(Bucket=bucket, Key=most_recent_map)
    # response =   "{'ResponseMetadata': {'RequestId': 'W53A9NA2JQ0NQ9SD', 'HostId': '/cHBcy/f94jfXny7GyxJshc1Q4ZNR/fmVgcRtvoAIn4M8KBXFelh/tzH/aMos08JWgWElXV6tuVFoXXkuwjfJA==', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': '/cHBcy/f94jfXny7GyxJshc1Q4ZNR/fmVgcRtvoAIn4M8KBXFelh/tzH/aMos08JWgWElXV6tuVFoXXkuwjfJA==', 'x-amz-request-id': 'W53A9NA2JQ0NQ9SD', 'date': 'Tue, 16 Jul 2024 14:56:54 GMT', 'last-modified': 'Tue, 16 Jul 2024 09:00:14 GMT', 'etag': '\"c216165dd74c3f24977a43a5b81cef9c\"', 'x-amz-server-side-encryption': 'AES256', 'x-amz-version-id': 'cuYG2rdUgM7fn457gNlEr88olE1ukdJJ', 'accept-ranges': 'bytes', 'content-type': 'binary/octet-stream', 'server': 'AmazonS3', 'content-length': '304'}, 'RetryAttempts': 0}, 'AcceptRanges': 'bytes', 'LastModified': datetime.datetime(2024, 7, 16, 9, 0, 14, tzinfo=tzutc()), 'ContentLength': 304, 'ETag': '\"c216165dd74c3f24977a43a5b81cef9c\"', 'VersionId': 'cuYG2rdUgM7fn457gNlEr88olE1ukdJJ', 'ContentType': 'binary/octet-stream', 'ServerSideEncryption': 'AES256', 'Metadata': {}, 'Body': <botocore.response.StreamingBody object at 0x7f5a8f151150>}"
    map = response["Body"].read()

    return {
        'statusCode': 200,
        'body': map,
        "headers": {"Content-Type": "html"}  # https://repost.aws/questions/QUfEPEO8EgThiEIFPIBy-GUA/return-html-to-browser-from-api-gateway-using-lambda-proxy-integration

    }
