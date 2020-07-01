import boto3

"""
    Quickly list the first n objects within a given S3 Bucket path.
"""

s3 = boto3.resource(
    's3'
    ,region_name = 'us-west-2'
    ,aws_access_key_id = ''
    ,aws_secret_access_key = ''
)

s3BucketInstance = s3.Bucket('udacity-dend')

for counter, objectSummary in enumerate(s3BucketInstance.objects.filter(Prefix='song_data/')):
    if counter < 2:
        print(objectSummary.key)
    else:
        break