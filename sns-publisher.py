import boto3
import click

aws_session = None

# Publish objects back to SNS
def publishS3ContentsToTopic(bucket, topic_arn):
    if aws_session.region_name == 'us-iso-east-1':
        snsclient = aws_session.client('sns', endpoint_url = 'https://sns.us-iso-east-1.c2s.ic.gov')
    else:
        snsclient = aws_session.client('sns')
    objs = listS3Objects(bucket)
    i = 1
    total = len(objs)

    for obj in listS3Objects(bucket):
        msg = readS3Object(bucket, obj)
        response = snsclient.publish(
            TopicArn = topic_arn,
            Message = msg
        )
        print("Published [" + str(i) + "/" + str(total) + "]: " + obj)
        i += 1

# Read the value of the S3 object as a string
def readS3Object(bucket, object):
    if aws_session.region_name == 'us-iso-east-1':
        s3 = aws_session.resource('s3', endpoint_url = 'https://s3.us-iso-east-1.c2s.ic.gov')
    else:    
        s3 = aws_session.resource('s3')
    obj = s3.Object(bucket, object)
    return obj.get()['Body'].read().decode('utf-8')

# List all S3 objects in a given bucket
def listS3Objects(bucket):
    if aws_session.region_name == 'us-iso-east-1':
        s3 = aws_session.resource('s3', endpoint_url = 'https://s3.us-iso-east-1.c2s.ic.gov')
    else:    
        s3 = aws_session.resource('s3')
    bucket = s3.Bucket(bucket)
    objCollection = bucket.objects.all()
    return [obj.key for obj in objCollection]

@click.command(
    help = "Publishes all objects in a REGION's S3 BUCKET to an SNS TOPIC_ARN.")
@click.argument("region")
@click.argument("bucket")
@click.argument("topic_arn")
def cli(region, bucket, topic_arn):
    global aws_session
    click.echo("region: " + region + " bucket: " + bucket + " topic_arn: " + topic_arn)
    aws_session = boto3.Session(region_name = region)
    publishS3ContentsToTopic(bucket, topic_arn)

def main():
    cli()

if __name__ == "__main__":
    main()
