import boto3
import click

aws_session = None

def publishS3ContentsToTopic(bucket, topic_arn):
    snsclient = aws_session.client('sns')

    for obj in listS3Objects(bucket):
        msg = readS3Object(bucket, obj)
        response = snsclient.publish(
            TopicArn = topic_arn,
            Message = msg
        )
        print("Published: " + obj)

def readS3Object(bucket, object):
    s3 = aws_session.resource('s3')
    obj = s3.Object(bucket, object)
    return obj.get()['Body'].read().decode('utf-8')

def listS3Objects(bucket):
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
