import boto3
import click

def publishS3ContentsToTopic(bucket, topic_arn):
    snsclient = boto3.client('sns')

    for obj in listS3Objects(bucket):
        msg = readS3Object(bucket, obj)
        response = snsclient.publish(
            TopicArn = topic_arn,
            Message = msg
        )
        print("Published: " + obj)

def readS3Object(bucket, object):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, object)
    return obj.get()['Body'].read().decode('utf-8')

def listS3Objects(bucket):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    objCollection = bucket.objects.all()
    return [obj.key for obj in objCollection]

@click.command(
    help = "Publishes all objects in an S3 BUCKET to an SNS TOPIC_ARN.")
@click.argument("bucket")
@click.argument("topic_arn")
def cli(bucket, topic_arn):
    click.echo("bucket: " + bucket + " topic_arn: " + topic_arn) 
    publishS3ContentsToTopic(bucket, topic_arn)

def main():
    cli()

if __name__ == "__main__":
    main()
