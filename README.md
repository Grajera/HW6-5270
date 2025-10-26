# HW6-5270

### How to run
Python 3.10+
pip install -r requirements.txt
Configure AWS CLI/SDK credentials (aws configure)

### Run the consumer to S3:
python consumer.py --source-bucket <bucket-2> --dest s3 --dest-bucket <bucket-3> --region us-west-2 --poll-ms 100

### Run to DynamoDB:
python consumer.py --source-bucket <bucket-2> --dest dynamo --dynamo-table <widgets-table> --region us-west-2

### Run tests:
pytest -q