import boto3
import os


def upload_file_to_s3(path, s3):
    for path, directory, files in os.walk(path):
        for f in files:
            file_path = path + "\\" + f
            file_path = file_path.replace("\\", "/")
            s3.upload_file(file_path, your_bucket_name, file_path)


if __name__ == '__main__':
    s3 = boto3.client('s3', aws_access_key_id='',
                      aws_secret_access_key='')
    upload_file_to_s3('log-data', s3)
    upload_file_to_s3('song-data', s3)
    s3.upload_file("log_json_path.json",
                   your_bucket_name, "log_json_path.json")
