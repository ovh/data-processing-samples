import boto3
import io

if __name__ == "__main__":

    # here we use boto3 to access the container through the Swift S3 API
    # so we have to define some attributes to provide an access to the swift container
    s3 = boto3.client('s3',
                      aws_access_key_id="7decf61921524a6b828c9305a77bb201",
                      aws_secret_access_key="9e9c50f2ff514fc3bdc5f98e61bec81f",
                      endpoint_url="https://s3.gra.cloud.ovh.net/",
                      region_name="gra")

    bytes_to_write = b"This a string to be written in a file."

    file = io.BytesIO(bytes_to_write)

    # write string through the S3 API with boto3
    s3.upload_fileobj(file, "odp-s3", "test_write_string.txt")
