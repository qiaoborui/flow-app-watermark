import boto3
import logging
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)
s3_client = boto3.client('s3')

def download_file_from_s3(bucket: str, file_key: str, file_path: str) -> str:
    """
    Download a file from S3 to local filesystem
    """
    logger.info(f"Downloading {file_key} from bucket {bucket} to {file_path}")
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    
    try:
        s3_client.download_file(bucket, file_key, file_path)
        logger.info(f"Successfully downloaded {file_key}")
        return file_path
    except Exception as e:
        logger.error(f"Error downloading file: {str(e)}")
        raise

def upload_file_to_s3(bucket: str, file_key: str, file_path: str) -> dict:
    """
    Upload a file from local filesystem to S3
    """
    logger.info(f"Uploading {file_path} to bucket {bucket} as {file_key}")
    try:
        response = s3_client.upload_file(file_path, bucket, file_key)
        logger.info(f"Successfully uploaded {file_key}")
        return response
    except Exception as e:
        logger.error(f"Error uploading file: {str(e)}")
        raise

def parse_s3_url(url: str) -> tuple[str, str]:
    """Parse S3 URL into bucket and key"""
    parsed = urlparse(url)
    bucket = parsed.netloc.split('.')[0]
    key = parsed.path.lstrip('/')
    return bucket, key

def check_processed_video(bucket: str, prefix: str, video_hash: str) -> str | None:
    """Check if a processed version of the video already exists in S3"""
    try:
        # List objects with the hash prefix
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=f"{prefix}/{video_hash}_"
        )
        
        # If any objects found, return the URL of the first one
        if response.get('Contents'):
            key = response['Contents'][0]['Key']
            return f"https://{bucket}.s3.amazonaws.com/{key}"
    except Exception as e:
        logger.warning(f"Error checking for processed video: {e}")
    return None 