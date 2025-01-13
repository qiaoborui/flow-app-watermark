import json
import logging
from pathlib import Path
from typing import Dict, Any, Callable, TypeVar, Union
from urllib.parse import unquote
import time
import sys
import hashlib
import functools

from s3_util import download_file_from_s3, upload_file_to_s3, parse_s3_url, check_processed_video
from process_util import run_command, get_video_info

# Configure logging for Lambda
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Type variable for the decorator
T = TypeVar('T')

def timing_decorator(func: Callable[..., T]) -> Callable[..., tuple[T, float]]:
    """Decorator to measure execution time of functions"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> tuple[T, float]:
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = time.time() - start_time
        logger.info(f"{func.__name__} time: {execution_time:.2f}s")
        return result, execution_time
    return wrapper

# S3 bucket for processed videos
OUTPUT_BUCKET = "flow-app-media"
# Output prefix for processed videos
OUTPUT_PREFIX = "processed"
# Path to watermark image
WATERMARK_PATH = str(Path(__file__).parent / "watermark.png")
# Supported video formats
SUPPORTED_FORMATS = [".mp4", ".mov", ".avi"]
# Duration of the outro in seconds
OUTRO_DURATION = 1
# Watermark size percentage (relative to video height)
WATERMARK_SIZE_PERCENT = 10
# Watermark opacity (0-1, where 1 is fully opaque and 0 is fully transparent)
WATERMARK_OPACITY = 0.5
# Position change interval in seconds
POSITION_CHANGE_INTERVAL = 2
# FFmpeg encoding preset (ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow)
FFMPEG_PRESET = "ultrafast"
# Whether to disable caching of processed videos
DISABLE_CACHE = True

# Development mode settings
DEV_MODE = True  # Set to True to keep temp files in current directory
# Use system temp directory or current directory based on dev mode
TEMP_DIR = Path('./tmp') if DEV_MODE else Path('/tmp')
CLEANUP_TEMP_FILES = not DEV_MODE

@timing_decorator
def create_outro(work_dir: Path, video_info: Dict, watermark_path: str) -> Path:
    video_stream = next(s for s in video_info['streams'] if s['codec_type'] == 'video')
    width = int(video_stream['width'])
    height = int(video_stream['height'])
    watermark_height = height * WATERMARK_SIZE_PERCENT // 100
    
    outro_path = work_dir / "outro.mp4"
    run_command(
        'ffmpeg',
        [
            '-f', 'lavfi',
            '-i', f'color=c=black:s={width}x{height}:d={OUTRO_DURATION}',
            '-f', 'lavfi',
            '-i', f'anullsrc=channel_layout=stereo:sample_rate=44100:d={OUTRO_DURATION}',
            '-c:v', 'libx264',
            '-c:a', 'aac',
            '-shortest',
            '-preset', FFMPEG_PRESET,
            '-tune', 'stillimage',
            '-pix_fmt', 'yuv420p',
            '-y',
            str(outro_path)
        ]
    )
    
    outro_with_watermark = work_dir / "outro_with_watermark.mp4"
    run_command(
        'ffmpeg',
        [
            '-i', str(outro_path),
            '-i', watermark_path,
            '-filter_complex', 
            f'[1:v]scale=-1:{watermark_height}[watermark];[0:v][watermark]overlay=(main_w-overlay_w)/2:(main_h-overlay_h)/2',
            '-c:v', 'libx264',
            '-c:a', 'copy',
            '-preset', FFMPEG_PRESET,
            '-y',
            str(outro_with_watermark)
        ]
    )
    
    outro_path.unlink()
    return outro_with_watermark

@timing_decorator
def add_watermark(input_file: Path, watermark_path: str, output_file: Path, watermark_height: int) -> None:
    """Add watermark to video with random position changes every 5 seconds"""
    logger.info("watermark height %d",watermark_height)
    run_command(
        'ffmpeg',
        [
            '-i', str(input_file),
            '-i', watermark_path,
            '-filter_complex',f"[1:v]scale=-1:{watermark_height},format=rgba,colorchannelmixer=aa={WATERMARK_OPACITY}[wm];[0:v][wm]overlay='if(ld(0), if(lte(mod(t/{POSITION_CHANGE_INTERVAL},1),0.05),st(0,0);NAN,ld(1)), st(0,1);ld(1);st(1,random(time(0))*(W-w));NAN)':'if(ld(0), if(lte(mod(t/{POSITION_CHANGE_INTERVAL},1),0.05),st(0,0);NAN,ld(1)), st(0,1);ld(1);st(1,random(time(0))*(H-h));NAN)'",
            str(output_file)
        ]
    )

@timing_decorator
def concat_videos(video_file: Path, outro_file: Path, output_file: Path) -> None:
    """Concatenate main video with outro"""
    # Get video info to match frame rate and other parameters
    video_info = get_video_info(str(video_file))
    video_stream = next(s for s in video_info['streams'] if s['codec_type'] == 'video')
    fps = eval(video_stream.get('r_frame_rate', '25/1'))  # Get original video frame rate
    
    run_command(
        'ffmpeg',
        [
            '-i', str(video_file),
            '-i', str(outro_file),
            '-filter_complex', 
            f'[1:v]fps={fps}[v1];[0:v][0:a][v1][1:a]concat=n=2:v=1:a=1[outv][outa]',
            '-map', '[outv]',
            '-map', '[outa]',
            '-c:v', 'libx264',
            '-preset', FFMPEG_PRESET,
            '-pix_fmt', 'yuv420p',
            '-y',
            str(output_file)
        ]
    )

@timing_decorator
def download_input_video(bucket: str, key: str, output_path: str) -> None:
    """Download input video from S3"""
    download_file_from_s3(bucket, key, output_path)

@timing_decorator
def upload_output_video(file_path: str, bucket: str, key: str) -> None:
    """Upload processed video to S3"""
    upload_file_to_s3(bucket, key, file_path)

def parse_request_body(event: Dict[str, Any]) -> str:
    """Parse request body from Lambda Function URL event"""
    if not event.get('body'):
        raise ValueError("Missing request body")
        
    # Decode base64 if needed
    body = event['body']
    if not event.get('isBase64Encoded', True):
        body = json.loads(body)
    
    video_url = body.get('videoUrl')
    
    if not video_url:
        raise ValueError("Missing 'videoUrl' in request body")
        
    return unquote(video_url)

def get_video_hash(video_url: str) -> str:
    """Calculate a hash of the video URL to use as a unique identifier"""
    return hashlib.md5(video_url.encode()).hexdigest()

def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    logger.info(f"Received event: {json.dumps(event)}")
    start_time = time.time()
    temp_files = []
    processing_times = {}
    
    try:
        # Create temp directory if it doesn't exist
        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        
        # Parse request body
        try:
            video_url = parse_request_body(event)
            logger.info(f"Parsed video URL: {video_url}")
        except ValueError as e:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': str(e)})
            }
        
        # Calculate video hash and check if already processed
        video_hash = get_video_hash(video_url)
        if not DISABLE_CACHE:
            existing_url = check_processed_video(OUTPUT_BUCKET, OUTPUT_PREFIX, video_hash)
            if existing_url:
                logger.info(f"Found existing processed video: {existing_url}")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Video was already processed',
                        'url': existing_url,
                        'cached': True
                    })
                }
        
        try:
            input_bucket, key = parse_s3_url(video_url)
        except ValueError as e:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': str(e)})
            }
        
        ext = Path(key).suffix.lower()
        if ext not in SUPPORTED_FORMATS:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': f'Unsupported file format: {ext}'})
            }
        
        file_id = video_hash
        input_file = TEMP_DIR / f"{file_id}_input{ext}"
        watermarked_file = TEMP_DIR / f"{file_id}_watermarked{ext}"
        final_output_file = TEMP_DIR / f"{file_id}_with_outro{ext}"
        output_key = f"{OUTPUT_PREFIX}/{video_hash}_with_outro{ext}"
        
        temp_files.extend([input_file, watermarked_file, final_output_file])
        
        # Download input video
        _, processing_times['download'] = download_input_video(input_bucket, key, str(input_file))
        
        # Get video info and calculate watermark size
        video_info = get_video_info(str(input_file))
        video_stream = next(s for s in video_info['streams'] if s['codec_type'] == 'video')
        height = int(video_stream['height'])
        watermark_height = height * WATERMARK_SIZE_PERCENT // 100
        logger.debug("watermark height",watermark_height)
        
        # Add watermark
        _, processing_times['watermark'] = add_watermark(input_file, WATERMARK_PATH, watermarked_file, watermark_height)
        
        # Create outro
        outro_file, processing_times['outro'] = create_outro(TEMP_DIR, video_info, WATERMARK_PATH)
        temp_files.append(outro_file)
        
        # Concatenate videos
        _, processing_times['concat'] = concat_videos(watermarked_file, outro_file, final_output_file)
        
        # Upload result
        _, processing_times['upload'] = upload_output_video(str(final_output_file), OUTPUT_BUCKET, output_key)
        
        total_time = time.time() - start_time
        processing_times['total'] = total_time
        logger.info(f"Total processing time: {total_time:.2f}s")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Video processed successfully with outro',
                'url': f'https://{OUTPUT_BUCKET}.s3.amazonaws.com/{output_key}',
                'cached': False,
                'processingTime': {k: round(v, 2) for k, v in processing_times.items()}
            })
        }
        
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Processing failed',
                'error': str(e)
            })
        }
    finally:
        # Only cleanup temporary files if not in dev mode
        if CLEANUP_TEMP_FILES:
            for file_path in temp_files:
                try:
                    if file_path.exists():
                        file_path.unlink()
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {file_path}: {e}")

if __name__ == "__main__":
    # 模拟Lambda Function URL的请求
    test_event = {
        "body": "{\"videoUrl\":\"https://flow-app-media.s3.us-west-1.amazonaws.com/trans-video/%25E5%25BF%2583%25E7%2581%25B5%25E9%2593%25BE%25E7%258E%25AF%25EF%25BC%259A%25E7%25A8%25BB%25E5%258F%25B6%25E5%25A7%25AC%25E5%25AD%2590%25EF%25BC%2588Inaba%2520Himeko%25EF%25BC%2589.mp4\"}",
        "isBase64Encoded": False
    }
    logger.info(f"Using watermark file: {WATERMARK_PATH}")
    logger.info(f"Watermark file exists: {Path(WATERMARK_PATH).exists()}")
    res = lambda_handler(test_event, None)
    print(res)
    # add_watermark("tmp/3658c8ba29b2176c12b113211b86b86d_input.mp4","./watermark.png","last.mp4",)
