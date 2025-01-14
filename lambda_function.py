import json
import logging
from pathlib import Path
from typing import Dict, Any, Callable, TypeVar, Union
from urllib.parse import unquote
import time
import sys
import hashlib
import functools
import os
import uuid
from PIL import Image, ImageDraw, ImageFont

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
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "flow-app-media")
# Output prefix for processed videos
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "processed")
# Path to watermark image
WATERMARK_PATH = os.environ.get("WATERMARK_PATH", str(Path(__file__).parent / "watermark.png"))
# Path to outro video
OUTRO_VIDEO_PATH = os.environ.get("OUTRO_VIDEO_PATH", str(Path(__file__).parent / "output.mp4"))
# Supported video formats
SUPPORTED_FORMATS = os.environ.get("SUPPORTED_FORMATS", ".mp4,.mov,.avi").split(',')
# Duration of the outro in seconds
OUTRO_DURATION = int(os.environ.get("OUTRO_DURATION", 3))
# Watermark size percentage (relative to video height)
WATERMARK_SIZE_PERCENT = int(os.environ.get("WATERMARK_SIZE_PERCENT", 15))
# Watermark opacity (0-1, where 1 is fully opaque and 0 is fully transparent)
WATERMARK_OPACITY = float(os.environ.get("WATERMARK_OPACITY", 0.7))
# Position change interval in seconds
POSITION_CHANGE_INTERVAL = int(os.environ.get("POSITION_CHANGE_INTERVAL", 5))
# FFmpeg encoding preset
FFMPEG_PRESET = os.environ.get("FFMPEG_PRESET", "medium")
# Whether to disable caching of processed videos
DISABLE_CACHE = os.environ.get("DISABLE_CACHE", "True").lower() in ['true', '1', 't']

# Target resolution for preprocessing (720p)
TARGET_HEIGHT = int(os.environ.get("TARGET_HEIGHT", "720"))
# Target video bitrate (2Mbps)
TARGET_VIDEO_BITRATE = os.environ.get("TARGET_VIDEO_BITRATE", "2M")
# Target audio bitrate (128kbps)
TARGET_AUDIO_BITRATE = os.environ.get("TARGET_AUDIO_BITRATE", "128k")

# Development mode settings
DEV_MODE = os.environ.get("DEV_MODE", "False").lower() in ['true', '1', 't']  # Set to True to keep temp files in current directory
# Use system temp directory or current directory based on dev mode
TEMP_DIR = Path(os.environ.get("TEMP_DIR", './tmp')) if DEV_MODE else Path('/tmp')
CLEANUP_TEMP_FILES = not DEV_MODE

# Path to base watermark image (without text)
BASE_WATERMARK_PATH = os.environ.get("BASE_WATERMARK_PATH", str(Path(__file__).parent / "watermark.png"))

@timing_decorator
def create_outro(work_dir: Path, video_info: Dict, watermark_path: str) -> Path:
    video_stream = next(s for s in video_info['streams'] if s['codec_type'] == 'video')
    width = int(video_stream['width'])
    height = int(video_stream['height'])
    
    # Get video encoding parameters from input video
    pix_fmt = video_stream.get('pix_fmt', 'yuv420p')
    video_codec = video_stream.get('codec_name', 'libx264')
    video_bitrate = video_stream.get('bit_rate')
    framerate = eval(video_stream.get('r_frame_rate', '30/1'))  # Convert fraction string to float
    
    # Get audio parameters
    audio_stream = next((s for s in video_info['streams'] if s['codec_type'] == 'audio'), None)
    audio_codec = audio_stream.get('codec_name', 'aac') if audio_stream else 'aac'
    audio_bitrate = audio_stream.get('bit_rate') if audio_stream else '128k'
    sample_rate = audio_stream.get('sample_rate', '44100') if audio_stream else '44100'
    
    # Create black background video
    outro_path = work_dir / "outro.mp4"
    ffmpeg_args = [
        '-f', 'lavfi',
        '-i', f'color=c=black:s={width}x{height}:r={framerate}:d={OUTRO_DURATION}',
        '-f', 'lavfi',
        '-i', f'anullsrc=channel_layout=stereo:sample_rate={sample_rate}:d={OUTRO_DURATION}'
    ]
    
    # Add encoding parameters
    ffmpeg_args.extend([
        '-c:v', video_codec,
        '-c:a', audio_codec,
        '-pix_fmt', pix_fmt,
        '-shortest',
        '-preset', FFMPEG_PRESET
    ])
    
    # Add bitrate if available
    if video_bitrate:
        ffmpeg_args.extend(['-b:v', str(video_bitrate)])
    if audio_bitrate:
        ffmpeg_args.extend(['-b:a', str(audio_bitrate)])
    
    ffmpeg_args.extend(['-y', str(outro_path)])
    
    run_command('ffmpeg', ffmpeg_args)
    
    # Get output video info
    output_video_info = get_video_info(OUTRO_VIDEO_PATH)
    output_stream = next(s for s in output_video_info['streams'] if s['codec_type'] == 'video')
    output_width = int(output_stream['width'])
    output_height = int(output_stream['height'])
    
    # Calculate scaling to fit output video in the center while maintaining aspect ratio
    scale_factor = min(width * 0.8 / output_width, height * 0.8 / output_height)
    new_width = int(output_width * scale_factor)
    new_height = int(output_height * scale_factor)
    
    # Add output video to the center of black background
    outro_with_video = work_dir / "outro_with_video.mp4"
    run_command(
        'ffmpeg',
        [
            '-i', str(outro_path),
            '-i', OUTRO_VIDEO_PATH,
            '-filter_complex',
            f'[1:v]scale={new_width}:{new_height}[output];[0:v][output]overlay=(main_w-overlay_w)/2:(main_h-overlay_h)/2',
            '-c:v', video_codec,
            '-c:a', audio_codec,
            '-pix_fmt', pix_fmt,
            '-preset', FFMPEG_PRESET,
            ] + 
            (['-b:v', str(video_bitrate)] if video_bitrate else []) +
            (['-b:a', str(audio_bitrate)] if audio_bitrate else []) +
            ['-y', str(outro_with_video)]
    )
    
    outro_path.unlink()
    return outro_with_video

@timing_decorator
def add_watermark(input_file: Path, watermark_path: str, output_file: Path, watermark_height: int) -> None:
    """Add watermark to video with random position changes every 5 seconds"""
    logger.info("watermark height %d",watermark_height)
    run_command(
        'ffmpeg',
        [
            '-i', str(input_file),
            '-i', watermark_path,
            '-filter_complex',f"[1:v]scale=-1:{watermark_height},format=rgba,colorchannelmixer=aa={WATERMARK_OPACITY}[wm];[0:v][wm]overlay=x='if(lt(mod(t\,16)\,8)\,W-w-W*10/100\,W*10/100)':y='if(lt(mod(t+4\,16)\,8)\,H-h-H*5/100\,H*5/100)'",
            '-c:v', 'libx264',
            '-preset', FFMPEG_PRESET,
            '-crf', '23',
            '-c:a', 'copy',
            str(output_file)
        ]
    )

@timing_decorator
def concat_videos(video_file: Path, outro_file: Path, output_file: Path) -> None:
    """Concatenate main video with outro"""
    # 使用concat选项直接合并视频
    concat_file = TEMP_DIR / "concat_list.txt"
    
    # 创建一个包含要合并文件的文本文件
    with open(concat_file, 'w') as f:
        f.write(f"file '{video_file}'\n")
        f.write(f"file '{outro_file}'\n")
    
    run_command(
        'ffmpeg',
        [
            '-f', 'concat',
            '-safe', '0',
            '-i', str(concat_file),
            '-c', 'copy',
            str(output_file)
        ]
    )
    concat_file.unlink()  # 删除临时文件

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
    
    # Replace image-cdn.flowgpt.com with flow-app-media.s3.amazonaws.com
    if 'image-cdn.flowgpt.com' in video_url:
        video_url = video_url.replace('image-cdn.flowgpt.com', 'flow-app-media.s3.amazonaws.com')
        
    return unquote(video_url)

def get_video_hash(video_url: str) -> str:
    """Calculate a hash of the video URL to use as a unique identifier"""
    return hashlib.md5(video_url.encode()).hexdigest()

@timing_decorator
def generate_watermark(username: str, output_path: str) -> str:
    """Generate a customized watermark with the given username"""
    logger.info(f"Generating watermark for user: {username}")
    
    # Open the base watermark image
    base_img = Image.open(BASE_WATERMARK_PATH).convert('RGBA')
    
    # Create draw object for text size calculation
    temp_img = Image.new('RGBA', (1, 1), (0, 0, 0, 0))
    draw = ImageDraw.Draw(temp_img)
    
    # Try to use Arial font, fallback to default if not available
    try:
        font = ImageFont.truetype("Arial", 24)
    except:
        font = ImageFont.load_default()
    
    # Add text
    text = f"@{username}"
    
    # Get text size
    text_bbox = draw.textbbox((0, 0), text, font=font)
    text_width = text_bbox[2] - text_bbox[0]
    text_height = text_bbox[3] - text_bbox[1]
    
    # Calculate required width and padding
    padding_vertical = 40  # Space for text vertically
    padding_horizontal = 40  # Minimum padding on sides
    required_width = max(base_img.width, text_width + padding_horizontal * 2)
    
    # Create a new image with calculated dimensions
    new_img = Image.new('RGBA', (required_width, base_img.height + padding_vertical), (0, 0, 0, 0))
    
    # Calculate position to center the original watermark
    watermark_x = (required_width - base_img.width) // 2
    new_img.paste(base_img, (watermark_x, 0))
    
    # Create draw object for final text
    draw = ImageDraw.Draw(new_img)
    
    # Calculate text position (centered horizontally, at the bottom with padding)
    x = (required_width - text_width) // 2
    y = base_img.height + (padding_vertical - text_height) // 2
    
    # Add black outline
    outline_color = "black"
    outline_width = 2
    for offset_x in range(-outline_width, outline_width + 1):
        for offset_y in range(-outline_width, outline_width + 1):
            draw.text((x + offset_x, y + offset_y), text, font=font, fill=outline_color)
    
    # Add white text on top
    draw.text((x, y), text, font=font, fill="white")
    
    # Save the result
    new_img.save(output_path, "PNG")
    return output_path

@timing_decorator
def preprocess_video(input_file: Path, output_file: Path) -> None:
    """Preprocess video by reducing resolution and bitrate"""
    logger.info("Preprocessing video to reduce size")
    run_command(
        'ffmpeg',
        [
            '-i', str(input_file),
            '-vf', f'scale=-2:{TARGET_HEIGHT}',  # Scale to target height while maintaining aspect ratio
            '-c:v', 'libx264',
            '-preset', FFMPEG_PRESET,
            '-b:v', TARGET_VIDEO_BITRATE,
            '-c:a', 'aac',
            '-b:a', TARGET_AUDIO_BITRATE,
            '-y',
            str(output_file)
        ]
    )

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
            
            # Get username from request body
            body = event['body']
            if not event.get('isBase64Encoded', True):
                body = json.loads(body)
            username = body.get('username', 'emochi')  # Default to 'flowgpt' if not provided
            
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
        preprocessed_file = TEMP_DIR / f"{file_id}_preprocessed{ext}"
        watermarked_file = TEMP_DIR / f"{file_id}_watermarked{ext}"
        final_output_file = TEMP_DIR / f"{file_id}_with_outro{ext}"
        output_key = f"{uuid.uuid4()}-{OUTPUT_PREFIX}/{video_hash}_with_outro{ext}"
        
        temp_files.extend([input_file, preprocessed_file, watermarked_file, final_output_file])
        
        # Download input video
        _, processing_times['download'] = download_input_video(input_bucket, key, str(input_file))
        
        # Preprocess video
        _, processing_times['preprocess'] = preprocess_video(input_file, preprocessed_file)
        
        # Get video info and calculate watermark size
        video_info = get_video_info(str(preprocessed_file))
        video_stream = next(s for s in video_info['streams'] if s['codec_type'] == 'video')
        height = int(video_stream['height'])
        watermark_height = height * WATERMARK_SIZE_PERCENT // 100
        logger.debug("watermark height",watermark_height)
        
        # Generate custom watermark
        custom_watermark_path = str(TEMP_DIR / f"{file_id}_watermark.png")
        generate_watermark(username, custom_watermark_path)
        temp_files.append(Path(custom_watermark_path))
        
        # Add watermark
        _, processing_times['watermark'] = add_watermark(preprocessed_file, custom_watermark_path, watermarked_file, watermark_height)
        
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
        "isBase64Encoded": False,
        "username": "emochi"
    }
    logger.info(f"Using watermark file: {WATERMARK_PATH}")
    logger.info(f"Watermark file exists: {Path(WATERMARK_PATH).exists()}")
    res = lambda_handler(test_event, None)
    print(res)
    # add_watermark("tmp/3658c8ba29b2176c12b113211b86b86d_input.mp4","./watermark.png","last.mp4",)
