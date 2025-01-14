import subprocess
import logging
import json
import shutil
from typing import List, Optional, Dict

logger = logging.getLogger(__name__)

def check_dependencies() -> None:
    """Check if required dependencies (ffmpeg and imagemagick) are installed"""
    missing_deps = []
    
    # Check ffmpeg
    if shutil.which('ffmpeg') is None:
        missing_deps.append('ffmpeg')
        
    # Check imagemagick (magick command)
    if shutil.which('magick') is None:
        missing_deps.append('imagemagick')
    
    if missing_deps:
        raise RuntimeError(f"Missing required dependencies: {', '.join(missing_deps)}. Please install them first.")
    
    # Log versions for debugging
    try:
        ffmpeg_version = subprocess.check_output(['ffmpeg', '-version'], stderr=subprocess.STDOUT).decode().split('\n')[0]
        magick_version = subprocess.check_output(['magick', '-version'], stderr=subprocess.STDOUT).decode().split('\n')[0]
        logger.info(f"FFmpeg version: {ffmpeg_version}")
        logger.info(f"ImageMagick version: {magick_version}")
    except subprocess.CalledProcessError as e:
        logger.warning(f"Failed to get version info: {e}")

def get_video_info(video_path: str) -> Dict:
    """
    使用ffprobe获取视频信息
    """
    logger.info(f"Getting video info for: {video_path}")
    try:
        result = subprocess.run(
            [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                '-show_streams',
                video_path
            ],
            capture_output=True,
            text=True,
            check=True
        )
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        error_message = f"Failed to get video info: {e.stderr}"
        logger.error(error_message)
        raise RuntimeError(error_message)

def run_command(command: str, args: List[str], env: Optional[Dict] = None, cwd: Optional[str] = None) -> str:
    """
    Execute a command with arguments and return the output
    """
    logger.info(f"Executing: {command} {' '.join(args)}")
    
    try:
        result = subprocess.run(
            [command, *args],
            env=env,
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True
        )
        
        if result.stderr:
            logger.error(result.stderr)
            
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        error_message = f"Command failed with exit code {e.returncode}"
        if e.stderr:
            error_message += f": {e.stderr}"
        logger.error(error_message)
        raise RuntimeError(error_message)
    except Exception as e:
        logger.error(f"Error executing command: {str(e)}")
        raise 