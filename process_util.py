import subprocess
import logging
import json
from typing import List, Optional, Dict

logger = logging.getLogger(__name__)

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