import os
import re
from datetime import datetime

def get_next_episode_number(folder_path: str, year: int) -> str:
    """
    Get the next episode number for the given year by scanning existing files
    and finding the highest episode number.
    
    Args:
        folder_path: The folder to scan for existing episodes
        year: The year to look for (e.g. 2025)
        
    Returns:
        A two-digit string episode number (e.g. "01", "02", etc)
    """
    pattern = rf"S{year}E(\d+)"
    max_episode = 0
    
    # Walk through all files in the directory
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".strm"):
                match = re.search(pattern, file)
                if match:
                    episode_num = int(match.group(1))
                    max_episode = max(max_episode, episode_num)
    
    # Return next episode number as 2-digit string
    return f"{max_episode + 1:02d}"

def get_episode_number_from_date(upload_date: str, use_mmdd: bool = False) -> str:
    """
    Get episode number from upload date.
    
    Args:
        upload_date: Upload date in format YYYY-MM-DD or YYYYMMDD
        use_mmdd: If True, use MMDD format (e.g. "0315" for March 15)
                  If False, return "01" for sequential numbering
        
    Returns:
        Episode number as string (e.g. "0315" or "01")
    """
    if not use_mmdd:
        return "01"  # Will be replaced by sequential numbering
    
    try:
        # Parse date - handle both YYYY-MM-DD and YYYYMMDD formats
        if '-' in upload_date:
            date_obj = datetime.strptime(upload_date, '%Y-%m-%d')
        else:
            date_obj = datetime.strptime(upload_date, '%Y%m%d')
        
        # Return MMDD format (4 digits)
        return f"{date_obj.month:02d}{date_obj.day:02d}"
    except:
        return "0101"  # Fallback to January 1st

def format_episode_title(title: str, folder_path: str, upload_date: str = None, use_mmdd: bool = False) -> str:
    """
    Format a title with the season/episode prefix
    
    Args:
        title: The original title
        folder_path: Path to check for existing episode numbers
        upload_date: Upload date (YYYY-MM-DD or YYYYMMDD) - required if use_mmdd is True
        use_mmdd: If True, use MMDD as episode number instead of sequential
        
    Returns:
        The formatted title with S{year}E{XX} prefix
    """
    current_year = datetime.now().year
    
    if use_mmdd and upload_date:
        # Use MMDD format from upload date
        episode_number = get_episode_number_from_date(upload_date, use_mmdd=True)
        # Extract year from upload_date
        try:
            if '-' in upload_date:
                year = datetime.strptime(upload_date, '%Y-%m-%d').year
            else:
                year = datetime.strptime(upload_date, '%Y%m%d').year
        except:
            year = current_year
        return f"S{year}E{episode_number} - {title}"
    else:
        # Use sequential numbering
        next_episode = get_next_episode_number(folder_path, current_year)
        return f"S{current_year}E{next_episode} - {title}"
