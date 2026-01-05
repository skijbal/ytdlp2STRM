import os
import json
import time
import platform
import subprocess
import requests
import html
import re
from datetime import datetime
from cachetools import TTLCache
from utils.episode_numbering import format_episode_title
from utils.sanitize import sanitize
from flask import stream_with_context, Response, send_file, redirect, abort, request
from clases.config import config as c
from clases.worker import worker as w
from clases.folders import folders as f
from clases.nfo import nfo as n
from clases.log import log as l
from clases.jellyfin_notifier.jellyfin_notifier import JellyfinNotifier

recent_requests = TTLCache(maxsize=200, ttl=30)
video_info_cache = TTLCache(maxsize=1000, ttl=60 * 60)  # 1 hour cache for original language probing

## -- LOAD CONFIG AND CHANNELS FILES
ytdlp2strm_config = c.config(
    './config/config.json'
).get_config()

config = c.config(
    './plugins/youtube/config.json'
).get_config()

channels = c.config(
    config["channels_list_file"]
).get_channels()

media_folder = config["strm_output_folder"]
days_dateafter = config["days_dateafter"]
videos_limit = config["videos_limit"]
try:
    cookies = config["cookies"]
    cookie_value = config["cookie_value"]
except Exception:
    cookies = 'cookies-from-browser'
    cookie_value = 'chrome'

try:
    lang = config["lang"]
except Exception:
    lang = 'en'

try:
    episode_format = config["episode_format"]
except Exception:
    episode_format = 'sequential'

source_platform = "youtube"
host = ytdlp2strm_config['ytdlp2strm_host']
port = ytdlp2strm_config['ytdlp2strm_port']

SECRET_KEY = os.environ.get('AM_I_IN_A_DOCKER_CONTAINER', False)
DOCKER_PORT = os.environ.get('DOCKER_PORT', False)
if SECRET_KEY:
    port = DOCKER_PORT

if 'proxy' in config:
    proxy = config['proxy']
    proxy_url = config['proxy_url']
else:
    proxy = False
    proxy_url = ""

## -- END


# =============================================================================
# Language preference helpers:
# 1) Original audio language first
# 2) English
# 3) Best available
# =============================================================================

def _normalize_lang(code):
    """Return primary language subtag (e.g. 'en' from 'en-US'), lowercased."""
    if not code or not isinstance(code, str):
        return None
    code = code.strip().lower().replace("_", "-")
    if not code:
        return None
    return re.split(r"[-]", code)[0] or None


def get_original_audio_lang(info):
    """
    Best-effort: yt-dlp may expose original language in different fields depending on extractor/version.
    Normalize to a primary language code like 'en', 'ja', 'es', etc.
    """
    if not isinstance(info, dict):
        return None
    for k in ("original_language", "original_language_code", "language"):
        v = info.get(k)
        norm = _normalize_lang(v)
        if norm:
            return norm
    return None


def fmt_best_single(orig_lang):
    """For direct URL fallback cases (single stream URL)."""
    orig_lang = _normalize_lang(orig_lang)
    if orig_lang:
        return f"best[language^={orig_lang}]/best[language^=en]/best"
    return "best[language^=en]/best"


def fmt_best_audio(orig_lang):
    """For audio-only selection."""
    orig_lang = _normalize_lang(orig_lang)
    if orig_lang:
        return f"bestaudio[language^={orig_lang}]/bestaudio[language^=en]/bestaudio"
    return "bestaudio[language^=en]/bestaudio"


def fmt_best_av(orig_lang):
    """For best video + preferred audio, then fall back."""
    orig_lang = _normalize_lang(orig_lang)
    if orig_lang:
        return (
            f"bestvideo*+bestaudio[language^={orig_lang}]"
            f"/bestvideo*+bestaudio[language^=en]"
            f"/bestvideo*+bestaudio"
            f"/best"
        )
    return "bestvideo*+bestaudio[language^=en]/bestvideo*+bestaudio/best"


def fetch_info_json_for_video(youtube_id):
    """
    Lightweight info probe to learn original language for format preference.
    youtube_id may be raw ID or full URL.
    Cached for performance.
    """
    cache_key = youtube_id
    if cache_key in video_info_cache:
        return video_info_cache[cache_key]

    url = youtube_id
    if not isinstance(url, str):
        return None
    if not url.startswith("http"):
        url = f"https://www.youtube.com/watch?v={youtube_id}"

    cmd = [
        "yt-dlp",
        "-j",
        "--no-warnings",
        "--extractor-args",
        "youtube:player-client=default,web_safari",
        url,
    ]
    Youtube().set_cookies(cmd)
    Youtube().set_proxy(cmd)

    try:
        out = w.worker(cmd).output()
        info = json.loads(out) if out else None
        if info is not None:
            video_info_cache[cache_key] = info
        return info
    except Exception:
        return None


class Youtube:
    def __init__(self, channel=None):
        self.channel = channel
        self.channel_url = None
        self.channel_name = None
        self.channel_description = None
        self.channel_poster = None
        self.channel_landscape = None

    def get_results(self):
        if 'extractaudio-' in self.channel:
            islist = False
            self.channel_url = self.channel.replace(
                'extractaudio-',
                ''
            )
            if 'list-' in self.channel:
                islist = True
                self.channel_url = self.channel.replace(
                    'list-',
                    ''
                )
                if not 'www.youtube' in self.channel_url:
                    self.channel_url = f'https://www.youtube.com/playlist?list={self.channel_url}'
            else:
                # Normalize URL - avoid double https://
                if self.channel_url.startswith('http'):
                    # Already a full URL, use as-is
                    pass
                elif not 'www.youtube' in self.channel_url:
                    self.channel_url = f'https://www.youtube.com/{self.channel_url}'

            self.channel_name = self.get_channel_name()
            self.channel_description = self.get_channel_description() if not islist else f'Playlist {self.channel_name}'
            thumbs = self.get_channel_images()
            self.channel_poster = thumbs['poster']
            self.channel_landscape = thumbs['landscape']

            return self.get_channel_audios() if not islist else self.get_list_audios()

        elif 'keyword' in self.channel:
            return self.get_keyword_videos()

        elif 'list' in self.channel:
            self.channel_url = self.channel.replace(
                'list-',
                ''
            )
            if not 'www.youtube' in self.channel_url:
                self.channel_url = f'https://www.youtube.com/playlist?list={self.channel_url}'

            self.channel_name = self.get_channel_name()
            self.channel_description = f'Playlist {self.channel_name}'
            thumbs = self.get_channel_images()
            self.channel_poster = thumbs['poster']
            self.channel_landscape = thumbs['landscape']
            return self.get_list_videos()

        else:
            # Normalize URL - avoid double https://
            if self.channel.startswith('http'):
                # Already a full URL, use as-is
                self.channel_url = self.channel
            elif not 'www.youtube' in self.channel:
                self.channel_url = f'https://www.youtube.com/{self.channel}'
            else:
                self.channel_url = self.channel

            self.channel_name = self.get_channel_name()
            self.channel_description = self.get_channel_description()
            thumbs = self.get_channel_images()
            self.channel_poster = thumbs['poster']
            self.channel_landscape = thumbs['landscape']
            return self.get_channel_videos()

    def get_list_videos(self):
        command = [
            'yt-dlp',
            '--compat-options', 'no-youtube-channel-redirect',
            '--compat-options', 'no-youtube-unavailable-videos',
            '--playlist-start', '1',
            '--playlist-end', str(videos_limit),
            '--no-warning',
            '--dump-json',
            self.channel_url
        ]
        self.set_cookies(command)
        self.set_language(command)
        result = w.worker(command).output()
        videos = []
        for line in result.split('\n'):
            if line.strip():
                data = json.loads(line)

                video = {
                    'id': data.get('id'),
                    'title': data.get('title'),
                    'upload_date': data.get('upload_date'),
                    'thumbnail': data.get('thumbnail'),
                    'description': data.get('description'),
                    'channel_id': self.channel_url.split('list=')[1],
                    'uploader_id': sanitize(self.channel_name)
                }
                videos.append(video)

        return videos

    def get_keyword_videos(self):
        keyword = self.channel.split('-')[1]
        command = [
            'yt-dlp',
            '-f', 'best', 'ytsearch:["{}"]'.format(keyword),
            '--compat-options', 'no-youtube-channel-redirect',
            '--compat-options', 'no-youtube-unavailable-videos',
            '--playlist-start', '1',
            '--playlist-end', videos_limit,
            '--no-warning',
            '--dump-json'
        ]
        self.set_cookies(command)
        self.set_language(command)

        if config['days_dateafter'] == "0":
            command.pop(8)
            command.pop(8)

        result = w.worker(command).output()
        videos = []
        for line in result.split('\n'):
            if line.strip():
                data = json.loads(line)

                video = {
                    'id': data.get('id'),
                    'title': data.get('title'),
                    'upload_date': data.get('upload_date'),
                    'thumbnail': data.get('thumbnail'),
                    'description': data.get('description'),
                    'channel_id': data.get('channel_id'),
                    'uploader_id': data.get('uploader_id')
                }
                videos.append(video)

        return videos

    def get_keyword_audios(self):
        keyword = self.channel.split('-')[1]
        command = [
            'yt-dlp',
            '-f', 'best', 'ytsearch10:["{}"]'.format(keyword),
            '--compat-options', 'no-youtube-channel-redirect',
            '--compat-options', 'no-youtube-unavailable-videos',
            '--playlist-start', '1',
            '--playlist-end', videos_limit,
            '--no-warning',
            '--dump-json'
        ]
        self.set_cookies(command)
        self.set_language(command)

        if config['days_dateafter'] == "0":
            command.pop(8)
            command.pop(8)

        result = w.worker(command).output()
        videos = []
        for line in result.split('\n'):
            if line.strip():
                data = json.loads(line)

                video = {
                    'id': f"{data.get('id')}-audio",
                    'title': data.get('title'),
                    'upload_date': data.get('upload_date'),
                    'thumbnail': data.get('thumbnail'),
                    'description': data.get('description'),
                    'channel_id': data.get('channel_id'),
                    'uploader_id': data.get('uploader_id')
                }
                videos.append(video)

        return videos

    def get_channel_audios(self):
        cu = self.channel

        if not '/streams' in self.channel:
            cu = f'{self.channel_url}/videos'

        command = [
            'yt-dlp',
            '--compat-options', 'no-youtube-channel-redirect',
            '--compat-options', 'no-youtube-unavailable-videos',
            '--dateafter', f"today-{days_dateafter}days",
            '--playlist-start', '1',
            '--playlist-end', str(videos_limit),
            '--no-warning',
            '--dump-json',
            f'{cu}'
        ]
        self.set_cookies(command)
        self.set_language(command)

        result = w.worker(command).output()
        videos = []
        for line in result.split('\n'):
            if line.strip():
                data = json.loads(line)
                video = {
                    'id': f"{data.get('id')}-audio",
                    'title': data.get('title'),
                    'upload_date': data.get('upload_date'),
                    'thumbnail': data.get('thumbnail'),
                    'description': data.get('description'),
                    'channel_id': data.get('channel_id'),
                    'uploader_id': data.get('uploader_id')
                }
                videos.append(video)

        return videos

    def get_list_audios(self):
        command = [
            'yt-dlp',
            '--compat-options', 'no-youtube-channel-redirect',
            '--compat-options', 'no-youtube-unavailable-videos',
            '--playlist-start', '1',
            '--playlist-end', str(videos_limit),
            '--no-warning',
            '--dump-json',
            self.channel_url
        ]
        self.set_cookies(command)
        self.set_language(command)
        result = w.worker(command).output()
        videos = []
        for line in result.split('\n'):
            if line.strip():
                data = json.loads(line)

                video = {
                    'id': f"{data.get('id')}-audio",
                    'title': data.get('title'),
                    'upload_date': data.get('upload_date'),
                    'thumbnail': data.get('thumbnail'),
                    'description': data.get('description'),
                    'channel_id': self.channel_url.split('list=')[1],
                    'uploader_id': sanitize(self.channel_name)
                }
                videos.append(video)

        return videos

    def get_channel_videos(self):
        cu = self.channel

        if not '/streams' in self.channel:
            cu = f'{self.channel}/videos'

        command = [
            'yt-dlp',
            '--compat-options', 'no-youtube-channel-redirect',
            '--compat-options', 'no-youtube-unavailable-videos',
            '--dateafter', f"today-{days_dateafter}days",
            '--playlist-start', '1',
            '--playlist-end', str(videos_limit),
            '--no-warning',
            '--dump-json',
            f'{cu}'
        ]
        self.set_cookies(command)
        self.set_language(command)
        result = w.worker(command).output()
        videos = []
        for line in result.split('\n'):
            if line.strip():
                data = json.loads(line)
                video = {
                    'id': data.get('id'),
                    'title': data.get('title'),
                    'upload_date': data.get('upload_date'),
                    'thumbnail': data.get('thumbnail'),
                    'description': data.get('description'),
                    'channel_id': data.get('channel_id'),
                    'uploader_id': data.get('uploader_id')
                }
                videos.append(video)

        return videos

    def get_channel_name(self):
        # get channel or playlist name
        if 'playlist' in self.channel_url:
            command = ['yt-dlp',
                       '--compat-options', 'no-youtube-unavailable-videos',
                       '--print', '%(playlist_title)s',
                       '--playlist-items', '1',
                       '--restrict-filenames',
                       '--ignore-errors',
                       '--no-warnings',
                       '--compat-options', 'no-youtube-channel-redirect',
                       '--no-warnings',
                       f'{self.channel_url}'
                       ]
        else:
            # Use uploader (friendly name) instead of channel (@-name)
            command = ['yt-dlp',
                       '--compat-options', 'no-youtube-unavailable-videos',
                       '--print', '%(uploader)s',
                       '--restrict-filenames',
                       '--ignore-errors',
                       '--no-warnings',
                       '--playlist-items', '1',
                       '--compat-options', 'no-youtube-channel-redirect',
                       f'{self.channel_url}'
                       ]
        self.set_cookies(command)
        self.set_language(command)
        self.set_proxy(command)
        channel_name = w.worker(command).output().strip().replace('"', '')

        # If uploader is empty, NA, or literally "channel", try channel field
        if not channel_name or channel_name == 'NA' or channel_name.lower() == 'channel':
            command = ['yt-dlp',
                       '--compat-options', 'no-youtube-unavailable-videos',
                       '--print', '%(channel)s',
                       '--restrict-filenames',
                       '--ignore-errors',
                       '--no-warnings',
                       '--playlist-items', '1',
                       '--compat-options', 'no-youtube-channel-redirect',
                       f'{self.channel_url}'
                       ]
            self.set_cookies(command)
            self.set_language(command)
            self.set_proxy(command)
            channel_name = w.worker(command).output().strip().replace('"', '')

        # Final fallback: use URL
        if not channel_name or channel_name == 'NA':
            channel_name = self.channel_url.split('/')[-1]

        self.channel_name = channel_name
        return sanitize(self.channel_name)

    def get_channel_description(self):
        # get description
        if platform.system() == "Linux":
            command = [
                'yt-dlp',
                self.channel_url,
                '--write-description',
                '--playlist-items', '0',
                '--output', '"{}/{}.description"'.format(
                    media_folder,
                    sanitize(self.channel_name)
                )
            ]
            self.set_cookies(command)
            self.set_language(command)
            self.set_proxy(command)
            command = (
                    command
                    + [
                        '>',
                        '/dev/null',
                        '2>&1',
                        '&&',
                        'cat',
                        '"{}/{}.description"'.format(
                            media_folder,
                            sanitize(
                                self.channel_name
                            )
                        )
                    ]
            )

            self.channel_description = w.worker(command).shell()
            try:
                os.remove("{}/{}.description".format(media_folder, sanitize(self.channel_name)))
            except Exception:
                pass
        else:
            command = [
                'yt-dlp',
                '--write-description',
                '--playlist-items', '0',
                '--output', '"{}/{}.description"'.format(
                    media_folder,
                    sanitize(
                        self.channel_name
                    )
                ),
                self.channel_url,
            ]
            self.set_cookies(command)
            self.set_language(command)
            self.set_proxy(command)
            command = (
                    command
                    + [
                        '>',
                        'nul',
                        '2>&1',
                        '&&',
                        'more',
                        '"{}/{}.description"'.format(
                            media_folder,
                            sanitize(
                                self.channel_name
                            )
                        )
                    ]
            )

            try:
                self.channel_description = w.worker(command).shell()
            except Exception:
                d_file = open(
                    "{}/{}.description".format(
                        media_folder,
                        sanitize(
                            self.channel_name
                        )
                    ),
                    'r',
                    encoding='utf-8'
                )

                self.channel_description = d_file.read()
                d_file.close()

            try:
                os.remove(
                    "{}/{}.description".format(
                        media_folder,
                        sanitize(
                            self.channel_name
                        )
                    )
                )
            except Exception:
                pass

        return self.channel_description

    def get_channel_images(self):
        command = ['yt-dlp',
                   '--list-thumbnails',
                   '--restrict-filenames',
                   '--ignore-errors',
                   '--no-warnings',
                   '--playlist-items', '0',
                   self.channel_url
                   ]
        self.set_cookies(command)
        self.set_language(command)
        self.set_proxy(command)
        landscape = None
        poster = None

        try:
            output = w.worker(command).output()
            lines = output.split('\n')

            # Parse thumbnails looking for specific IDs
            for line in lines:
                line = line.strip()

                # Look for avatar_uncropped (poster)
                if 'avatar_uncropped' in line:
                    parts = line.split()
                    # URL is the last part
                    if len(parts) >= 4:
                        poster = parts[-1]

                # Look for banner_uncropped (landscape)
                if 'banner_uncropped' in line:
                    parts = line.split()
                    # URL is the last part
                    if len(parts) >= 4:
                        landscape = parts[-1]

        except Exception as e:
            l.log("youtube", f"Error getting channel images: {e}")
            pass

        return {
            "landscape": landscape,
            "poster": poster
        }

    def set_proxy(self, command):
        if proxy:
            if proxy_url != "":
                command.append('--proxy')
                command.append(proxy_url)

    def set_cookies(self, command):
        # Only add cookies if cookie_value is not empty
        if cookie_value and cookie_value.strip():
            command.append(f'--{cookies}')
            command.append(cookie_value)

    def set_language(self, command):
        """Configura el idioma para YouTube según la configuración"""
        extractor_args = []

        if lang and lang.strip():
            extractor_args.append(f'youtube:lang={lang}')

        # Agregar skip=authcheck para evitar errores con playlists que requieren autenticación
        extractor_args.append('youtubetab:skip=authcheck')

        if extractor_args:
            command.extend(['--extractor-args', ';'.join(extractor_args)])


def filter_and_modify_bandwidth(m3u8_content, original_lang=None):
    lines = m3u8_content.splitlines()

    highest_bandwidth = 0
    best_video_info = None
    best_video_url = None

    best_audio_line = None
    best_audio_score = -10**9
    fallback_audio_line = None

    original_lang = _normalize_lang(original_lang)

    def parse_attrs(ext_line: str) -> dict:
        attrs = {}
        try:
            raw = ext_line.split(":", 1)[1]
        except Exception:
            return attrs

        # Split on commas that are NOT inside quotes
        parts = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', raw)
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                attrs[k.strip().upper()] = v.strip().strip('"')
        return attrs

    for i in range(len(lines)):
        line = lines[i]

        if line.startswith("#EXT-X-STREAM-INF:"):
            info = line
            url = lines[i + 1]
            try:
                bandwidth = int(info.split("BANDWIDTH=")[1].split(",")[0])
            except Exception:
                bandwidth = 0

            if bandwidth > highest_bandwidth:
                highest_bandwidth = bandwidth
                best_video_info = info.replace(f"BANDWIDTH={bandwidth}", "BANDWIDTH=279001") if bandwidth else info
                best_video_url = url

        if line.startswith("#EXT-X-MEDIA:"):
            # Audio renditions live here in master playlists
            if "URI=" not in line:
                continue

            attrs = parse_attrs(line)

            # Keep only AUDIO (some manifests may omit TYPE)
            if attrs.get("TYPE", "").upper() not in ("AUDIO", ""):
                continue

            # Always have a fallback audio line in case preferred isn't present
            fallback_audio_line = fallback_audio_line or line

            langv = (attrs.get("LANGUAGE", "") or "").lower()
            namev = (attrs.get("NAME", "") or "").lower()

            # Scoring: prefer ORIGINAL, then English, then best available
            score = 0

            # 1) Original language first
            if original_lang and langv.startswith(original_lang):
                score += 400

            # 2) English next
            if langv.startswith("en"):
                score += 200
            if "english" in namev:
                score += 200

            # Tie-breakers
            if (attrs.get("DEFAULT", "") or "").upper() == "YES":
                score += 40
            if (attrs.get("AUTOSELECT", "") or "").upper() == "YES":
                score += 10

            # Keep the old heuristic as a minor tie-breaker
            if "234" in line or attrs.get("GROUP-ID", "") == "234":
                score += 3

            if score > best_audio_score:
                best_audio_score = score
                best_audio_line = line

    # Create the final M3U8 content
    final_m3u8 = "#EXTM3U\n#EXT-X-INDEPENDENT-SEGMENTS\n"

    if best_audio_line:
        final_m3u8 += f"{best_audio_line}\n"
    elif fallback_audio_line:
        final_m3u8 += f"{fallback_audio_line}\n"

    if best_video_info and best_video_url:
        final_m3u8 += f"{best_video_info}\n{best_video_url}\n"

    return final_m3u8


def clean_text(text):
    # Escapando caracteres que deben mantenerse pero asegurándote de que sean seguros
    text = html.escape(text)

    # Eliminar cualquier carácter no deseado usando expresiones regulares
    text = re.sub(r'[^\w\s\[\]\(\)\-\_\'\"\/\.\:\;\,]', '', text)

    return text


def video_id_exists_in_content(media_folder, video_id):
    for root, dirs, files in os.walk(media_folder):
        for file in files:
            if file.endswith(".strm"):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f2:
                    if video_id in f2.read():
                        return True
    return False


def to_strm(method):
    for youtube_channel in channels:
        yt = Youtube(youtube_channel)
        log_text = (" --------------- ")
        l.log("youtube", log_text)
        log_text = (f'Working {youtube_channel}...')
        l.log("youtube", log_text)
        videos = yt.get_results()
        channel_name = yt.channel_name
        channel_url = yt.channel_url
        channel_description = yt.channel_description

        log_text = (f'Channel URL: {channel_url}')
        l.log("youtube", log_text)
        log_text = (f'Channel Name: {channel_name}')
        l.log("youtube", log_text)
        log_text = (f'Channel Poster: {yt.channel_poster}')
        l.log("youtube", log_text)
        log_text = (f'Channel Landscape: {yt.channel_landscape}')
        l.log("youtube", log_text)
        log_text = ('Channel Description: ')
        l.log("youtube", log_text)
        log_text = (channel_description)
        l.log("youtube", log_text)

        if videos:
            log_text = (f'Videos detected: {len(videos)}')
            l.log("youtube", log_text)
            # Reverse video list so oldest videos get lower episode numbers
            videos.reverse()
            channel_nfo = False
            channel_folder_created = False

            # Get channel_id from first video to create channel folder and NFO
            first_video = videos[0]
            channel_id = first_video['channel_id']
            youtube_channel_folder = first_video['uploader_id'].replace('/user/', '@').replace('/streams', '')

            # Create channel folder
            channel_folder = sanitize(
                "{} [{}]".format(
                    youtube_channel_folder,
                    channel_id
                )
            )
            f.folders().make_clean_folder(
                "{}/{}".format(media_folder, channel_folder),
                False,
                ytdlp2strm_config
            )

            # Create channel NFO with correct images
            n.nfo(
                "tvshow",
                "{}/{}".format(media_folder, channel_folder),
                {
                    "title": channel_name,
                    "plot": channel_description.replace('\n', ' <br/>'),
                    "landscape": yt.channel_landscape,
                    "poster": yt.channel_poster,
                    "studio": "Youtube"
                }
            ).make_nfo()
            channel_nfo = True
            channel_folder_created = True

            for video in videos:
                video_id = video['id']
                channel_id = video['channel_id']
                video_name = video['title']
                thumbnail = video['thumbnail']
                description = video['description']
                date = datetime.strptime(video['upload_date'], '%Y%m%d')
                upload_date = date.strftime('%Y-%m-%d')
                year = date.year
                youtube_channel = video['uploader_id']
                youtube_channel_folder = youtube_channel.replace('/user/', '@').replace('/streams', '')
                file_content = f'http://{host}:{port}/{source_platform}/{method}/{video_id}'

                channel_folder = sanitize(
                    "{} [{}]".format(
                        youtube_channel_folder,
                        channel_id
                    )
                )

                # Create season folder based on video year
                season_folder = f"Season {year}"
                folder_full_path = "{}/{}/{}".format(media_folder, channel_folder, season_folder)

                # Format title with episode number
                use_mmdd = (episode_format.lower() == 'mmdd')
                formatted_title = format_episode_title(video_name, folder_full_path, upload_date, use_mmdd)

                file_path = "{}/{}/{}/{}.{}".format(
                    media_folder,
                    channel_folder,
                    season_folder,
                    sanitize(formatted_title),
                    "strm"
                )

                folder_path = "{}/{}".format(
                    media_folder,
                    sanitize(
                        "{} [{}]".format(
                            youtube_channel_folder,
                            channel_id
                        )
                    )
                )

                if video_id_exists_in_content(folder_path, video_id):
                    l.log("youtube", f'Video {video_id} already exists')
                    continue

                if not channel_folder_created:
                    f.folders().make_clean_folder(
                        "{}/{}".format(
                            media_folder,
                            sanitize(
                                "{} [{}]".format(
                                    youtube_channel_folder,
                                    channel_id
                                )
                            )
                        ),
                        False,
                        ytdlp2strm_config
                    )
                    channel_folder_created = True

                # Create season folder if it doesn't exist
                season_folder_path = "{}/{}/{}".format(media_folder, channel_folder, season_folder)
                if not os.path.exists(season_folder_path):
                    os.makedirs(season_folder_path, exist_ok=True)

                if channel_url is None:
                    channel_url = f'https://www.youtube.com/channel/{channel_id}'
                    channel = Youtube(channel_url)
                    images = channel.get_channel_images()
                    channel.channel_url = channel_url
                    channel_name = channel.get_channel_name()
                    channel_description = channel.get_channel_description()
                    channel_landscape = images['landscape']
                    channel_poster = images['poster']
                else:
                    channel_landscape = yt.channel_landscape
                    channel_poster = yt.channel_poster

                ## -- BUILD CHANNEL NFO FILE
                if not channel_nfo:
                    n.nfo(
                        "tvshow",
                        "{}/{}".format(
                            media_folder,
                            "{} [{}]".format(
                                youtube_channel,
                                channel_id
                            )
                        ),
                        {
                            "title": channel_name,
                            "plot": channel_description.replace('\n', ' <br/>'),
                            "landscape": channel_landscape,
                            "poster": channel_poster,
                            "studio": "Youtube"
                        }
                    ).make_nfo()
                    channel_nfo = True
                ## -- END

                ## -- BUILD VIDEO NFO FILE
                n.nfo(
                    "episode",
                    "{}/{}/{}".format(
                        media_folder,
                        "{} [{}]".format(
                            youtube_channel,
                            channel_id
                        ),
                        season_folder
                    ),
                    {
                        "item_name": sanitize(formatted_title),
                        "title": sanitize(formatted_title),
                        "upload_date": upload_date,
                        "year": year,
                        "plot": description.replace('\n', ' <br/>\n '),
                        "season": "1",
                        "episode": "",
                        "preview": thumbnail
                    }
                ).make_nfo()
                ## -- END

                if not os.path.isfile(file_path):
                    f.folders().write_file(
                        file_path,
                        file_content
                    )

            # Notify Jellyfin/Emby after processing all videos for this channel
            jellyfin_notifier = JellyfinNotifier(config)
            if jellyfin_notifier.enabled:
                jellyfin_notifier.notify_new_content(f"{media_folder}/{channel_folder}")
        else:
            log_text = (" no videos detected...")
            l.log("youtube", log_text)


def direct(youtube_id, remote_addr):
    current_time = time.time()
    cache_key = f"{remote_addr}_{youtube_id}"

    # Check if the request is already cached
    if cache_key not in recent_requests:
        log_text = f'[{remote_addr}] Playing {youtube_id}'
        l.log("youtube", log_text)
        recent_requests[cache_key] = current_time

    if '-audio' not in youtube_id:
        command = [
            'yt-dlp',
            '-j',
            '--no-warnings',
            '--extractor-args', 'youtube:player-client=default,web_safari',
            f'https://www.youtube.com/watch?v={youtube_id}'
        ]
        Youtube().set_cookies(command)
        Youtube().set_proxy(command)
        full_info_json_str = w.worker(command).output()
        m3u8_url = None
        original_lang = None
        try:
            full_info_json = json.loads(full_info_json_str)
            original_lang = get_original_audio_lang(full_info_json)

            for fmt in full_info_json.get("formats", []):
                if "manifest_url" in fmt.keys():
                    m3u8_url = fmt["manifest_url"]
                    break
        except Exception:
            pass

        if not m3u8_url:
            log_text = (
                'No manifest detected. Check your cookies config. \n'
                '* This video is age-restricted; some formats may be missing without authentication. '
                'Use --cookies-from-browser or --cookies for the authentication \n'
                '* Serving SD format. Please configure your cookies appropriately to access the manifest '
                'that serves the highest quality for this video'
            )
            l.log("youtube", log_text)
            command = [
                'yt-dlp',
                '-f', fmt_best_single(original_lang),
                '--get-url',
                '--no-warnings',
                f'https://www.youtube.com/watch?v={youtube_id}'
            ]
            Youtube().set_cookies(command)
            Youtube().set_proxy(command)
            sd_url = w.worker(command).output()
            return redirect(sd_url.strip(), 301)
        else:
            response = requests.get(m3u8_url)
            if response.status_code == 200:
                # Ensure UTF-8 encoding
                response.encoding = 'utf-8'
                m3u8_content = response.text
                filtered_content = filter_and_modify_bandwidth(m3u8_content, original_lang)

                # Create Response with headers optimized for VLC and media players
                flask_response = Response(filtered_content, mimetype='application/vnd.apple.mpegurl')
                flask_response.headers['Content-Type'] = 'application/vnd.apple.mpegurl; charset=utf-8'
                flask_response.headers['Content-Disposition'] = 'inline; filename="index.m3u8"'
                flask_response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
                flask_response.headers['Pragma'] = 'no-cache'
                flask_response.headers['Expires'] = '0'
                flask_response.headers['Accept-Ranges'] = 'bytes'
                flask_response.headers['Access-Control-Allow-Origin'] = '*'
                flask_response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
                flask_response.headers['Access-Control-Allow-Headers'] = 'Range'

                return flask_response
    else:
        s_youtube_id = youtube_id.split('-audio')[0]
        info = fetch_info_json_for_video(s_youtube_id) or {}
        orig = get_original_audio_lang(info)
        command = [
            'yt-dlp',
            '-f', fmt_best_audio(orig),
            '--get-url',
            '--no-warnings',
            f'https://www.youtube.com/watch?v={s_youtube_id}'
        ]
        Youtube().set_cookies(command)
        Youtube().set_proxy(command)
        audio_url = w.worker(command).output()
        return redirect(audio_url, 301)

    return "Manifest URL not found or failed to redirect.", 404


def bridge(youtube_id):
    raw_id = youtube_id.split('-audio')[0]
    s_youtube_id = f'https://www.youtube.com/watch?v={raw_id}'

    def generate():
        startTime = time.time()
        buffer = []
        sentBurst = False

        info = fetch_info_json_for_video(raw_id) or {}
        orig = get_original_audio_lang(info)
        fmt = fmt_best_audio(orig) if '-audio' in youtube_id else fmt_best_av(orig)

        command = ['yt-dlp', '--no-warnings', '-o', '-', '-f', fmt]

        if config.get("sponsorblock"):
            command += ['--sponsorblock-remove', config['sponsorblock_cats']]

        command += ['--restrict-filenames', s_youtube_id]

        Youtube().set_cookies(command)
        Youtube().set_language(command)
        Youtube().set_proxy(command)

        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        time.sleep(3)
        try:
            while True:
                chunk = process.stdout.read(1024)

                if not chunk:
                    break

                buffer.append(chunk)

                # Minimum buffer time, 3 seconds
                if sentBurst is False and time.time() > startTime + 3 and len(buffer) > 0:
                    sentBurst = True
                    for _ in range(0, len(buffer) - 2):
                        yield buffer.pop(0)
                elif time.time() > startTime + 3 and len(buffer) > 0:
                    yield buffer.pop(0)

                process.poll()
        finally:
            process.kill()

    return Response(
        stream_with_context(generate()),
        mimetype="video/mp4"
    )


def download(youtube_id):
    raw_id = youtube_id.split('-audio')[0]
    video_url = f'https://www.youtube.com/watch?v={raw_id}'
    current_dir = os.getcwd()

    # Ruta hacia la carpeta 'temp' dentro del directorio actual
    temp_dir = os.path.join(current_dir, 'temp')

    info = fetch_info_json_for_video(raw_id) or {}
    orig = get_original_audio_lang(info)

    if '-audio' in youtube_id:
        fmt = fmt_best_audio(orig)
    else:
        orig_norm = _normalize_lang(orig)
        if orig_norm:
            fmt = f"bv*+ba[language^={orig_norm}]/bv*+ba[language^=en]/bv*+ba/best"
        else:
            fmt = "bv*+ba[language^=en]/bv*+ba/best"

    command = ['yt-dlp', '-f', fmt, '-o', os.path.join(temp_dir, '%(title)s.%(ext)s')]

    if config.get("sponsorblock"):
        command += ['--sponsorblock-remove', config['sponsorblock_cats']]

    command += ['--restrict-filenames', video_url]

    Youtube().set_cookies(command)
    Youtube().set_language(command)
    Youtube().set_proxy(command)

    w.worker(command).call()

    filename_command = ['yt-dlp', '--print', 'filename', '--restrict-filenames', video_url]
    Youtube().set_cookies(filename_command)
    Youtube().set_language(filename_command)
    filename = w.worker(filename_command).output()

    return send_file(
        os.path.join(temp_dir, filename)
    )
