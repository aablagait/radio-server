"""
TEST
Личное онлайн-радио.

Два раздела:
- "/"        — живой эфир: один непрерывный поток MP3, все слушатели слышат
               один и тот же момент одновременно (настоящее радио).
- "/library" — показывает, какие треки будут играть следующими (по реальному
               времени воспроизведения), и даёт их скачать. Запускать треки
               вручную отсюда нельзя — только посмотреть очередь и скачать.

Запуск:
    pip install -r requirements.txt
    python main.py

Открой в браузере: http://<IP-этого-компьютера>:8000
"""

import asyncio
import mimetypes
import re
import subprocess
import time
import urllib.parse
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse

# ---------- НАСТРОЙКИ ----------
MUSIC_DIR = Path(__file__).parent / "music"
HOST = "0.0.0.0"          # слушать на всех интерфейсах, чтобы был доступен по IP
PORT = 8000
BITRATE = "192k"
EXTENSIONS = {".mp3", ".flac", ".wav", ".ogg", ".m4a", ".aac"}
UPCOMING_COUNT = 15        # сколько треков показывать в "Далее в эфире"
# --------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ffmpeg_process, broadcaster_task
    ffmpeg_process = start_ffmpeg()
    broadcaster_task = asyncio.create_task(broadcaster())
    yield
    if ffmpeg_process:
        ffmpeg_process.terminate()
    if broadcaster_task:
        broadcaster_task.cancel()


app = FastAPI(title="My Radio", lifespan=lifespan)

# каждый подключённый слушатель эфира = своя очередь байт-чанков,
# broadcaster рассылает в них одни и те же данные -> у всех один и тот же момент
listeners: set[asyncio.Queue] = set()

ffmpeg_process: subprocess.Popen | None = None
broadcaster_task: asyncio.Task | None = None

# для раздела "Далее в эфире": какой именно плейлист играет сейчас и когда
# начался текущий круг — по ним вычисляем, какой трек звучит прямо сейчас
current_cycle_tracks: list[Path] = []
current_cycle_start: float = 0.0
_duration_cache: dict[str, float] = {}   # путь -> длительность в секундах


def list_tracks() -> list[Path]:
    """Единый источник правды для порядка треков — используется и эфиром,
    и библиотекой, чтобы номера треков (id) были одинаковыми и стабильными."""
    return sorted(
        p for p in MUSIC_DIR.rglob("*")
        if p.suffix.lower() in EXTENSIONS
    )


def get_track_duration(path: Path) -> float:
    """Длительность трека в секундах через ffprobe, с кэшем по пути+mtime."""
    key = f"{path}:{path.stat().st_mtime}"
    if key in _duration_cache:
        return _duration_cache[key]
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "csv=p=0", str(path)],
            capture_output=True, text=True, timeout=10,
        )
        duration = float(out.stdout.strip())
    except Exception:
        duration = 0.0
    _duration_cache[key] = duration
    return duration


def build_playlist_file(tracks: list[Path]) -> Path:
    """Собирает playlist.txt для ffmpeg concat-демультиплексора."""
    playlist_path = MUSIC_DIR / "_playlist.txt"
    if not tracks:
        raise RuntimeError(
            f"В папке {MUSIC_DIR} нет аудиофайлов. "
            f"Добавь mp3/flac/wav/ogg/m4a и перезапусти сервер."
        )
    with open(playlist_path, "w", encoding="utf-8") as f:
        for track in tracks:
            # экранируем одинарные кавычки для ffmpeg concat-синтаксиса
            safe_path = str(track.resolve()).replace("'", "'\\''")
            f.write(f"file '{safe_path}'\n")
    return playlist_path


def start_ffmpeg() -> subprocess.Popen:
    tracks = list_tracks()
    playlist_path = build_playlist_file(tracks)

    global current_cycle_tracks, current_cycle_start
    current_cycle_tracks = tracks
    current_cycle_start = time.monotonic()

    cmd = [
        "ffmpeg",
        "-re",                    # читать с реальной скоростью воспроизведения
        "-f", "concat",
        "-safe", "0",
        "-i", str(playlist_path),
        "-vn",
        "-acodec", "libmp3lame",
        "-b:a", BITRATE,
        "-f", "mp3",
        "pipe:1",
    ]
    # ВАЖНО: -stream_loop -1 здесь намеренно не используется — ffmpeg не умеет
    # зацикливать -f concat демультиплексор (падает с "Operation not permitted"
    # при попытке перемотки в начало). Поэтому зацикливание сделано ниже,
    # на уровне Python: broadcaster() сам перезапускает ffmpeg по завершении.
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        bufsize=0,
    )


async def broadcaster():
    """Читает байты из ffmpeg и рассылает их всем активным слушателям эфира.

    Когда текущий ffmpeg-процесс доигрывает плейлист и завершается (EOF),
    сразу перезапускает новый — так получается бесконечное зацикливание
    "по кругу", плюс на каждом новом круге плейлист пересобирается заново,
    так что новые треки в music/ подхватятся сами.
    """
    global ffmpeg_process
    loop = asyncio.get_event_loop()
    while True:
        chunk = await loop.run_in_executor(None, ffmpeg_process.stdout.read, 4096)
        if not chunk:
            ffmpeg_process.wait()
            ffmpeg_process = start_ffmpeg()
            continue
        dead = []
        for q in listeners:
            try:
                q.put_nowait(chunk)
            except asyncio.QueueFull:
                dead.append(q)  # отстающего слушателя отключаем, а не тормозим всех
        for q in dead:
            listeners.discard(q)


@app.get("/stream")
async def stream(request: Request):
    q: asyncio.Queue = asyncio.Queue(maxsize=50)
    listeners.add(q)

    async def audio_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                chunk = await q.get()
                yield chunk
        finally:
            listeners.discard(q)

    return StreamingResponse(audio_generator(), media_type="audio/mpeg")


# ---------- БИБЛИОТЕКА: что играет сейчас / что дальше, + скачивание ----------

def get_now_playing_index() -> int | None:
    """Определяет индекс (в current_cycle_tracks) трека, который звучит
    прямо сейчас, исходя из реального времени с начала текущего круга —
    -re гарантирует, что кодирование идёт в темпе реального воспроизведения,
    поэтому это вычисление надёжно отражает то, что реально сейчас в эфире."""
    if not current_cycle_tracks:
        return None
    durations = [get_track_duration(t) for t in current_cycle_tracks]
    total = sum(durations)
    if total <= 0:
        return None
    elapsed = (time.monotonic() - current_cycle_start) % total
    acc = 0.0
    for i, d in enumerate(durations):
        if elapsed < acc + d:
            return i
        acc += d
    return len(current_cycle_tracks) - 1


def ranged_file_response(request: Request, file_path: Path, as_attachment: bool) -> StreamingResponse:
    """Отдаёт файл с поддержкой HTTP Range (для докачки больших файлов)."""
    file_size = file_path.stat().st_size
    content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"

    range_header = request.headers.get("range")
    start, end, status_code = 0, file_size - 1, 200
    if range_header:
        m = re.match(r"bytes=(\d*)-(\d*)", range_header)
        if m and (m.group(1) or m.group(2)):
            if m.group(1):
                start = int(m.group(1))
                end = int(m.group(2)) if m.group(2) else file_size - 1
            else:
                # суффиксный диапазон "bytes=-N" — последние N байт
                suffix_len = int(m.group(2))
                start = max(file_size - suffix_len, 0)
                end = file_size - 1
            status_code = 206

    chunk_size = end - start + 1

    def iterfile():
        with open(file_path, "rb") as f:
            f.seek(start)
            remaining = chunk_size
            while remaining > 0:
                data = f.read(min(1024 * 1024, remaining))
                if not data:
                    break
                remaining -= len(data)
                yield data

    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(chunk_size),
    }
    if as_attachment:
        # RFC 5987/6266: filename= должен быть latin-1/ascii (фолбэк для старых
        # клиентов), а настоящее имя (с кириллицей и т.п.) — в filename*=
        # как percent-encoded UTF-8, это поддерживают все современные браузеры.
        ascii_fallback = file_path.name.encode("ascii", errors="ignore").decode("ascii")
        if not ascii_fallback:
            ascii_fallback = f"track{file_path.suffix}"
        encoded_name = urllib.parse.quote(file_path.name)
        headers["Content-Disposition"] = (
            f"attachment; filename=\"{ascii_fallback}\"; filename*=UTF-8''{encoded_name}"
        )
    return StreamingResponse(
        iterfile(), status_code=status_code, headers=headers, media_type=content_type
    )


@app.get("/track/{track_id}")
async def get_track(track_id: int, request: Request):
    """Скачивание конкретного трека (не воспроизведение)."""
    tracks = list_tracks()
    if track_id < 0 or track_id >= len(tracks):
        return Response(status_code=404)
    return ranged_file_response(request, tracks[track_id], as_attachment=True)


@app.get("/library", response_class=HTMLResponse)
async def library():
    tracks = list_tracks()
    now_idx = None
    playing_idx = get_now_playing_index()
    if playing_idx is not None and playing_idx < len(current_cycle_tracks):
        try:
            now_idx = tracks.index(current_cycle_tracks[playing_idx])
        except ValueError:
            now_idx = None

    if now_idx is not None and tracks:
        n = len(tracks)
        upcoming_order = [(now_idx + 1 + i) % n for i in range(min(UPCOMING_COUNT, n))]
        now_playing_name = tracks[now_idx].stem
    else:
        upcoming_order = list(range(min(UPCOMING_COUNT, len(tracks))))
        now_playing_name = None

    rows = "".join(
        f'<li><span class="pos">{i + 1}</span>'
        f'<span class="name">{tracks[tid].stem}</span>'
        f'<a class="dl" href="/track/{tid}">Скачать</a></li>'
        for i, tid in enumerate(upcoming_order)
    )
    now_playing_html = (
        f'<div class="now-playing"><span class="dot"></span>Сейчас играет: {now_playing_name}</div>'
        if now_playing_name else ""
    )

    return f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <title>Далее в эфире</title>
        <style>
            body {{
                background: #0f0f10; color: #f2f2f2;
                font-family: -apple-system, sans-serif;
                max-width: 480px; margin: 0 auto; padding: 24px;
            }}
            nav a {{ color: #9aa0a6; text-decoration: none; margin-right: 16px; }}
            nav a.active {{ color: #f2f2f2; font-weight: 600; }}
            h1 {{ font-size: 20px; }}
            .now-playing {{
                background: #1a1a1c; padding: 10px 14px; border-radius: 8px;
                margin-bottom: 20px; font-size: 14px; color: #ccc;
            }}
            .dot {{
                width: 8px; height: 8px; border-radius: 50%; background: #ff4b4b;
                display: inline-block; margin-right: 8px; animation: pulse 1.5s infinite;
            }}
            @keyframes pulse {{ 0%,100% {{opacity:1;}} 50% {{opacity:0.3;}} }}
            ul {{ list-style: none; padding: 0; }}
            li {{
                display: flex; align-items: center; gap: 10px;
                background: #1a1a1c; padding: 10px 14px; margin-bottom: 6px;
                border-radius: 8px; font-size: 14px;
            }}
            .pos {{ color: #666; width: 20px; flex-shrink: 0; }}
            .name {{ flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
            .dl {{
                color: #9aa0a6; text-decoration: none; border: 1px solid #333;
                border-radius: 6px; padding: 4px 10px; font-size: 12px; flex-shrink: 0;
            }}
            .dl:hover {{ color: #f2f2f2; border-color: #666; }}
        </style>
    </head>
    <body>
        <nav>
            <a href="/">Эфир</a>
            <a href="/library" class="active">Далее в эфире</a>
        </nav>
        <h1>Далее в эфире</h1>
        {now_playing_html}
        <ul>{rows}</ul>
    </body>
    </html>
    """


# ---------- ГЛАВНАЯ (эфир) ----------

@app.get("/", response_class=HTMLResponse)
async def index():
    return """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <title>Моё радио</title>
        <style>
            body {
                background: #0f0f10;
                color: #f2f2f2;
                font-family: -apple-system, sans-serif;
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                height: 100vh;
                margin: 0;
            }
            nav { position: absolute; top: 24px; }
            nav a { color: #9aa0a6; text-decoration: none; margin-right: 16px; }
            nav a.active { color: #f2f2f2; font-weight: 600; }
            h1 { font-weight: 600; letter-spacing: 0.5px; }
            .dot {
                width: 10px; height: 10px; border-radius: 50%;
                background: #ff4b4b; display: inline-block;
                margin-right: 8px; animation: pulse 1.5s infinite;
            }
            @keyframes pulse { 0%,100% {opacity:1;} 50% {opacity:0.3;} }
            audio { margin-top: 24px; width: 320px; }
        </style>
    </head>
    <body>
        <nav>
            <a href="/" class="active">Эфир</a>
            <a href="/library">Далее в эфире</a>
        </nav>
        <h1><span class="dot"></span>МОЁ РАДИО</h1>
        <audio controls autoplay>
            <source src="/stream" type="audio/mpeg">
        </audio>
    </body>
    </html>
    """


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT)
