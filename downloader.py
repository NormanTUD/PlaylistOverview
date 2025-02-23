import os
import sys
import sqlite3
import json
import subprocess
from datetime import datetime
from itertools import islice
from youtube_comment_downloader import YoutubeCommentDownloader
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.table import Table
import argparse
import random
import time
from pprint import pprint

def dier(msg):
    pprint(msg)
    sys.exit(10)

DB_NAME = "yt_data.db"
console = Console()

def parse_args():
    parser = argparse.ArgumentParser(description="Extrahiere YouTube-Kommentare aus einer Playlist und speichere sie in einer SQLite-Datenbank.")
    parser.add_argument("playlist_url", help="Die URL der YouTube-Playlist")
    parser.add_argument("--output_file", help="Pfad zur Outputdatei")

    return parser.parse_args()

args = parse_args()

def execute_with_retry(cur, query, params=(), delay=0.1):
    """Führt eine SQLite-Abfrage aus und versucht es erneut, falls die Datenbank gesperrt ist."""
    while True:
        try:
            cur.execute(query, params)
            return  # Erfolgreich, also raus hier
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                console.print("Waiting for DB to unlock...")
                time.sleep(delay)  # Wartezeit zwischen Versuchen
            else:
                raise  # Andere Fehler direkt weiterleiten

def init_db():
    """ Erstellt die notwendigen Tabellen, falls sie nicht existieren. """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    execute_with_retry(cur, """
        CREATE TABLE IF NOT EXISTS playlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            last_updated TEXT
        )
    """)

    execute_with_retry(cur, """
        CREATE TABLE IF NOT EXISTS videos (
            id TEXT PRIMARY KEY,
            title TEXT,
            is_available INTEGER,
            last_updated TEXT
        )
    """)

    execute_with_retry(cur, """
        CREATE TABLE IF NOT EXISTS playlist_videos (
            playlist_id INTEGER,
            video_id TEXT,
            last_updated TEXT,
            FOREIGN KEY (playlist_id) REFERENCES playlists(id),
            FOREIGN KEY (video_id) REFERENCES videos(id),
            UNIQUE (playlist_id, video_id)
        )
    """)

    execute_with_retry(cur, """
        CREATE TABLE IF NOT EXISTS comments (
            id TEXT PRIMARY KEY,
            video_id TEXT,
            text TEXT,
            author TEXT,
            votes INTEGER,
            time_parsed INTEGER,
            FOREIGN KEY (video_id) REFERENCES videos(id)
        )
    """)

    execute_with_retry(cur, "CREATE VIRTUAL TABLE IF NOT EXISTS fts_comments USING fts5(id, text)")

    conn.commit()
    conn.close()

def get_playlist_videos(playlist_url):
    """ Holt die Video-IDs und Titel einer YouTube-Playlist mit yt-dlp. """
    command = [
        "yt-dlp",
        "--flat-playlist",
        "--print", "%(id)s\t%(title)s",
        playlist_url
    ]
    
    with console.status("[bold blue]Lade Playlist-Daten...[/]", spinner="dots"):
        result = subprocess.run(command, capture_output=True, text=True)

    videos = []
    for line in result.stdout.strip().split("\n"):
        if line:
            try:
                video_id, title = line.split("\t", 1)
                videos.append((video_id, title))
            except ValueError:
                console.print(f"[bold red]Fehler beim Parsen:[/]\n{line}")

    return videos

def show_video_table(videos):
    """ Zeigt eine formatierte Tabelle der Videos an. """
    table = Table(title="Gefundene Videos", header_style="bold magenta")
    table.add_column("Video-ID", style="cyan")
    table.add_column("Titel", style="white")

    for video_id, title in videos:
        table.add_row(video_id, title)

    console.print(table)

def save_playlist(playlist_url, videos):
    """ Speichert die Playlist und Videos in der Datenbank. """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    vids = []

    execute_with_retry(cur, "INSERT OR IGNORE INTO playlists (name, last_updated) VALUES (?, ?)", 
                (playlist_url, datetime.utcnow().isoformat()))
    execute_with_retry(cur, "UPDATE playlists SET last_updated = ? WHERE name = ?", 
                (datetime.utcnow().isoformat(), playlist_url))

    execute_with_retry(cur, "SELECT id FROM playlists WHERE name = ?", (playlist_url,))
    playlist_id = cur.fetchone()[0]

    with Progress(SpinnerColumn(), BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), console=console) as progress:
        task = progress.add_task("Speichere Videos...", total=len(videos))

        for video_id, title in videos:
            execute_with_retry(cur, "INSERT OR IGNORE INTO videos (id, title, is_available, last_updated) VALUES (?, ?, 1, ?)",
                        (video_id, title, datetime.utcnow().isoformat()))
            execute_with_retry(cur, "UPDATE videos SET last_updated = ?, is_available = 1 WHERE id = ?", 
                        (datetime.utcnow().isoformat(), video_id))
            
            execute_with_retry(cur, "INSERT OR IGNORE INTO playlist_videos (playlist_id, video_id, last_updated) VALUES (?, ?, ?)",
                        (playlist_id, video_id, datetime.utcnow().isoformat()))

            progress.update(task, advance=1)

            vids.append([video_id, title])

    conn.commit()
    conn.close()

    return vids

def comments_exist(video_id):
    """ Überprüft, ob bereits Kommentare für das Video existieren. """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("SELECT EXISTS(SELECT 1 FROM comments WHERE video_id = ? LIMIT 1)", (video_id,))
    exists = cur.fetchone()[0]

    conn.close()
    return bool(exists)

def download_comments(video_id, progress):
    """ Lädt die Kommentare eines Videos herunter und speichert sie in der Datenbank. """
    if comments_exist(video_id):
        task = progress.add_task(f"Kommentare bereits runtergeladen für Video {video_id}")
        return task

    downloader = YoutubeCommentDownloader()
    comments = downloader.get_comments_from_url(f"https://www.youtube.com/watch?v={video_id}", sort_by=0)

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    task = progress.add_task(f"Lade Kommentare für {video_id}...")

    for comment in comments:
        try:
            votes = int(comment['votes'] or 0)
        except:
            votes = 0

        execute_with_retry(cur, "INSERT OR IGNORE INTO comments (id, video_id, text, author, votes, time_parsed) VALUES (?, ?, ?, ?, ?, ?)",
                    (comment['cid'], video_id, comment['text'], comment['author'], votes, comment['time_parsed']))
        
        execute_with_retry(cur, "INSERT OR REPLACE INTO fts_comments (id, text) VALUES (?, ?)", (comment['cid'], comment['text']))
        progress.update(task, advance=1)

    conn.commit()
    conn.close()

    return task

def write_html_to_file(vids):
    if args.output_file:
        output_path = os.path.dirname(args.output_file)  # Verzeichnis extrahieren

        inner_html = ""

        for v in vids:
            video_id = v[0]
            title = v[1]

            inner_html = f'{inner_html}\n<a target="_blank" href="https://www.youtube.com/watch?v={video_id}"><img src="https://i.ytimg.com/vi/{video_id}/hqdefault.jpg" width="150px"><div class="caption">{title}</div></a>'

        if output_path and not os.path.exists(output_path):
            os.makedirs(output_path, exist_ok=True)  # Verzeichnis erstellen, falls nicht vorhanden

        html_file = """
<head>
<style>#images{ text-align:center; margin:50px auto; }
#images a{margin:0px 20px; display:inline-block; text-decoration:none; color:black; }
.caption { width: 150px; height: 80px; overflow-y: auto; }
</style>
<meta charset="UTF-8">
</head>
<div id="images">
            """

        html_file += inner_html

        html_file += """
</div>

<center>
<button id="random" onclick="player.loadVideoById(get_random_ytid(0))">Next random video</button><br><br>
<div id="player"></div>
<center>

<script src="https://www.youtube.com/iframe_api"></script>

<script>
        var player;

        var anchors = document.getElementsByTagName("a");
        var youtube_ids = [];
        for(var i = 0; i < anchors.length; i++){
                youtube_ids.push(anchors[i].href.replace("https://youtube.com/watch?v=", ""));
        }

        function get_random_ytid (recursion) {
                if(youtube_ids.length) {
                        var index = Math.floor(Math.random()*youtube_ids.length);
                        var item = youtube_ids[index];
                        youtube_ids.splice(index, 1);
                } else {
                        if(recursion) {
                                alert("Cannot get IDs");
                        } else {
                                for(var i = 0; i < anchors.length; i++){
                                        youtube_ids.push(anchors[i].href.replace("https://youtube.com/watch?v=", ""));
                                }
                                item = get_random_ytid(1);
                        }
                }
                return item;

        }

        function onPlayerReady(event) {
                event.target.playVideo();
        }

        function onPlayerStateChange(event) {
                if(event.data === YT.PlayerState.ENDED) {
                        player.loadVideoById(get_random_ytid(0));
                }
        }

        function onYouTubePlayerAPIReady() {
                player = new YT.Player("player", {
                        height: "390",
                        width: "640",
                        videoId: get_random_ytid(0),
                        playerVars: { 
                                "autoplay": 1,
                                "controls": 1
                        },
                        events: {
                                "onReady": onPlayerReady,
                                "onStateChange": onPlayerStateChange
                        }
                });
        }
</script>
        """

        with open(args.output_file, "w", encoding="utf-8") as f:
            f.write(html_file)
    else:
        console.print(f"[green]--output_file not set[/]")

def main():
    playlist_url = args.playlist_url
    
    init_db()
    console.print(f"[green]✔ Datenbank initialisiert: {DB_NAME}[/]")

    videos = get_playlist_videos(playlist_url)
    console.print(f"[cyan]✔ {len(videos)} Videos gefunden[/]")

    show_video_table(videos)

    vids = save_playlist(playlist_url, videos)
    console.print(f"[green]✔ Playlist gespeichert[/]")

    write_html_to_file(vids)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("[red]❌ Abbruch durch Benutzer[/]")
        sys.exit(0)
