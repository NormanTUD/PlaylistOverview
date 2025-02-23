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
    parser.add_argument("--shuffle", action="store_true", help="Kommentare in zufälliger Reihenfolge verarbeiten (Standard: False)")

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

    conn.commit()
    conn.close()

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

def main():
    playlist_url = args.playlist_url
    
    init_db()
    console.print(f"[green]✔ Datenbank initialisiert: {DB_NAME}[/]")

    videos = get_playlist_videos(playlist_url)
    console.print(f"[cyan]✔ {len(videos)} Videos gefunden[/]")

    show_video_table(videos)

    save_playlist(playlist_url, videos)
    console.print(f"[green]✔ Playlist gespeichert[/]")

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console, transient=True) as progress:
        if args.shuffle:
            random.shuffle(videos)

        #for video_id, _ in videos:
        #    task = download_comments(video_id, progress)
        #    progress.remove_task(task)

    console.print("[bold green]✔ Alle Kommentare gespeichert[/]")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("[red]❌ Abbruch durch Benutzer[/]")
        sys.exit(0)
