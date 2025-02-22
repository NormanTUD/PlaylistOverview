import sys
import sqlite3
import json
import subprocess
from datetime import datetime
from itertools import islice
from youtube_comment_downloader import YoutubeCommentDownloader

DB_NAME = "yt_data.db"

def init_db():
    """ Erstellt die notwendigen Tabellen, falls sie nicht existieren. """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Playlists-Tabelle
    cur.execute("""
        CREATE TABLE IF NOT EXISTS playlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            last_updated TEXT
        )
    """)

    # Videos-Tabelle
    cur.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            id TEXT PRIMARY KEY,
            title TEXT,
            is_available INTEGER,
            last_updated TEXT
        )
    """)

    # Playlist-Video-Verknüpfung
    cur.execute("""
        CREATE TABLE IF NOT EXISTS playlist_videos (
            playlist_id INTEGER,
            video_id TEXT,
            last_updated TEXT,
            FOREIGN KEY (playlist_id) REFERENCES playlists(id),
            FOREIGN KEY (video_id) REFERENCES videos(id),
            UNIQUE (playlist_id, video_id)
        )
    """)

    # Volltextsuche für Videos (Deutsch & Englisch)
    cur.execute("CREATE VIRTUAL TABLE IF NOT EXISTS fts_videos_en USING fts5(id, title)")
    cur.execute("CREATE VIRTUAL TABLE IF NOT EXISTS fts_videos_de USING fts5(id, title)")

    # Kommentare-Tabelle
    cur.execute("""
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

    # Volltextsuche für Kommentare
    cur.execute("CREATE VIRTUAL TABLE IF NOT EXISTS fts_comments USING fts5(id, text)")

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
    result = subprocess.run(command, capture_output=True, text=True)

    videos = []
    for line in result.stdout.strip().split("\n"):
        if line:
            video_id, title = line.split("\t", 1)
            videos.append((video_id, title))
    
    return videos

def save_playlist(playlist_url, videos):
    """ Speichert die Playlist und Videos in der Datenbank. """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    # Playlist hinzufügen oder aktualisieren
    cur.execute("INSERT OR IGNORE INTO playlists (name, last_updated) VALUES (?, ?)", 
                (playlist_url, datetime.utcnow().isoformat()))
    cur.execute("UPDATE playlists SET last_updated = ? WHERE name = ?", 
                (datetime.utcnow().isoformat(), playlist_url))
    
    cur.execute("SELECT id FROM playlists WHERE name = ?", (playlist_url,))
    playlist_id = cur.fetchone()[0]

    # Videos speichern
    for video_id, title in videos:
        cur.execute("INSERT OR IGNORE INTO videos (id, title, is_available, last_updated) VALUES (?, ?, 1, ?)",
                    (video_id, title, datetime.utcnow().isoformat()))
        cur.execute("UPDATE videos SET last_updated = ?, is_available = 1 WHERE id = ?", 
                    (datetime.utcnow().isoformat(), video_id))

        # Verknüpfung Playlist <-> Video
        cur.execute("INSERT OR IGNORE INTO playlist_videos (playlist_id, video_id, last_updated) VALUES (?, ?, ?)",
                    (playlist_id, video_id, datetime.utcnow().isoformat()))

        # Volltextsuche aktualisieren
        cur.execute("INSERT OR REPLACE INTO fts_videos_en (id, title) VALUES (?, ?)", (video_id, title))
        cur.execute("INSERT OR REPLACE INTO fts_videos_de (id, title) VALUES (?, ?)", (video_id, title))

    conn.commit()
    conn.close()

def download_comments(video_id):
    """ Lädt die Kommentare eines Videos herunter und speichert sie in der Datenbank. """
    downloader = YoutubeCommentDownloader()
    comments = downloader.get_comments_from_url(f"https://www.youtube.com/watch?v={video_id}", sort_by=0)  # 0 = Beste Kommentare zuerst

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    for comment in islice(comments, 50):  # Maximal 50 Kommentare pro Video speichern
        cur.execute("INSERT OR IGNORE INTO comments (id, video_id, text, author, votes, time_parsed) VALUES (?, ?, ?, ?, ?, ?)",
                    (comment['cid'], video_id, comment['text'], comment['author'], int(comment['votes'] or 0), comment['time_parsed']))

        # Kommentar zur Volltextsuche hinzufügen
        cur.execute("INSERT OR REPLACE INTO fts_comments (id, text) VALUES (?, ?)", (comment['cid'], comment['text']))

    conn.commit()
    conn.close()

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 yt_to_sqlite.py <playlist_url>")
        sys.exit(1)

    playlist_url = sys.argv[1]
    
    init_db()
    print(f"[+] Datenbank initialisiert: {DB_NAME}")

    videos = get_playlist_videos(playlist_url)
    print(f"[+] {len(videos)} Videos gefunden")

    save_playlist(playlist_url, videos)
    print(f"[+] Playlist {playlist_url} gespeichert")

    # Kommentare abrufen
    for video_id, _ in videos:
        print(f"  -> Lade Kommentare für {video_id} ...")
        download_comments(video_id)

    print("[+] Alle Kommentare gespeichert")

if __name__ == "__main__":
    main()
