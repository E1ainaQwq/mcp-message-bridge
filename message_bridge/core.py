from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
from pathlib import Path
import re
import secrets
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone

MAX_TEXT_CHARS = 12000
MAX_LIMIT = 100
MAX_WAIT_SECONDS = 30
MAX_PER_PARTICIPANT_CONVERSATION = 8
MAX_PER_CONVERSATION = 16
MAX_TURNS = 36
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")

class BridgeError(RuntimeError):
    pass

def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def participant(value: str) -> str:
    value = str(value or "").strip()
    if not ID_RE.fullmatch(value):
        raise BridgeError("participant must match [A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
    return value

def identifier(value: str, name: str) -> str:
    value = str(value or "").strip()
    if not ID_RE.fullmatch(value):
        raise BridgeError(f"{name} has an invalid format")
    return value

def logger_for(path: Path) -> logging.Logger:
    path.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger(f"message_bridge:{path.resolve()}")
    log.setLevel(logging.INFO)
    log.propagate = False
    if not log.handlers:
        h = logging.FileHandler(path, encoding="utf-8")
        h.setFormatter(logging.Formatter("%(message)s"))
        log.addHandler(h)
    return log

def close_logger(log: logging.Logger) -> None:
    for handler in list(log.handlers):
        handler.close()
        log.removeHandler(handler)

class Auth:
    def __init__(self, path: Path, name: str, token: str, log: logging.Logger):
        self.name = participant(name)
        self.token = token.strip()
        self.log = log
        try:
            entry = json.loads(path.read_text(encoding="utf-8"))["participants"][self.name]
            actual = hashlib.pbkdf2_hmac("sha256", self.token.encode(), bytes.fromhex(entry["salt"]), int(entry["rounds"]))
        except (OSError, KeyError, TypeError, ValueError) as exc:
            raise BridgeError("authentication metadata is missing or invalid") from exc
        if len(self.token) < 24 or not hmac.compare_digest(actual, bytes.fromhex(entry["hash"])):
            log.warning(json.dumps({"event":"auth_failure","participant":self.name}))
            raise BridgeError("authentication failed")

def create_auth(path: Path, names: list[str], force: bool = False) -> dict[str, str]:
    if path.exists() and not force:
        raise BridgeError("auth file already exists; use --force to rotate")
    path.parent.mkdir(parents=True, exist_ok=True)
    result, entries = {}, {}
    for raw in names:
        name = participant(raw)
        token, salt, rounds = secrets.token_urlsafe(32), secrets.token_bytes(16), 210_000
        result[name] = token
        entries[name] = {"salt": salt.hex(), "hash": hashlib.pbkdf2_hmac("sha256", token.encode(), salt, rounds).hex(), "rounds": rounds}
    path.write_text(json.dumps({"version":1,"participants":entries}, indent=2)+"\n", encoding="utf-8")
    return result

class Queue:
    def __init__(self, path: Path, log: logging.Logger):
        self.path, self.log, self.lock = path, log, threading.RLock()
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=10, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS cursors(participant TEXT PRIMARY KEY,last_seen INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY AUTOINCREMENT,message_id TEXT NOT NULL,sender TEXT NOT NULL,recipient TEXT NOT NULL,conversation_id TEXT NOT NULL,text TEXT NOT NULL,created_at TEXT NOT NULL,UNIQUE(sender,message_id));
        CREATE TABLE IF NOT EXISTS watcher(participant TEXT PRIMARY KEY,last_emitted INTEGER NOT NULL DEFAULT 0,failures INTEGER NOT NULL DEFAULT 0);
        INSERT OR IGNORE INTO meta(key,value) VALUES('paused','0');
        INSERT OR IGNORE INTO meta(key,value) VALUES('pause_reason','');
        INSERT OR IGNORE INTO meta(key,value) VALUES('turns_used','0');
        INSERT OR IGNORE INTO meta(key,value) VALUES('max_turns','36');
        """)
    def close(self): self.db.close()
    def paused(self):
        rows=self.db.execute("SELECT key,value FROM meta WHERE key IN ('paused','pause_reason')").fetchall(); d={r[0]:r[1] for r in rows}; return d.get('paused')=='1',d.get('pause_reason','')
    def status(self, name):
        name=participant(name)
        with self.lock:
            paused,reason=self.paused(); c=self.db.execute("SELECT last_seen FROM cursors WHERE participant=?",(name,)).fetchone(); cursor=int(c[0]) if c else 0
            pending=self.db.execute("SELECT COUNT(*) FROM messages WHERE recipient=? AND id>?",(name,cursor)).fetchone()[0]; total=self.db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            turns=int(self.db.execute("SELECT value FROM meta WHERE key='turns_used'").fetchone()[0]); max_turns=int(self.db.execute("SELECT value FROM meta WHERE key='max_turns'").fetchone()[0])
        return {"participant":name,"authenticated":True,"paused":paused,"pause_reason":reason,"queue":{"pending":pending,"total":total,"cursor":cursor},"limits":{"per_participant_conversation":MAX_PER_PARTICIPANT_CONVERSATION,"per_conversation":MAX_PER_CONVERSATION,"turns_used":turns,"max_turns":max_turns},"privacy":{"explicit_text_only":True,"automatic_forwarding":False,"private_memory":False,"other_chats":False,"tool_output":False},"polling":{"client_driven":True,"server_push":False,"background_trigger":False}}
    def send(self,sender,recipient,text,message_id,conversation_id):
        sender,recipient=participant(sender),participant(recipient)
        if sender==recipient: raise BridgeError("recipient must differ from sender")
        text=str(text or '').strip(); message_id=identifier(message_id,'message_id'); conversation_id=identifier(conversation_id,'conversation_id')
        if not text or len(text)>MAX_TEXT_CHARS: raise BridgeError(f"text must contain 1-{MAX_TEXT_CHARS} characters")
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                paused,reason=self.paused()
                if paused: raise BridgeError(f"bridge is paused: {reason}")
                old=self.db.execute("SELECT * FROM messages WHERE sender=? AND message_id=?",(sender,message_id)).fetchone()
                if old:
                    if old['text']!=text or old['recipient']!=recipient or old['conversation_id']!=conversation_id: raise BridgeError("IDEMPOTENCY_CONFLICT")
                    self.db.execute("COMMIT"); return {"ok":True,"duplicate":True,"message_id":old['message_id'],"sequence":old['id']}
                turns=int(self.db.execute("SELECT value FROM meta WHERE key='turns_used'").fetchone()[0]); max_turns=int(self.db.execute("SELECT value FROM meta WHERE key='max_turns'").fetchone()[0])
                if turns>=max_turns: raise BridgeError("TURN_LIMIT")
                if self.db.execute("SELECT COUNT(*) FROM messages WHERE sender=? AND conversation_id=?",(sender,conversation_id)).fetchone()[0]>=MAX_PER_PARTICIPANT_CONVERSATION: raise BridgeError("CONVERSATION_LIMIT")
                if self.db.execute("SELECT COUNT(*) FROM messages WHERE conversation_id=?",(conversation_id,)).fetchone()[0]>=MAX_PER_CONVERSATION: raise BridgeError("CONVERSATION_LIMIT")
                cur=self.db.execute("INSERT INTO messages(message_id,sender,recipient,conversation_id,text,created_at) VALUES(?,?,?,?,?,?)",(message_id,sender,recipient,conversation_id,text,now())); self.db.execute("UPDATE meta SET value=value+1 WHERE key='turns_used'"); self.db.execute("COMMIT")
            except Exception:
                self.db.execute("ROLLBACK"); raise
        return {"ok":True,"duplicate":False,"message_id":message_id,"sequence":cur.lastrowid,"conversation_id":conversation_id}
    def inbox(self,name,after,limit,wait,ack):
        name=participant(name); limit=int(limit); wait=float(wait); start=time.monotonic()+wait
        if not 1<=limit<=MAX_LIMIT or not 0<=wait<=MAX_WAIT_SECONDS: raise BridgeError("invalid inbox bounds")
        while True:
            with self.lock:
                if after is None:
                    row=self.db.execute("SELECT last_seen FROM cursors WHERE participant=?",(name,)).fetchone(); after=int(row[0]) if row else 0
                rows=self.db.execute("SELECT id,message_id,sender,recipient,conversation_id,text,created_at FROM messages WHERE recipient=? AND id>? ORDER BY id LIMIT ?",(name,after,limit)).fetchall(); msgs=[dict(r) for r in rows]; next_id=rows[-1]['id'] if rows else after
                if ack and rows: self.db.execute("INSERT INTO cursors(participant,last_seen) VALUES(?,?) ON CONFLICT(participant) DO UPDATE SET last_seen=MAX(last_seen,excluded.last_seen)",(name,next_id))
            if msgs or time.monotonic()>=start: return {"participant":name,"messages":msgs,"after_id":after,"next_id":next_id,"acknowledged":bool(ack and msgs)}
            time.sleep(.1)
