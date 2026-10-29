from __future__ import annotations
import argparse,time
from pathlib import Path
from .core import Queue,close_logger,logger_for

def main():
 p=argparse.ArgumentParser(); p.add_argument("--data-dir",default="runtime"); p.add_argument("--interval-seconds",type=float,default=300); a=p.parse_args(); d=Path(a.data_dir); q=Queue(d/"queue.sqlite3",logger_for(d/"watcher.log"))
 try:
  while True:
   q.log.info("watcher_check"); time.sleep(a.interval_seconds)
 finally:
  q.close(); close_logger(q.log)
if __name__=="__main__":main()
