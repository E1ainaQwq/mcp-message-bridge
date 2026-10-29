from __future__ import annotations
import argparse
from pathlib import Path
from .core import create_auth

def main():
 p=argparse.ArgumentParser(); p.add_argument("--file",default="runtime/auth.json"); p.add_argument("--participants",nargs="+",required=True); p.add_argument("--force",action="store_true"); a=p.parse_args(); tokens=create_auth(Path(a.file),a.participants,a.force)
 for name,token in tokens.items(): print(f"{name}: {token}")
if __name__=="__main__": main()
