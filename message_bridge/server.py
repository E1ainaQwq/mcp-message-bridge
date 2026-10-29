from __future__ import annotations
import asyncio, os
from pathlib import Path
from mcp.server.fastmcp import FastMCP
from .core import Auth, BridgeError, Queue, close_logger, logger_for

ROOT=Path(__file__).resolve().parent.parent

def main():
    data=Path(os.environ.get("BRIDGE_DATA_DIR",ROOT/"runtime")); auth_path=Path(os.environ.get("BRIDGE_AUTH_FILE",data/"auth.json")); log=logger_for(Path(os.environ.get("BRIDGE_LOG_FILE",data/"bridge.log")))
    name=os.environ.get("BRIDGE_PARTICIPANT",""); auth=Auth(auth_path,name,os.environ.get("BRIDGE_TOKEN",""),log); queue=Queue(Path(os.environ.get("BRIDGE_DB_FILE",data/"queue.sqlite3")),log)
    mcp=FastMCP("mcp-message-bridge",instructions="Authenticated explicit-text local message queue. It never invokes models or imports chats, memory, or tool output. The client must poll; the server cannot wake a new model turn.")
    @mcp.tool()
    async def bridge_send(to:str,text:str,message_id:str,conversation_id:str)->dict:
        """Send explicit plain text to another configured participant; retries reuse message_id."""
        return await asyncio.to_thread(queue.send,auth.name,to,text,message_id,conversation_id)
    @mcp.tool()
    async def bridge_inbox(limit:int=20,after_id:int|None=None,wait_seconds:float=0,acknowledge:bool=False)->dict:
        """Read messages addressed to this participant. Default acknowledge=false is replay-safe."""
        return await asyncio.to_thread(queue.inbox,auth.name,after_id,limit,wait_seconds,acknowledge)
    @mcp.tool()
    async def bridge_status(action:str="status",reason:str="")->dict:
        """Read status; configured admin participant may pause, resume, or reset turns."""
        action=action.lower(); admin=os.environ.get("BRIDGE_ADMIN_PARTICIPANT","")
        if action=="status": return await asyncio.to_thread(queue.status,auth.name)
        if auth.name!=admin: raise BridgeError("admin participant required")
        if action in ("pause","resume"):
            with queue.lock:
                queue.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('paused',?)",('1' if action=='pause' else '0',)); queue.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('pause_reason',?)",(reason if action=='pause' else '',))
            return queue.status(auth.name)
        if action=="reset_turns":
            queue.db.execute("UPDATE meta SET value='0' WHERE key='turns_used'"); return queue.status(auth.name)
        raise BridgeError("action must be status, pause, resume, or reset_turns")
    try: mcp.run("stdio")
    finally:
        queue.close()
        close_logger(log)
if __name__=="__main__": main()
