#!/usr/bin/env python3
"""GitHub Actions worker: process a bounded request queue, publish backfill packages.
Never runs on user's PC. API key only from GitHub Actions secret.
"""
from __future__ import annotations
import gzip
import hashlib
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime,timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from gece_toplayici import request_history,normalize_rows,INTERVAL

ROOT=Path(__file__).resolve().parent.parent
CLOUD=ROOT/'cloud'
PENDING=CLOUD/'requests'/'pending.json'
IST=ZoneInfo('Europe/Istanbul')
MAX_RUN=120
DAY_LIMIT=7

def is_valid(r):
    from re import fullmatch
    try:
        days=(datetime.now(IST).date()-datetime.strptime(r['day'],'%Y-%m-%d').date()).days
        return (0<days<=DAY_LIMIT and bool(fullmatch(r'[A-Z][A-Z0-9_-]{1,19}',r['scope'])) and
                bool(fullmatch(r'\d{1,15}',str(r['item_id']))) and r['side'] in ('sell','buy'))
    except (ValueError,TypeError,KeyError):return False

def process(requester=request_history,sleep=time.sleep):
    if not PENDING.exists():return 0
    queue=json.loads(PENDING.read_text(encoding='utf-8'))
    if queue.get('version')!=1 or not isinstance(queue.get('requests'),list):raise ValueError('Invalid request queue')
    tasks=[r for r in queue['requests'] if is_valid(r)]
    # Discard expired requests so a queue created under the older 90-day
    # policy cannot block collection of the recent seven days.
    expired=len(queue['requests'])-len(tasks)
    if expired:
        PENDING.write_text(json.dumps({'version':1,'requests':tasks},ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    if not tasks:return 0
    api_key=os.getenv('ENUCUZGB_API_KEY','').strip()
    if not api_key:raise RuntimeError('Missing ENUCUZGB_API_KEY GitHub secret')
    batch=tasks[:MAX_RUN]
    by_day=defaultdict(list)
    last=0
    # Do not modify the queue unless every attempted request succeeds.
    for r in batch:
        wait=INTERVAL-(time.monotonic()-last)
        if wait>0:sleep(wait)
        last=time.monotonic()
        rows=requester(api_key,r['scope'],str(r['item_id']),r['side'],r['day'])
        by_day[(r['scope'],r['day'])].append((r,normalize_rows(rows,r['scope'],str(r['item_id']),r['side'],r['day'])))
    manifest_path=CLOUD/'manifest.json'
    manifest=json.loads(manifest_path.read_text(encoding='utf-8'))
    if manifest.get('version')!=1:raise RuntimeError('Unsupported manifest')
    manifest_files=manifest.get('files',[])
    for (scope,day),entries in by_day.items():
        # Same-day partial batches must accumulate without losing earlier backfills.
        path=CLOUD/'backfill'/scope/f'{day}.json.gz'
        records=[];requested=[]
        if path.exists():
            old=json.loads(gzip.decompress(path.read_bytes()))
            if old.get('scope')!=scope or old.get('day')!=day:raise RuntimeError('Backfill scope mismatch')
            records=list(old['records']);requested=list(old.get('requested',[]))
        requested_keys={(r['item_id'],r['side']) for r in requested}
        for req,values in entries:
            pair=(str(req['item_id']),req['side'])
            # Fresh retry replaces prior data for exactly this item/side in this day.
            records=[v for v in records if (str(v['item_id']),v['side'])!=pair]
            records.extend(values)
            if pair not in requested_keys:requested.append({'item_id':pair[0],'side':pair[1]});requested_keys.add(pair)
        package={'version':1,'day':day,'scope':scope,'captured_at':time.time(),
                 'complete':True,'records':records,'requested':requested}
        blob=gzip.compress(json.dumps(package,ensure_ascii=False,separators=(',',':')).encode(),mtime=0)
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(blob)
        rel=path.relative_to(ROOT).as_posix()
        entry={'day':day,'path':rel,'sha256':hashlib.sha256(blob).hexdigest(), 'entries':len(records)}
        manifest_files=[r for r in manifest_files if r.get('path')!=rel]
        manifest_files.append(entry)
    manifest['files']=sorted(manifest_files,key=lambda r:(r['day'],r['path']))
    manifest_path.write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    PENDING.write_text(json.dumps({'version':1,'requests':tasks[len(batch):]},ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print('Backfilled',len(batch),'requests; remaining',len(tasks)-len(batch),flush=True)
    return len(batch)

if __name__=='__main__':process()
