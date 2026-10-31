#!/usr/bin/env python3
"""GitHub Actions nightly EnUcuzGB per-item SELL/BUY daily summary collector.
Only one calendar day is requested; neither live offers nor 90-day history are scraped.
"""
from __future__ import annotations
import gzip
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime,timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parent.parent
CLOUD=ROOT/'cloud'
BASE='https://www.enucuzgb.com/api/v2/market/history'
IST=ZoneInfo('Europe/Istanbul')
INTERVAL=4.5  # <=13.34 requests per minute with initial start-to-start spacing

def request_history(api_key,scope,iid,side,day):
    args={'item_id':iid,'server':scope,'type':side,'from':day,'to':day}
    url=BASE+'?'+urllib.parse.urlencode(args)
    req=urllib.request.Request(url,headers={'X-API-Key':api_key,'Accept':'application/json','User-Agent':'USKO-Coin-Hunter-Cloud/0.9.38'})
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                payload=json.loads(resp.read(3_000_000).decode('utf-8'))
            if payload.get('success') is not True or not isinstance(payload.get('data'),list):
                raise RuntimeError('API denied request or returned unexpected data: '+str(payload.get('error',{}).get('code','unknown')))
            return payload['data']
        except urllib.error.HTTPError as exc:
            if exc.code not in (429,500,502,503,504) or attempt==5:
                raise RuntimeError(f'API HTTP {exc.code} for item {iid} {side}') from None
            retry=exc.headers.get('Retry-After','')
            try: delay=min(180,max(15,int(retry)))
            except ValueError: delay=min(180,10*2**attempt)
            print(f'API busy: HTTP {exc.code}, retry after {delay}s',flush=True)
            time.sleep(delay)
        except (urllib.error.URLError,TimeoutError) as exc:
            if attempt==5: raise RuntimeError(f'Network failed on item {iid} {side}') from exc
            time.sleep(min(120,10*2**attempt))
    raise RuntimeError('Retries exhausted')

def normalize_rows(rows, scope, item, side, day):
    result=[]
    for r in rows:
        if not isinstance(r,dict) or str(r.get('date',''))[:10]!=day:continue
        if str(r.get('item_id',item)).strip()!=item:continue
        if str(r.get('type',side)).strip().lower()!=side:continue
        channel=str(r.get('server',scope)).upper()
        if not re.fullmatch(r'[A-Z0-9_-]{2,20}',channel) or not channel.startswith(scope):continue
        try:
            low=int(r['min_price']);high=int(r['max_price']);avg=float(r['avg_price']);count=int(r['listings_count'])
        except (ValueError,TypeError,KeyError):continue
        if not (0<low<=avg<=high<=10**17 and 0<count<=10**9):continue
        result.append(dict(item_id=item,day=day,channel=channel,side=side,
                           min_price=low,avg_price=avg,max_price=high,listings_count=count))
    return result

def collect(day=None, requester=request_history, sleep=time.sleep):
    api_key=os.getenv('ENUCUZGB_API_KEY','').strip()
    if not api_key:raise RuntimeError('GitHub Secrets ENUCUZGB_API_KEY is missing.')
    source=json.loads((CLOUD/'items.json').read_text(encoding='utf-8'))
    scope=str(source.get('scope','')).upper()
    ids=source.get('item_ids')
    if not re.fullmatch(r'[A-Z][A-Z0-9_-]{1,19}',scope):raise ValueError('Missing valid scope in cloud/items.json')
    if not isinstance(ids,list) or not 1<=len(ids)<=1500:raise ValueError('cloud/items.json must contain 1..1500 item IDs.')
    ids=list(dict.fromkeys(str(v).strip() for v in ids))
    if not all(re.fullmatch(r'\d{1,15}',v) for v in ids):raise ValueError('Invalid item ID in cloud/items.json')
    day=day or (datetime.now(IST).date()-timedelta(days=1)).isoformat()
    datetime.strptime(day,'%Y-%m-%d')
    if day >= datetime.now(IST).date().isoformat():raise ValueError('Only completed past calendar days may be collected.')
    records=[];last_time=0;total=len(ids)*2;n=0
    for iid in ids:
        for side in ('sell','buy'):
            wait=INTERVAL-(time.monotonic()-last_time)
            if wait>0:sleep(wait)
            last_time=time.monotonic()
            raw=requester(api_key,scope,iid,side,day)
            records.extend(normalize_rows(raw,scope,iid,side,day))
            n+=1
            if n%25==0 or n==total:print(f'Finished {n}/{total} API calls · {len(records)} daily rows',flush=True)
    captured=time.time()
    package=dict(version=1,day=day,scope=scope,captured_at=captured,complete=True,records=records)
    json_bytes=json.dumps(package,ensure_ascii=False,separators=(',',':')).encode('utf-8')
    payload=gzip.compress(json_bytes,mtime=0)
    out=CLOUD/'data'/f'{day}.json.gz'
    out.parent.mkdir(exist_ok=True,parents=True)
    out.write_bytes(payload)
    manifest_path=CLOUD/'manifest.json'
    if manifest_path.exists():
        manifest=json.loads(manifest_path.read_text(encoding='utf-8'))
        if manifest.get('version')!=1:raise RuntimeError('Unsupported manifest version.')
    else: manifest={'version':1,'files':[]}
    entry=dict(day=day,path=f'cloud/data/{day}.json.gz',sha256=hashlib.sha256(payload).hexdigest(),entries=len(records))
    entries=[e for e in manifest.get('files',[]) if e.get('path')!=entry['path']]
    entries.append(entry)
    manifest['files']=sorted(entries,key=lambda x:x['day'])
    manifest_path.write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(f'Published locally: {out} ({len(records)} rows); workflow will commit the package.',flush=True)
    return entry

if __name__=='__main__':
    target=sys.argv[1] if len(sys.argv)>1 and sys.argv[1] else None
    collect(target)
