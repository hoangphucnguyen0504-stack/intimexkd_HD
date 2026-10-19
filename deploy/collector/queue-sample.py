#!/usr/bin/env python3
"""Read-only HDoi sampler. Token is read from /opt/hdoi/.env and never printed."""
import json, os, re, stat, subprocess, sys, time, urllib.request
from datetime import datetime, timezone, timedelta

ENV_PATH = '/opt/hdoi/.env'
LOG_DIR = '/opt/hdoi/logs'
LOG_PATH = os.path.join(LOG_DIR, 'queue-sample.jsonl')
ERR_PATH = os.path.join(LOG_DIR, 'queue-sample.err')
ENABLED = '/opt/hdoi/bin/queue-sample.enabled'
INTERVAL = '/opt/hdoi/bin/queue-sample.interval'
CONTAINERS = ['hdoi-queue-api-1', 'hdoi-queue-worker-1', 'hdoi-redis-1', 'hdoi-proxy-1']
MAX_LOG_BYTES = 2 * 1024 * 1024
KEEP_LOGS = 8
VN = timezone(timedelta(hours=7))

def warn(msg):
    sys.stderr.write(msg + '\n')

def run(args, timeout=20, stdin=None):
    try:
        p = subprocess.run(
            args, input=stdin, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, timeout=timeout)
        return p.returncode, p.stdout, p.stderr
    except Exception as exc:
        return 1, '', type(exc).__name__

def read_token():
    mode = os.stat(ENV_PATH).st_mode
    if stat.S_IMODE(mode) & 0o077:
        warn('env_file_mode_not_600')
    token = None
    with open(ENV_PATH, 'r', encoding='utf-8') as fh:
        for line in fh:
            if line.startswith('QUEUE_API_TOKEN='):
                token = line.split('=', 1)[1].strip().strip('"').strip("'")
                break
    if not token:
        raise SystemExit('missing_queue_api_token')
    return token

def timed_get(url, token=None):
    headers = {}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    req = urllib.request.Request(url, headers=headers)
    started = time.time()
    with urllib.request.urlopen(req, timeout=8) as resp:
        body = json.load(resp)
        status = resp.status
    return body, int((time.time() - started) * 1000), status

def rotate(path):
    if not os.path.isfile(path) or os.path.getsize(path) < MAX_LOG_BYTES:
        return
    oldest = path + '.' + str(KEEP_LOGS)
    if os.path.isfile(oldest):
        os.remove(oldest)
    for i in range(KEEP_LOGS, 0, -1):
        src = path if i == 1 else path + '.' + str(i - 1)
        dst = path + '.' + str(i)
        if os.path.isfile(src):
            os.replace(src, dst)

def parse_info(text):
    out = {}
    for key in ('used_memory', 'used_memory_peak', 'aof_rewrite_in_progress'):
        match = re.search(r'^' + key + r':(\S+)', text, re.M)
        out[key] = int(match.group(1)) if match else None
    return out

def docker_snapshot():
    stats, restarts = {}, {}
    code, out, _ = run(['docker', 'stats', '--no-stream', '--format', '{{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}'] + CONTAINERS)
    if code == 0:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) >= 3:
                mem = parts[1].split('/')[0].strip()
                stats[parts[0]] = {'mem': mem, 'cpu': parts[2].strip()}
    for name in CONTAINERS:
        code, out, _ = run(['docker', 'inspect', '-f', '{{.RestartCount}} {{.State.OOMKilled}} {{.State.Status}}', name])
        if code == 0:
            bits = out.strip().split()
            restarts[name] = {
                'restartCount': int(bits[0]) if bits else None,
                'oomKilled': bits[1] if len(bits) > 1 else None,
                'status': bits[2] if len(bits) > 2 else None,
            }
    return stats, restarts

def dmesg_oom():
    code, out, err = run(['dmesg', '-T'])
    if code != 0:
        return None, 'dmesg_denied'
    return len(re.findall(r'oom|Killed process', out, re.I)), None

def sample(token):
    row = {'at': datetime.now(VN).strftime('%Y-%m-%dT%H:%M:%S+07:00')}
    live_ms = metrics_ms = None
    try:
        _, live_ms, _ = timed_get('http://127.0.0.1:3100/health/live')
        metrics, metrics_ms, _ = timed_get('http://127.0.0.1:3100/v1/metrics', token)
        sheets = metrics.get('sheets') or {}
        bg = metrics.get('background') or {}
        row.update({
            'oldestOutstandingAgeMs': metrics.get('oldestOutstandingAgeMs'),
            'dispatchPending': metrics.get('dispatchPending'),
            'unknownCount': metrics.get('unknownCount'),
            'sheetsActive': sheets.get('active'),
            'sheetsWaiting': sheets.get('waiting'),
            'bgActive': bg.get('active'),
            'bgWaiting': bg.get('waiting'),
            'active': int(sheets.get('active') or 0) + int(bg.get('active') or 0),
            'waiting': int(sheets.get('waiting') or 0) + int(bg.get('waiting') or 0) + int(metrics.get('dispatchPending') or 0),
        })
    except Exception as exc:
        row['metricsError'] = type(exc).__name__
    row['liveMs'] = live_ms
    row['metricsMs'] = metrics_ms
    code, info, _ = run(['docker', 'exec', 'hdoi-redis-1', 'redis-cli', 'INFO', 'memory'])
    if code == 0:
        row['redis'] = parse_info(info)
    stats, restarts = docker_snapshot()
    row['dockerStats'] = stats
    row['restarts'] = restarts
    api = stats.get('hdoi-queue-api-1') or {}
    row['apiMem'] = api.get('mem')
    oom, oom_err = dmesg_oom()
    row['dmesgOomCount'] = oom
    if oom_err:
        row['dmesgError'] = oom_err
    return row

def write_sample(row):
    os.makedirs(LOG_DIR, exist_ok=True)
    rotate(LOG_PATH)
    rotate(ERR_PATH)
    with open(LOG_PATH, 'a', encoding='utf-8') as fh:
        fh.write(json.dumps(row, separators=(',', ':')) + '\n')

def probe():
    token = read_token()
    rows = []
    for i in range(3):
        row = sample(token)
        rows.append(row)
        print(json.dumps({
            'n': i + 1,
            'liveMs': row.get('liveMs'),
            'metricsMs': row.get('metricsMs'),
            'apiMem': row.get('apiMem'),
            'active': row.get('active'),
            'waiting': row.get('waiting'),
            'oldestOutstandingAgeMs': row.get('oldestOutstandingAgeMs'),
        }))
        if i < 2:
            time.sleep(2)
    times = [r.get('metricsMs') for r in rows if r.get('metricsMs') is not None]
    avg = int(sum(times) / len(times)) if times else None
    heavy = (avg is None) or (avg > 500)
    interval = 15 if heavy else 5
    print(json.dumps({'avgMetricsMs': avg, 'intervalMin': interval, 'heavy': heavy}))
    return interval, rows

def install_cron(minutes):
    line = '%s * * * * /usr/bin/python3 /opt/hdoi/bin/queue-sample.py\n' % ('*/' + str(minutes))
    code, current, _ = run(['crontab', '-l'])
    kept = []
    if code == 0:
        kept = [row for row in current.splitlines(True) if 'queue-sample.py' not in row]
    os.makedirs('/opt/hdoi/bin', exist_ok=True)
    with open(ENABLED, 'w', encoding='utf-8') as fh:
        fh.write('1\n')
    with open(INTERVAL, 'w', encoding='utf-8') as fh:
        fh.write(str(minutes) + '\n')
    payload = ''.join(kept) + line
    code, _, err = run(['crontab', '-'], stdin=payload)
    if code != 0:
        raise SystemExit('crontab_install_failed')
    print(json.dumps({'installed': True, 'intervalMin': minutes, 'log': LOG_PATH}))

def main():
    args = sys.argv[1:]
    if '--probe' in args:
        probe()
        return
    if '--install' in args:
        minutes, _ = probe()
        install_cron(minutes)
        return
    if '--stop' in args:
        if os.path.isfile(ENABLED):
            os.remove(ENABLED)
        code, current, _ = run(['crontab', '-l'])
        if code == 0:
            kept = ''.join(row for row in current.splitlines(True) if 'queue-sample.py' not in row)
            run(['crontab', '-'], stdin=kept)
        print(json.dumps({'stopped': True}))
        return
    if not os.path.isfile(ENABLED):
        return
    token = read_token()
    try:
        write_sample(sample(token))
    except Exception as exc:
        os.makedirs(LOG_DIR, exist_ok=True)
        rotate(ERR_PATH)
        with open(ERR_PATH, 'a', encoding='utf-8') as fh:
            fh.write(datetime.now(VN).isoformat() + ' ' + type(exc).__name__ + '\n')

if __name__ == '__main__':
    main()
