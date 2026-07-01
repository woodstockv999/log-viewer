const express = require('express');
const { execSync, execFileSync, spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const os = require('os');

const app = express();
const PORT = 3008;
const PM2_LOGS = path.join(os.homedir(), '.pm2', 'logs');
const SAFE_APP_NAME = /^[a-zA-Z0-9_-]+$/;

function tailFile(file, n, opts) {
  return execFileSync('tail', ['-n', String(n), file], { encoding: 'utf8', ...opts });
}

app.use(express.static(path.join(__dirname, 'public')));

// ── Basic app list ──────────────────────────────────────
app.get('/api/apps', (req, res) => {
  try {
    const procs = JSON.parse(execSync('pm2 jlist', { encoding: 'utf8' }));
    res.json(procs.map(p => ({
      name: p.name,
      id: p.pm_id,
      status: p.pm2_env.status,
      restarts: p.pm2_env.restart_time,
      uptime: p.pm2_env.pm_uptime,
      pid: p.pid,
    })));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Full status with error counts + system memory ───────
app.get('/api/status', (req, res) => {
  try {
    const procs = JSON.parse(execSync('pm2 jlist', { encoding: 'utf8', timeout: 5000 }));

    const apps = procs.map(p => {
      const name = p.name;
      const errFile = path.join(PM2_LOGS, `${name}-error.log`);
      let errLineCount = 0, lastError = null;

      if (fs.existsSync(errFile)) {
        try {
          const lines = tailFile(errFile, 200, { timeout: 2000, maxBuffer: 1024 * 1024 })
            .split('\n').filter(Boolean);
          errLineCount = lines.length;
          lastError = lines[lines.length - 1]?.substring(0, 300) || null;
        } catch {}
      }

      return {
        name,
        id: p.pm_id,
        status: p.pm2_env?.status || 'unknown',
        restarts: p.pm2_env?.restart_time || 0,
        uptime: p.pm2_env?.pm_uptime || null,
        cpu: p.monit?.cpu ?? 0,
        memory: p.monit?.memory ?? 0,
        pid: p.pid || null,
        errLineCount,
        lastError,
      };
    });

    // Accurate memory from /proc/meminfo
    let totalMem = os.totalmem(), availMem = os.freemem(), swapTotal = 0, swapFree = 0;
    try {
      const mi = fs.readFileSync('/proc/meminfo', 'utf8');
      const kb = key => { const m = mi.match(new RegExp(key + ':\\s+(\\d+)')); return m ? +m[1] * 1024 : 0; };
      totalMem  = kb('MemTotal');
      availMem  = kb('MemAvailable');
      swapTotal = kb('SwapTotal');
      swapFree  = kb('SwapFree');
    } catch {}

    res.json({
      apps,
      system: {
        totalMem,
        availMem,
        usedMem: totalMem - availMem,
        swapTotal,
        swapUsed: swapTotal - swapFree,
        uptime: os.uptime(),
        loadavg: os.loadavg(),
      },
      timestamp: Date.now(),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Log analysis ─────────────────────────────────────────
app.get('/api/analyze/:app', (req, res) => {
  const name = req.params.app;
  if (!SAFE_APP_NAME.test(name)) return res.status(400).json({ error: 'invalid app name' });
  const outFile = path.join(PM2_LOGS, `${name}-out.log`);
  const errFile = path.join(PM2_LOGS, `${name}-error.log`);

  let outLines = [], errLines = [];
  const read = (file, n) => {
    if (!fs.existsSync(file)) return [];
    try {
      return tailFile(file, n, { timeout: 3000, maxBuffer: 4 * 1024 * 1024 }).split('\n').filter(Boolean);
    } catch { return []; }
  };

  outLines = read(outFile, 300);
  errLines = read(errFile, 100);

  const errPat  = /error|exception|fail(?:ed|ure)?|crash|fatal|traceback|panic|uncaught/i;
  const warnPat = /warn(?:ing)?|deprecat|timeout|retry|retrying/i;

  const outErrors = outLines.filter(l => errPat.test(l));
  const outWarns  = outLines.filter(l => warnPat.test(l) && !errPat.test(l));

  const recentErrors = [...errLines.slice(-4), ...outErrors.slice(-2)]
    .slice(-5).map(l => l.substring(0, 250));

  res.json({
    app: name,
    analyzed: outLines.length + errLines.length,
    totalErrors:  outErrors.length + errLines.length,
    totalWarns:   outWarns.length,
    errLineCount: errLines.length,
    outErrorCount: outErrors.length,
    recentErrors,
  });
});

// ── Log SSE stream ────────────────────────────────────────
app.get('/api/logs/:app', (req, res) => {
  const appName = req.params.app;
  if (!SAFE_APP_NAME.test(appName)) return res.status(400).json({ error: 'invalid app name' });
  const lines = Math.min(parseInt(req.query.lines || '300'), 1000);

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (type, data) => {
    try { res.write(`event: ${type}\ndata: ${JSON.stringify(data)}\n\n`); } catch {}
  };

  const outFile = path.join(PM2_LOGS, `${appName}-out.log`);
  const errFile = path.join(PM2_LOGS, `${appName}-error.log`);

  for (const [file, type] of [[outFile, 'out'], [errFile, 'err']]) {
    if (fs.existsSync(file)) {
      try {
        tailFile(file, lines, { maxBuffer: 4 * 1024 * 1024 })
          .split('\n').filter(Boolean)
          .forEach(line => send('log', { type, line, app: appName }));
      } catch {}
    }
  }
  send('ready', { app: appName });

  const tails = [];
  for (const [file, type] of [[outFile, 'out'], [errFile, 'err']]) {
    if (!fs.existsSync(file)) continue;
    const tail = spawn('tail', ['-f', '-n', '0', file]);
    tail.stdout.on('data', chunk => {
      chunk.toString().split('\n').filter(Boolean)
        .forEach(line => send('log', { type, line, app: appName }));
    });
    tails.push(tail);
  }

  req.on('close', () => tails.forEach(t => t.kill()));
});

app.listen(PORT, '127.0.0.1', () => {
  console.log(`Log viewer listening on http://127.0.0.1:${PORT}`);
});
