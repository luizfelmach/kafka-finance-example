(function() {
  const BASE = '';

  function rand(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }
  function pick(arr) { return arr[rand(0, arr.length - 1)]; }
  function uid() { return 'u-' + String(rand(0, 99)).padStart(3, '0'); }
  function txId() { return 'tx-' + Math.random().toString(36).slice(2, 10); }
  function authId() { return 'auth-' + Math.random().toString(36).slice(2, 10); }
  function device(uid) { return uid === 'random' ? 'dev-unknown-' + rand(100,999) : 'dev-' + uid + '-0'; }
  function ip() { return '177.10.' + rand(1,255) + '.' + rand(1,255); }

  const FARAWAY_DESTINATIONS = [
    { label: 'Tokyo', lat: 35.7, lon: 139.7 },
    { label: 'Moscow', lat: 55.8, lon: 37.6 },
    { label: 'Sydney', lat: -33.9, lon: 151.2 },
    { label: 'London', lat: 51.5, lon: -0.1 },
    { label: 'Dubai', lat: 25.2, lon: 55.3 }
  ];

  const FRAUDS = [
    {
      id: 'high-amount', label: 'High Amount',
      desc: 'Transaction > R$50k',
      run: () => postTx({ amount: 80000 + Math.random() * 70000 })
    },
    {
      id: 'burst', label: 'Burst',
      desc: '5 rapid transactions',
      run: async () => {
        for (let i = 0; i < 5; i++) {
          await postTx({ amount: 150 + Math.random() * 50 });
          await sleep(200);
        }
      }
    },
    {
      id: 'unknown-device', label: 'Unknown Device',
      desc: 'Auth from unknown device',
      run: () => postAuth({ deviceId: device('random'), eventType: 'login' })
    },
    {
      id: 'password-change', label: 'Password Change',
      desc: 'Password change event',
      run: () => postAuth({ eventType: 'password_change' })
    },
    {
      id: 'account-takeover', label: 'Account Takeover',
      desc: 'Login → pw change → high tx',
      run: async () => {
        const u = uid();
        await postAuth({ userId: u, deviceId: device('random'), eventType: 'login' });
        await sleep(300);
        await postAuth({ userId: u, eventType: 'password_change', recentFailedAttempts: 3 });
        await sleep(300);
        await postTx({ userId: u, amount: 50000 + Math.random() * 30000 });
      }
    },
    {
      id: 'emptying-account', label: 'Emptying Account',
      desc: '3+ high value transactions',
      run: async () => {
        const u = uid();
        for (let i = 0; i < 4; i++) {
          await postTx({ userId: u, amount: 3000 + Math.random() * 2000 });
          await sleep(200);
        }
      }
    },
    {
      id: 'parallel-login', label: 'Parallel Login',
      desc: '2 logins different devices',
      run: async () => {
        const u = uid();
        await postAuth({ userId: u, deviceId: device(u), eventType: 'login', latitude: -23.5, longitude: -46.6 });
        await sleep(100);
        await postAuth({ userId: u, deviceId: device('random'), eventType: 'login', latitude: -8.0, longitude: -34.9 });
      }
    },
    {
      id: 'faraway-login', label: 'Faraway Login',
      desc: 'SP → random destination',
      run: async () => {
        const u = uid();
        const dest = pick(FARAWAY_DESTINATIONS);
        await postAuth({ userId: u, deviceId: device(u), eventType: 'login', latitude: -23.5, longitude: -46.6 });
        await sleep(500);
        await postAuth({ userId: u, deviceId: device(u), eventType: 'login', latitude: dest.lat, longitude: dest.lon });
      }
    },
    {
      id: 'under-observation', label: 'Under Observation',
      desc: '5+ transactions same account',
      run: async () => {
        const u = uid();
        for (let i = 0; i < 6; i++) {
          await postTx({ userId: u, amount: 200 + Math.random() * 100 });
          await sleep(150);
        }
      }
    }
  ];

  function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

  function log(msg, type) {
    const el = document.getElementById('simLog');
    const entry = document.createElement('div');
    entry.className = 'log-entry ' + (type || 'info');
    entry.textContent = '> ' + msg;
    el.appendChild(entry);
    el.scrollTop = el.scrollHeight;
  }

  async function postTx(overrides) {
    const u = overrides.userId || uid();
    const tx = {
      transactionId: txId(),
      accountId: 'acc-' + u + '-0',
      userId: u,
      type: pick(['PIX','CRED','DEB']),
      amount: +(overrides.amount || (150 + Math.random() * 50)).toFixed(2),
      deviceId: overrides.deviceId || device(u),
      ipAddress: ip(),
      destinationAccount: 'acc-' + uid() + '-0',
      timestamp: Date.now()
    };
    const r = await fetch(BASE + '/api/events/transaction', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify(tx)
    });
    if (r.ok) log('TX  R$' + tx.amount.toFixed(2) + ' | ' + tx.userId + ' | ' + tx.transactionId, 'ok');
    else log('TX  FAILED: ' + (await r.text()), 'err');
    return r;
  }

  async function postAuth(overrides) {
    const u = overrides.userId || uid();
    const lat = overrides.latitude !== undefined ? overrides.latitude : -23.5 + Math.random() * 4;
    const lon = overrides.longitude !== undefined ? overrides.longitude : -46.6 + Math.random() * 4;
    const auth = {
      eventId: authId(),
      userId: u,
      eventType: overrides.eventType || 'login',
      deviceId: overrides.deviceId || device(u),
      ipAddress: ip(),
      latitude: lat,
      longitude: lon,
      timestamp: Date.now()
    };
    if (overrides.recentFailedAttempts !== undefined) auth.recentFailedAttempts = overrides.recentFailedAttempts;
    const r = await fetch(BASE + '/api/events/auth', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify(auth)
    });
    if (r.ok) log('AUTH ' + auth.eventType + ' | ' + auth.userId + ' | ' + auth.eventId, 'ok');
    else log('AUTH FAILED: ' + (await r.text()), 'err');
    return r;
  }

  function generateTxForm() {
    const u = uid();
    document.getElementById('tx-txId').value = txId();
    document.getElementById('tx-accountId').value = 'acc-' + u + '-0';
    document.getElementById('tx-userId').value = u;
    document.getElementById('tx-type').value = 'PIX';
    document.getElementById('tx-amount').value = '';
    document.getElementById('tx-deviceId').value = device(u);
    document.getElementById('tx-ip').value = ip();
    document.getElementById('tx-dest').value = 'acc-' + uid() + '-0';
  }

  function initForms() {
    const txForm = document.getElementById('tx-form');
    txForm.querySelector('h2').after(Object.assign(document.createElement('button'), {
      type: 'button', className: 'btn btn-sm', textContent: 'Generate',
      onclick: generateTxForm
    }));
    txForm.addEventListener('submit', async e => {
      e.preventDefault();
      const el = document.getElementById('tx-result');
      const amountVal = document.getElementById('tx-amount').value;
      if (!amountVal) {
        el.className = 'form-result err';
        el.textContent = '✗ Fill in or generate the amount';
        return;
      }
      const tx = {
        transactionId: document.getElementById('tx-txId').value,
        accountId: document.getElementById('tx-accountId').value,
        userId: document.getElementById('tx-userId').value,
        type: document.getElementById('tx-type').value,
        amount: parseFloat(amountVal),
        deviceId: document.getElementById('tx-deviceId').value,
        ipAddress: document.getElementById('tx-ip').value,
        destinationAccount: document.getElementById('tx-dest').value,
        timestamp: Date.now()
      };
      const r = await fetch(BASE + '/api/events/transaction', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify(tx)
      });
      if (r.ok) { el.className = 'form-result ok'; el.textContent = '✓ Transaction sent: ' + tx.transactionId; }
      else { el.className = 'form-result err'; el.textContent = '✗ ' + (await r.text()); }
    });

    const authForm = document.getElementById('auth-form');
    let authTab = 'login';

    authForm.querySelectorAll('[data-auth-tab]').forEach(btn => {
      btn.addEventListener('click', () => {
        authTab = btn.dataset.authTab;
        authForm.querySelectorAll('[data-auth-tab]').forEach(b => b.classList.toggle('active', b === btn));
        authForm.querySelectorAll('.auth-panel').forEach(p => p.classList.remove('active'));
        document.getElementById('auth-' + authTab).classList.add('active');
        document.getElementById('auth-result').className = 'form-result';
        document.getElementById('auth-result').textContent = '';
      });
    });

    authForm.addEventListener('submit', async e => {
      e.preventDefault();
      const el = document.getElementById('auth-result');

      if (authTab === 'login') {
        const userId = document.getElementById('login-userId').value.trim();
        if (!userId) { el.className = 'form-result err'; el.textContent = '✗ Enter a username'; return; }
        const remember = document.getElementById('login-remember').checked;
        const pw = document.getElementById('login-password').value;

        const auth = {
          eventId: authId(),
          userId: userId,
          eventType: 'login',
          deviceId: remember ? device(userId) : device('random'),
          ipAddress: ip(),
          latitude: +(-23.5 + Math.random() * 4).toFixed(4),
          longitude: +(-46.6 + Math.random() * 4).toFixed(4),
          timestamp: Date.now()
        };

        const r = await fetch(BASE + '/api/events/auth', {
          method: 'POST', headers: {'Content-Type':'application/json'},
          body: JSON.stringify(auth)
        });
        if (r.ok) {
          el.className = 'form-result ok';
          el.textContent = '✓ ' + userId + ' logged in' + (pw ? '' : '') + (remember ? ' (trusted device)' : ' (unknown device)');
        } else {
          el.className = 'form-result err';
          el.textContent = '✗ ' + (await r.text());
        }
      } else {
        const userId = document.getElementById('pw-userId').value.trim();
        if (!userId) { el.className = 'form-result err'; el.textContent = '✗ Enter a username'; return; }
        const pwNew = document.getElementById('pw-new').value;
        const pwConfirm = document.getElementById('pw-confirm').value;
        if (pwNew !== pwConfirm) { el.className = 'form-result err'; el.textContent = '✗ Passwords do not match'; return; }

        const auth = {
          eventId: authId(),
          userId: userId,
          eventType: 'password_change',
          deviceId: device(userId),
          ipAddress: ip(),
          latitude: +(-23.5 + Math.random() * 4).toFixed(4),
          longitude: +(-46.6 + Math.random() * 4).toFixed(4),
          timestamp: Date.now()
        };

        const r = await fetch(BASE + '/api/events/auth', {
          method: 'POST', headers: {'Content-Type':'application/json'},
          body: JSON.stringify(auth)
        });
        if (r.ok) {
          el.className = 'form-result ok';
          el.textContent = '✓ Password changed for ' + userId;
        } else {
          el.className = 'form-result err';
          el.textContent = '✗ ' + (await r.text());
        }
      }
    });

    generateTxForm();
  }

  function initSimulator() {
    const grid = document.getElementById('simButtons');
    FRAUDS.forEach(f => {
      const btn = document.createElement('button');
      btn.className = 'sim-btn';
      btn.innerHTML = `<strong>${f.label}</strong><br><small>${f.desc}</small>`;
      btn.addEventListener('click', async () => {
        btn.disabled = true;
        log('Starting ' + f.label + '...', 'info');
        try { await f.run(); } catch (err) { log(f.label + ' failed: ' + err.message, 'err'); }
        log(f.label + ' done', 'info');
        btn.disabled = false;
      });
      grid.appendChild(btn);
    });
  }

  document.addEventListener('DOMContentLoaded', () => {
    initSimulator();
    initForms();
  });
})();
