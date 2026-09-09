/* FireLite Console — vanilla JS, no build step. */
'use strict';

const $ = (sel, root) => (root || document).querySelector(sel);
const esc = (s) => String(s == null ? '' : s)
  .replace(/&/g, '&amp;').replace(/</g, '&lt;')
  .replace(/>/g, '&gt;').replace(/"/g, '&quot;');

let ME = null; // {username, role}
let ES = null; // shared EventSource for the dashboard log

async function api(path, opts) {
  opts = opts || {};
  const res = await fetch(path, Object.assign({
    credentials: 'include',
    headers: { 'Content-Type': 'application/json' },
  }, opts));
  if (res.status === 401) {
    ME = null;
    showLogin();
    throw new Error('unauthorized');
  }
  let body = null;
  try { body = await res.json(); } catch (e) { body = {}; }
  if (!res.ok) throw new Error((body && body.error) || ('HTTP ' + res.status));
  return body;
}

function toast(msg, isErr) {
  const d = document.createElement('div');
  d.className = 'toast' + (isErr ? ' err' : '');
  d.textContent = msg;
  $('#toast').appendChild(d);
  setTimeout(() => d.remove(), 6000);
}

function table(headers, rows) {
  let h = '<table><thead><tr>' + headers.map((c) => '<th>' + esc(c) + '</th>').join('') + '</tr></thead><tbody>';
  for (const r of rows) h += '<tr>' + r.map((c) => '<td>' + c + '</td>').join('') + '</tr>';
  return h + '</tbody></table>';
}
const mono = (s) => '<span class="mono">' + esc(s) + '</span>';

// ---------------------------------------------------------------- boot

async function boot() {
  window.addEventListener('hashchange', router);
  $('#logout').addEventListener('click', async () => {
    try { await api('/api/logout', { method: 'POST', body: '{}' }); } catch (e) {}
    ME = null;
    showLogin();
  });
  try {
    const s = await (await fetch('/api/setup/status', { credentials: 'include' })).json();
    if (s.setup_required) return showSetup();
  } catch (e) { /* fall through to login attempt */ }
  try {
    ME = await api('/api/me');
    showApp();
  } catch (e) {
    if (!ME) showLogin();
  }
}

function showSetup() {
  $('#nav').hidden = true;
  $('#view').innerHTML =
    '<div id="setup-wrap"><h2>First run setup</h2>' +
    '<p class="muted">No admin exists yet. Create the initial administrator. This page never appears again afterwards.</p>' +
    '<form id="f" class="grid">' +
    '<label>Username</label><input id="u" autocomplete="username">' +
    '<label>Password</label><input id="p" type="password" autocomplete="new-password">' +
    '<span></span><button class="btn" type="submit">Create admin</button>' +
    '</form><p id="e" class="err"></p></div>';
  $('#f').addEventListener('submit', async (ev) => {
    ev.preventDefault();
    try {
      ME = await api('/api/setup', { method: 'POST', body: JSON.stringify({ username: $('#u').value, password: $('#p').value }) });
      showApp();
    } catch (err) { $('#e').textContent = err.message; }
  });
}

function showLogin() {
  if (ES) { ES.close(); ES = null; }
  $('#nav').hidden = true;
  $('#view').innerHTML =
    '<div id="login-wrap"><h2>FireLite Console</h2>' +
    '<p class="muted">Sign in with an admin-console account.</p>' +
    '<form id="f" class="grid">' +
    '<label>Username</label><input id="u" autocomplete="username">' +
    '<label>Password</label><input id="p" type="password" autocomplete="current-password">' +
    '<span></span><button class="btn" type="submit">Sign in</button>' +
    '</form><p id="e" class="err"></p></div>';
  $('#f').addEventListener('submit', async (ev) => {
    ev.preventDefault();
    try {
      ME = await api('/api/login', { method: 'POST', body: JSON.stringify({ username: $('#u').value, password: $('#p').value }) });
      showApp();
    } catch (err) { $('#e').textContent = err.message; }
  });
}

function showApp() {
  $('#nav').hidden = false;
  $('#whoami').textContent = ME.username + ' · ' + ME.role;
  document.querySelectorAll('nav a').forEach((a) => {
    const adminOnly = a.getAttribute('href') === '#/users';
    a.style.display = (adminOnly && ME.role !== 'admin') ? 'none' : '';
  });
  if (!location.hash) location.hash = '#/dashboard';
  router();
}

function router() {
  const h = location.hash || '#/dashboard';
  document.querySelectorAll('nav a').forEach((a) => a.classList.toggle('active', a.getAttribute('href') === h));
  const v = $('#view');
  if (h === '#/dashboard') return vDashboard(v);
  if (h === '#/groups') return vGroups(v);
  if (h === '#/users') return vUsers(v);
  if (h === '#/data') return vData(v);
  if (h === '#/indexes') return vIndexes(v);
  if (h === '#/maintenance') return vMaintenance(v);
  if (h === '#/audit') return vAudit(v);
  if (h === '#/config') return vConfig(v);
  v.innerHTML = '<h2>Not found</h2>';
}

// ---------------------------------------------------------------- dashboard

async function vDashboard(v) {
  v.innerHTML = '<h2>Dashboard</h2><div id="c"></div><h3>Live events</h3><ul id="eventlog"></ul>';
  const c = $('#c', v);
  try {
    const s = await api('/api/status');
    const cards = [
      [s.version, 'version'],
      [s.collections, 'collections'],
      [s.sync ? s.sync.active_clients : '—', 'sync peers'],
      [s.sync ? s.sync.hosted_rooms : '—', 'rooms hosted'],
    ];
    c.innerHTML = '<div class="cards">' + cards.map((x) => '<div class="card"><b>' + esc(x[0]) + '</b><span>' + esc(x[1]) + '</span></div>').join('') + '</div>';
    if (s.sync && s.sync.peers.length) {
      c.innerHTML += '<h3>Peers</h3>' + table(['peer', 'prefix'], s.sync.peers.map((p) => [mono(p.peer_key), mono(p.prefix)]));
    }
    const r = await api('/api/rooms');
    if (r.rooms.length) {
      c.innerHTML += '<h3>Rooms</h3>' + table(
        ['room', 'prefix', 'peers', 'collections'],
        r.rooms.map((x) => [esc(x.room_name), mono(x.prefix), esc((x.peers || []).join(', ')), esc(Object.keys(x.collections || {}).length)])
      );
    }
  } catch (e) { c.innerHTML = '<p class="err">' + esc(e.message) + '</p>'; }
  if (ES) ES.close();
  ES = new EventSource('/api/events');
  const log = $('#eventlog', v);
  const add = (kind, obj) => {
    const li = document.createElement('li');
    li.innerHTML = '<span class="pill">' + esc(kind) + '</span> ' + esc(JSON.stringify(obj));
    log.prepend(li);
    while (log.children.length > 100) log.lastChild.remove();
  };
  ES.addEventListener('doc', (e) => add('doc', JSON.parse(e.data)));
  ES.addEventListener('peers', (e) => add('peers', JSON.parse(e.data)));
  ES.addEventListener('versions', (e) => add('versions', JSON.parse(e.data)));
  ES.onerror = () => {};
}

// ---------------------------------------------------------------- groups

async function vGroups(v) {
  v.innerHTML = '<h2>Groups &amp; keys</h2><div id="g"></div>' +
    '<h3>New group</h3><form id="f" class="grid">' +
    '<label>Room name</label><input id="room">' +
    '<label>Mode</label><select id="mode"><option value="open">open</option><option value="registered">registered</option></select>' +
    '<span></span><button class="btn" type="submit">Create</button></form><div id="newkey"></div>';
  const refresh = async () => {
    try {
      const r = await api('/api/groups');
      $('#g', v).innerHTML = r.groups.length ? table(
        ['room', 'mode', 'members', 'key?', 'actions'],
        r.groups.map((g) => [
          esc(g.room_name),
          '<span class="pill">' + esc(g.mode) + '</span>',
          esc((g.members || []).join(', ')),
          g.has_key ? 'set' : '—',
          '<button class="btn" data-rotate="' + esc(g.room_name) + '">rotate key</button> ' +
          '<button class="btn" data-mode="' + esc(g.room_name) + '">toggle mode</button> ' +
          '<button class="btn danger" data-del="' + esc(g.room_name) + '">delete</button>',
        ])
      ) : '<p class="muted">No groups. Absent groups are open by default.</p>';
      v.querySelectorAll('[data-rotate]').forEach((b) => b.addEventListener('click', async () => {
        try {
          const r2 = await api('/api/groups/' + encodeURIComponent(b.dataset.rotate) + '/rotate-key', { method: 'POST' });
          $('#newkey', v).innerHTML = '<p>New key (shown once):</p><div class="keybox">' + esc(r2.api_key) + '</div>';
          refresh();
        } catch (e) { toast(e.message, true); }
      }));
      v.querySelectorAll('[data-mode]').forEach((b) => b.addEventListener('click', async () => {
        try {
          const cur = (await api('/api/groups/' + encodeURIComponent(b.dataset.mode))).group;
          await api('/api/groups/' + encodeURIComponent(b.dataset.mode) + '/mode', { method: 'PUT', body: JSON.stringify({ mode: cur.mode === 'open' ? 'registered' : 'open' }) });
          refresh();
        } catch (e) { toast(e.message, true); }
      }));
      v.querySelectorAll('[data-del]').forEach((b) => b.addEventListener('click', async () => {
        if (!confirm('Delete group ' + b.dataset.del + '? (Room data is untouched; admission falls back to open.)')) return;
        try { await api('/api/groups/' + encodeURIComponent(b.dataset.del), { method: 'DELETE' }); refresh(); }
        catch (e) { toast(e.message, true); }
      }));
    } catch (e) { $('#g', v).innerHTML = '<p class="err">' + esc(e.message) + '</p>'; }
  };
  $('#f', v).addEventListener('submit', async (ev) => {
    ev.preventDefault();
    try {
      const r = await api('/api/groups', { method: 'POST', body: JSON.stringify({ room_name: $('#room', v).value, mode: $('#mode', v).value }) });
      if (r.api_key) $('#newkey', v).innerHTML = '<p>API key (shown once — copy now):</p><div class="keybox">' + esc(r.api_key) + '</div>';
      refresh();
    } catch (e) { toast(e.message, true); }
  });
  refresh();
}

// ---------------------------------------------------------------- users

async function vUsers(v) {
  v.innerHTML = '<h2>Users</h2><div id="u"></div>' +
    '<h3>New user</h3><form id="f" class="grid">' +
    '<label>Username</label><input id="un">' +
    '<label>Password</label><input id="pw" type="password">' +
    '<label>Role</label><select id="rl"><option>viewer</option><option>operator</option><option>admin</option></select>' +
    '<span></span><button class="btn" type="submit">Create</button></form>';
  const refresh = async () => {
    try {
      const r = await api('/api/users');
      $('#u', v).innerHTML = table(
        ['username', 'role', 'disabled', 'actions'],
        r.users.map((u) => [
          esc(u.username),
          '<span class="pill ' + esc(u.role) + '">' + esc(u.role) + '</span>',
          u.disabled ? 'yes' : 'no',
          '<button class="btn" data-toggle="' + esc(u.username) + '">' + (u.disabled ? 'enable' : 'disable') + '</button> ' +
          '<button class="btn danger" data-del="' + esc(u.username) + '">delete</button>',
        ])
      );
      v.querySelectorAll('[data-toggle]').forEach((b) => b.addEventListener('click', async () => {
        try {
          const cur = (await api('/api/users')).users.find((x) => x.username === b.dataset.toggle);
          await api('/api/users/' + encodeURIComponent(b.dataset.toggle), { method: 'PUT', body: JSON.stringify({ disabled: !cur.disabled }) });
          refresh();
        } catch (e) { toast(e.message, true); }
      }));
      v.querySelectorAll('[data-del]').forEach((b) => b.addEventListener('click', async () => {
        if (!confirm('Delete user ' + b.dataset.del + '?')) return;
        try { await api('/api/users/' + encodeURIComponent(b.dataset.del), { method: 'DELETE' }); refresh(); }
        catch (e) { toast(e.message, true); }
      }));
    } catch (e) { $('#u', v).innerHTML = '<p class="err">' + esc(e.message) + '</p>'; }
  };
  $('#f', v).addEventListener('submit', async (ev) => {
    ev.preventDefault();
    try {
      await api('/api/users', { method: 'POST', body: JSON.stringify({ username: $('#un', v).value, password: $('#pw', v).value, role: $('#rl', v).value }) });
      refresh();
    } catch (e) { toast(e.message, true); }
  });
  refresh();
}

// ---------------------------------------------------------------- data

const OPS = ['==', '!=', '>', '>=', '<', '<=', 'in', 'not-in', 'contains', 'starts-with', 'match', 'match-prefix', 'array-contains', 'array-contains-any'];

async function vData(v) {
  v.innerHTML = '<h2>Data</h2>' +
    '<div class="row"><label>Collection</label><select id="col"></select>' +
    '<label>Limit</label><input id="lim" value="50" size="5">' +
    '<button class="btn" id="run">Query</button></div>' +
    '<div id="filters"></div>' +
    '<div class="row"><button class="btn" id="addf">+ filter</button></div>' +
    '<div id="rows"></div>' +
    '<h3>Document</h3><div class="row"><input id="docid" placeholder="doc id">' +
    '<button class="btn" id="load">Load</button>' +
    '<button class="btn" id="save">Save</button>' +
    '<button class="btn danger" id="del">Delete</button></div>' +
    '<textarea id="doc" spellcheck="false" placeholder="{ ... }"></textarea>';
  const cols = await api('/api/collections');
  $('#col', v).innerHTML = cols.collections.map((c) => '<option>' + esc(c.name) + '</option>').join('');
  const addFilter = () => {
    const d = document.createElement('div');
    d.className = 'row';
    d.innerHTML = '<input placeholder="field" size="12"> <select>' + OPS.map((o) => '<option>' + o + '</option>').join('') + '</select> ' +
      '<input placeholder="value (JSON)" size="24"> <button class="btn" type="button">×</button>';
    d.querySelector('button').addEventListener('click', () => d.remove());
    $('#filters', v).appendChild(d);
  };
  $('#addf', v).addEventListener('click', addFilter);
  addFilter();
  const parseVal = (s) => { try { return JSON.parse(s); } catch (e) { return s; } };
  $('#run', v).addEventListener('click', async () => {
    const filters = [...$('#filters', v).children].map((d) => {
      const [f, o, val] = [d.children[0].value, d.children[1].value, d.children[2].value];
      return f ? { field: f, op: o, value: parseVal(val) } : null;
    }).filter(Boolean);
    try {
      const r = await api('/api/query', { method: 'POST', body: JSON.stringify({ collection: $('#col', v).value, filters, limit: parseInt($('#lim', v).value) || 50 }) });
      $('#rows', v).innerHTML = r.rows.length ? table(
        ['id', 'document'],
        r.rows.map((x) => ['<a href="#" data-id="' + esc(x.id) + '">' + esc(x.id) + '</a>', '<span class="mono">' + esc(JSON.stringify(x)) + '</span>'])
      ) + '<p class="muted">' + r.count + ' rows</p>' : '<p class="muted">No rows.</p>';
      v.querySelectorAll('[data-id]').forEach((a) => a.addEventListener('click', (ev) => {
        ev.preventDefault();
        $('#docid', v).value = a.dataset.id;
        $('#load', v).click();
      }));
    } catch (e) { toast(e.message, true); }
  });
  $('#load', v).addEventListener('click', async () => {
    try {
      const r = await api('/api/docs/' + encodeURIComponent($('#col', v).value) + '/' + encodeURIComponent($('#docid', v).value));
      $('#doc', v).value = JSON.stringify(r, null, 2);
    } catch (e) { toast(e.message, true); }
  });
  $('#save', v).addEventListener('click', async () => {
    try {
      const data = JSON.parse($('#doc', v).value);
      await api('/api/docs/' + encodeURIComponent($('#col', v).value) + '/' + encodeURIComponent($('#docid', v).value), { method: 'PUT', body: JSON.stringify({ data }) });
      toast('Saved');
      $('#run', v).click();
    } catch (e) { toast(e.message, true); }
  });
  $('#del', v).addEventListener('click', async () => {
    if (!confirm('Delete ' + $('#docid', v).value + '?')) return;
    try {
      await api('/api/docs/' + encodeURIComponent($('#col', v).value) + '/' + encodeURIComponent($('#docid', v).value), { method: 'DELETE' });
      toast('Deleted');
      $('#run', v).click();
    } catch (e) { toast(e.message, true); }
  });
}

// ---------------------------------------------------------------- indexes

async function vIndexes(v) {
  v.innerHTML = '<h2>Indexes</h2><div id="l"></div>' +
    '<h3>Create index</h3><form id="f" class="grid">' +
    '<label>Collection</label><input id="col">' +
    '<label>Kind</label><select id="kind"><option value="simple">simple</option><option value="fts">fts</option><option value="composite">composite</option></select>' +
    '<label>Field(s)</label><input id="fields" placeholder="age or age:desc,name:asc">' +
    '<span></span><button class="btn" type="submit">Create</button></form>';
  const refresh = async () => {
    try {
      const r = await api('/api/indexes');
      $('#l', v).innerHTML = '<pre>' + esc(JSON.stringify(r.indexes, null, 2)) + '</pre>';
    } catch (e) { $('#l', v).innerHTML = '<p class="err">' + esc(e.message) + '</p>'; }
  };
  $('#f', v).addEventListener('submit', async (ev) => {
    ev.preventDefault();
    const kind = $('#kind', v).value, col = $('#col', v).value, raw = $('#fields', v).value;
    try {
      if (kind === 'composite') {
        const fields = raw.split(',').map((s) => {
          const [field, dir] = s.split(':');
          return { field: field.trim(), desc: (dir || '').trim() === 'desc' };
        });
        await api('/api/indexes', { method: 'POST', body: JSON.stringify({ kind, collection: col, fields }) });
      } else {
        await api('/api/indexes', { method: 'POST', body: JSON.stringify({ kind, collection: col, field: raw.trim() }) });
      }
      toast('Index created');
      refresh();
    } catch (e) { toast(e.message, true); }
  });
  refresh();
}

// ---------------------------------------------------------------- maintenance / audit / config

async function vMaintenance(v) {
  v.innerHTML = '<h2>Maintenance</h2>' +
    '<div class="row"><label>Backup path</label><input id="bp" size="40" placeholder="/var/backups/fl.db">' +
    '<button class="btn" id="dobackup">Backup</button></div>' +
    '<div class="row"><button class="btn" id="docompact">Compact all shards</button>' +
    '<input id="vcol" placeholder="collection"><button class="btn" id="dovac">Vacuum tombstones</button></div>' +
    '<div id="out"></div>';
  const say = (m, err) => { $('#out', v).innerHTML = '<p class="' + (err ? 'err' : 'ok') + '">' + esc(m) + '</p>'; };
  $('#dobackup', v).addEventListener('click', async () => {
    try { await api('/api/backup', { method: 'POST', body: JSON.stringify({ path: $('#bp', v).value }) }); say('Backup written.'); }
    catch (e) { say(e.message, true); }
  });
  $('#docompact', v).addEventListener('click', async () => {
    try { await api('/api/compact', { method: 'POST' }); say('Compact done.'); }
    catch (e) { say(e.message, true); }
  });
  $('#dovac', v).addEventListener('click', async () => {
    try {
      const r = await api('/api/vacuum', { method: 'POST', body: JSON.stringify({ collection: $('#vcol', v).value }) });
      say('Vacuumed ' + r.tombstones + ' tombstones.');
    } catch (e) { say(e.message, true); }
  });
}

async function vAudit(v) {
  v.innerHTML = '<h2>Audit log</h2><div id="a"></div>';
  try {
    const r = await api('/api/audit');
    const rows = (r.entries || []).slice().reverse();
    $('#a', v).innerHTML = rows.length ? table(
      ['op', 'collection', 'doc', 'ok'],
      rows.slice(0, 500).map((e) => [esc(e.op), esc(e.collection || ''), esc(e.doc_id || ''), e.ok ? '<span class="ok">ok</span>' : '<span class="err">fail</span>'])
    ) : '<p class="muted">Empty (audit logging is off unless enabled in server config).</p>';
  } catch (e) { $('#a', v).innerHTML = '<p class="err">' + esc(e.message) + '</p>'; }
}

async function vConfig(v) {
  v.innerHTML = '<h2>Config</h2><div id="c"></div>';
  try {
    const r = await api('/api/config');
    $('#c', v).innerHTML = '<pre>' + esc(JSON.stringify(r, null, 2)) + '</pre>';
  } catch (e) { $('#c', v).innerHTML = '<p class="err">' + esc(e.message) + '</p>'; }
}

document.addEventListener('DOMContentLoaded', boot);
