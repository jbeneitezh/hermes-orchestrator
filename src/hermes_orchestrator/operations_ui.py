from __future__ import annotations

# ruff: noqa: E501

OPERATIONS_DASHBOARD_HTML = r"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Hermes // Mesa de operaciones</title>
  <style>
    :root {
      --ink: #172019;
      --paper: #f1efe5;
      --paper-deep: #e1ddcd;
      --signal: #d7ff43;
      --orange: #ff6b35;
      --muted: #687069;
      --line: rgba(23, 32, 25, .22);
      --dark: #101611;
    }
    * { box-sizing: border-box; }
    html { background: var(--dark); color: var(--ink); }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: "Aptos Narrow", "Arial Narrow", sans-serif;
      background:
        linear-gradient(90deg, transparent 31px, rgba(23,32,25,.055) 32px, transparent 33px),
        linear-gradient(0deg, transparent 31px, rgba(23,32,25,.055) 32px, transparent 33px),
        var(--paper);
      background-size: 32px 32px;
    }
    button, select { font: inherit; }
    .shell { min-height: 100vh; display: grid; grid-template-columns: 80px 1fr; }
    .rail {
      position: sticky; top: 0; height: 100vh; padding: 20px 14px;
      background: var(--dark); color: var(--paper); display: flex;
      flex-direction: column; justify-content: space-between; align-items: center;
      border-right: 1px solid #384239;
    }
    .mark { font: 700 28px/1 Georgia, serif; color: var(--signal); transform: rotate(-8deg); }
    .rail-word { writing-mode: vertical-rl; transform: rotate(180deg); letter-spacing: .24em; font-size: 11px; text-transform: uppercase; }
    .pulse { width: 10px; height: 10px; border-radius: 50%; background: var(--signal); box-shadow: 0 0 0 7px rgba(215,255,67,.13); }
    main { padding: 28px clamp(18px, 4vw, 64px) 64px; overflow: hidden; }
    header { display: grid; grid-template-columns: 1fr auto; gap: 28px; align-items: end; border-bottom: 3px solid var(--ink); padding-bottom: 20px; }
    .eyebrow { font: 700 11px/1.2 monospace; letter-spacing: .17em; text-transform: uppercase; color: var(--muted); }
    h1 { margin: 7px 0 0; max-width: 850px; font: 600 clamp(40px, 7vw, 92px)/.84 Georgia, serif; letter-spacing: -.065em; }
    h1 em { color: var(--orange); font-style: italic; }
    .stamp { min-width: 190px; padding: 12px; border: 1px solid var(--ink); background: var(--signal); font: 700 12px/1.45 monospace; text-transform: uppercase; box-shadow: 6px 6px 0 var(--ink); }
    .toolbar { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; padding: 18px 0 22px; }
    .toolbar button, .toolbar select { border: 1px solid var(--ink); background: transparent; padding: 9px 12px; color: var(--ink); }
    .toolbar button { background: var(--ink); color: var(--paper); cursor: pointer; text-transform: uppercase; letter-spacing: .08em; font-weight: 700; }
    .toolbar button:hover, .toolbar button:focus-visible { background: var(--orange); outline: 2px solid var(--ink); outline-offset: 2px; }
    .meta { margin-left: auto; font: 12px monospace; color: var(--muted); }
    .strip { display: grid; grid-template-columns: repeat(5, minmax(120px, 1fr)); border: 1px solid var(--ink); background: var(--paper); }
    .metric { min-height: 98px; padding: 15px; border-right: 1px solid var(--ink); position: relative; }
    .metric:last-child { border-right: 0; }
    .metric span { display: block; font: 700 10px monospace; letter-spacing: .12em; text-transform: uppercase; color: var(--muted); }
    .metric strong { display: block; margin-top: 12px; font: 600 34px/1 Georgia, serif; }
    .metric[data-alert="true"]::after { content: "!"; position: absolute; top: 8px; right: 10px; color: var(--orange); font: 800 26px Georgia, serif; }
    .grid { display: grid; grid-template-columns: minmax(0, 1.5fr) minmax(290px, .7fr); gap: 18px; margin-top: 18px; }
    .panel { border-top: 5px solid var(--ink); background: rgba(241,239,229,.88); min-width: 0; }
    .panel-head { padding: 12px 0 10px; display: flex; justify-content: space-between; gap: 16px; align-items: baseline; }
    h2 { margin: 0; font: 600 25px/1 Georgia, serif; letter-spacing: -.03em; }
    .counter { font: 11px monospace; color: var(--muted); }
    .table-wrap { overflow-x: auto; border: 1px solid var(--ink); }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th { background: var(--ink); color: var(--paper); padding: 9px 10px; text-align: left; font: 700 10px monospace; text-transform: uppercase; letter-spacing: .08em; }
    td { padding: 10px; border-bottom: 1px solid var(--line); vertical-align: top; }
    tbody tr:last-child td { border-bottom: 0; }
    tbody tr:hover { background: #fff; }
    .status { display: inline-flex; gap: 6px; align-items: center; font: 700 10px monospace; text-transform: uppercase; }
    .status::before { content: ""; width: 7px; height: 7px; border-radius: 50%; background: var(--signal); border: 1px solid var(--ink); }
    .status.bad::before, .status.stale::before { background: var(--orange); }
    .objective { max-width: 440px; font-weight: 650; }
    .mono { font-family: monospace; font-size: 11px; }
    .timeline { border: 1px solid var(--ink); background: var(--dark); color: var(--paper); padding: 4px 15px; max-height: 430px; overflow-y: auto; }
    .event { display: grid; grid-template-columns: 82px 1fr; gap: 12px; padding: 13px 0; border-bottom: 1px solid #354137; }
    .event:last-child { border-bottom: 0; }
    .event time { font: 10px monospace; color: #a7b1a8; }
    .event b { display: block; color: var(--signal); font: 700 11px monospace; letter-spacing: .04em; }
    .event p { margin: 5px 0 0; color: #d5dbd5; font-size: 12px; overflow-wrap: anywhere; }
    .governance { display: grid; gap: 10px; }
    .notice { border: 1px solid var(--ink); padding: 13px; background: var(--paper-deep); }
    .notice.alert { background: var(--orange); color: var(--dark); }
    .notice h3 { margin: 0 0 6px; font: 700 11px monospace; text-transform: uppercase; }
    .notice p { margin: 0; font-size: 13px; }
    .footer-note { margin-top: 26px; display: flex; justify-content: space-between; gap: 20px; padding-top: 12px; border-top: 1px solid var(--ink); font: 10px monospace; color: var(--muted); text-transform: uppercase; letter-spacing: .08em; }
    .empty { padding: 22px; color: var(--muted); font-style: italic; }
    @keyframes reveal { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: none; } }
    header, .toolbar, .strip, .grid { animation: reveal .45s both; }
    .toolbar { animation-delay: .06s; } .strip { animation-delay: .12s; } .grid { animation-delay: .18s; }
    @media (max-width: 880px) {
      .shell { grid-template-columns: 50px 1fr; }
      .rail { padding-inline: 8px; }
      header { grid-template-columns: 1fr; }
      .stamp { justify-self: start; }
      .strip { grid-template-columns: repeat(2, 1fr); }
      .metric { border-bottom: 1px solid var(--ink); }
      .grid { grid-template-columns: 1fr; }
      .meta { width: 100%; margin-left: 0; }
    }
    @media (prefers-reduced-motion: reduce) { * { animation: none !important; } }
  </style>
</head>
<body>
<div class="shell">
  <aside class="rail" aria-label="Identidad de la mesa de operaciones">
    <div class="mark">H.</div><div class="rail-word">control plane / tradix</div><div class="pulse" title="watchdog"></div>
  </aside>
  <main>
    <header>
      <div><div class="eyebrow">Hermes Orchestrator · informe operativo</div><h1>Mesa de <em>operaciones</em></h1></div>
      <div class="stamp"><span id="fleet-status">Cargando</span><br><span id="clock">—</span></div>
    </header>
    <section class="toolbar" aria-label="Filtros">
      <select id="task-status" aria-label="Estado de tarea"><option value="">Todos los estados</option><option>running</option><option>awaiting_approval</option><option>completed</option><option>failed</option></select>
      <select id="usage-group" aria-label="Agrupar consumo"><option value="operation">Consumo / operación</option><option value="agent">Consumo / agente</option><option value="profile">Consumo / perfil</option><option value="day">Consumo / día</option></select>
      <button id="refresh">Actualizar ahora</button><span class="meta" id="refresh-meta">Sin polling continuo · API pública</span>
    </section>
    <section class="strip" aria-label="Indicadores">
      <div class="metric"><span>Servicios listos</span><strong id="metric-fleet">—</strong></div>
      <div class="metric"><span>Tareas activas</span><strong id="metric-tasks">—</strong></div>
      <div class="metric" id="stale-box"><span>Estado stale</span><strong id="metric-stale">—</strong></div>
      <div class="metric" id="approval-box"><span>Approvals pendientes</span><strong id="metric-approvals">—</strong></div>
      <div class="metric"><span>Runs contabilizados</span><strong id="metric-usage">—</strong></div>
    </section>
    <div class="grid">
      <section class="panel">
        <div class="panel-head"><h2>Trabajo en curso</h2><span class="counter" id="task-counter">0 registros</span></div>
        <div class="table-wrap"><table><thead><tr><th>Estado</th><th>Objetivo</th><th>Asignado</th><th>Actividad</th></tr></thead><tbody id="tasks-body"></tbody></table></div>
        <div class="panel-head"><h2>Consumo agregado</h2><span class="counter">unknown permanece unknown</span></div>
        <div class="table-wrap"><table><thead><tr><th>Grupo</th><th>Runs</th><th>Input</th><th>Output</th><th>Coste</th></tr></thead><tbody id="usage-body"></tbody></table></div>
      </section>
      <aside>
        <section class="panel"><div class="panel-head"><h2>Timeline</h2><span class="counter" id="timeline-counter">reconectable</span></div><div class="timeline" id="timeline"></div></section>
        <section class="panel"><div class="panel-head"><h2>Gates y cuota</h2></div><div class="governance" id="governance"></div></section>
      </aside>
    </div>
    <div class="footer-note"><span>Vista read-only · sin transcript ni secretos</span><span id="watchdog-note">Watchdog: —</span></div>
  </main>
</div>
<script>
  const headers = {'X-Actor-Id': 'agent:operator'};
  const state = { cursor: null };
  const esc = value => String(value ?? '—').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const short = value => value ? String(value).slice(0, 8) : '—';
  const time = value => value ? new Date(value).toLocaleString('es-ES', {hour:'2-digit', minute:'2-digit', day:'2-digit', month:'short'}) : '—';
  async function get(path) { const r = await fetch(path, {headers}); if (!r.ok) throw new Error(`${r.status} ${path}`); return r.json(); }
  function renderTasks(data) {
    document.querySelector('#metric-tasks').textContent = data.active_count;
    document.querySelector('#metric-stale').textContent = data.stale_count;
    document.querySelector('#stale-box').dataset.alert = data.stale_count > 0;
    document.querySelector('#task-counter').textContent = `${data.count} registros`;
    document.querySelector('#tasks-body').innerHTML = data.items.length ? data.items.map(t => `<tr><td><span class="status ${t.stale?'stale':''}">${esc(t.status)}</span></td><td><div class="objective">${esc(t.objective)}</div><div class="mono">op ${short(t.operation_id)}</div></td><td>${esc(t.assignee_actor_id)}</td><td>${time(t.last_activity_at)}</td></tr>`).join('') : '<tr><td colspan="4" class="empty">No hay tareas para este filtro.</td></tr>';
  }
  function renderUsage(data) {
    document.querySelector('#metric-usage').textContent = data.entries;
    document.querySelector('#usage-body').innerHTML = data.groups.length ? data.groups.map(g => `<tr><td class="mono">${esc(g.key)}</td><td>${g.runs}</td><td>${esc(g.tokens.input_tokens)}</td><td>${esc(g.tokens.output_tokens)}</td><td>${esc(g.cost_status)}</td></tr>`).join('') : '<tr><td colspan="5" class="empty">Sin asientos para el filtro.</td></tr>';
  }
  function renderTimeline(data) {
    state.cursor = data.next_cursor;
    document.querySelector('#timeline-counter').textContent = `${data.count} eventos · cursor ${state.cursor ? 'activo' : 'vacío'}`;
    document.querySelector('#timeline').innerHTML = data.items.length ? [...data.items].reverse().slice(0,12).map(e => `<article class="event"><time>${time(e.created_at)}</time><div><b>${esc(e.event_type)}</b><p>${esc(e.aggregate_type)} / ${short(e.aggregate_id)}</p></div></article>`).join('') : '<div class="empty">Sin eventos nuevos.</div>';
  }
  function renderGovernance(approvals, quota) {
    document.querySelector('#metric-approvals').textContent = approvals.pending_count;
    document.querySelector('#approval-box').dataset.alert = approvals.pending_count > 0;
    const wd = quota.watchdog || {};
    document.querySelector('#watchdog-note').textContent = `Watchdog: ${wd.status || '—'} · model calls ${wd.model_calls ?? 0}`;
    const pending = approvals.items.filter(a => a.status === 'pending').slice(0,3);
    const circuitOpen = quota.circuits.filter(c => c.state === 'open').length;
    const quotaBlocked = quota.quota.length;
    document.querySelector('#governance').innerHTML = [
      `<div class="notice ${pending.length?'alert':''}"><h3>Approval queue</h3><p>${pending.length ? pending.map(a => esc(a.objective)).join(' · ') : 'Sin decisiones pendientes'}</p></div>`,
      `<div class="notice ${circuitOpen?'alert':''}"><h3>Circuitos</h3><p>${circuitOpen} abiertos / ${quota.circuits.length} registrados</p></div>`,
      `<div class="notice ${quotaBlocked?'alert':''}"><h3>Cuota</h3><p>${quotaBlocked ? quotaBlocked+' agotamientos activos' : 'Disponible según ledger'}</p></div>`,
      `<div class="notice"><h3>Resumen periódico</h3><p>${wd.summary_count ?? 0} emitidos · ${wd.idle_checks ?? 0} checks idle</p></div>`
    ].join('');
  }
  async function refresh() {
    const status = document.querySelector('#task-status').value;
    const group = document.querySelector('#usage-group').value;
    document.querySelector('#refresh-meta').textContent = 'Leyendo seis rutas…';
    try {
      const [fleet,tasks,timeline,usage,approvals,quota] = await Promise.all([
        get('/v1/operations/fleet'), get('/v1/operations/tasks?active_only=false'+(status?'&status='+encodeURIComponent(status):'')), get('/v1/operations/timeline?limit=80'), get('/v1/operations/usage?group_by='+group), get('/v1/operations/approvals'), get('/v1/operations/quota')
      ]);
      document.querySelector('#fleet-status').textContent = fleet.status;
      document.querySelector('#metric-fleet').textContent = `${fleet.service_count-fleet.unhealthy_count}/${fleet.service_count}`;
      document.querySelector('#clock').textContent = new Date().toLocaleString('es-ES');
      renderTasks(tasks); renderTimeline(timeline); renderUsage(usage); renderGovernance(approvals, quota);
      document.querySelector('#refresh-meta').textContent = `Actualizado ${new Date().toLocaleTimeString('es-ES')} · sin llamada de modelo`;
    } catch (error) {
      document.querySelector('#refresh-meta').textContent = 'Error de lectura: '+error.message;
      document.querySelector('#fleet-status').textContent = 'degraded';
    }
  }
  document.querySelector('#refresh').addEventListener('click', refresh);
  document.querySelector('#task-status').addEventListener('change', refresh);
  document.querySelector('#usage-group').addEventListener('change', refresh);
  refresh();
</script>
</body></html>"""
