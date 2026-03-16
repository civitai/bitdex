#!/usr/bin/env node
/**
 * BitDex CLI — Agent-friendly wrapper for the BitDex HTTP API.
 *
 * Usage: node ~/.claude/skills/bitdex/cli.mjs <command> [options]
 *
 * All output is JSON for easy parsing by agents.
 */

import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

// Load .env from dev-server skill dir (shared token config)
try {
  const __d = dirname(fileURLToPath(import.meta.url));
  const envPath = resolve(__d, '..', 'dev-server', '.env');
  const envFile = readFileSync(envPath, 'utf8');
  for (const line of envFile.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eq = trimmed.indexOf('=');
    if (eq > 0) {
      const key = trimmed.slice(0, eq).trim();
      const val = trimmed.slice(eq + 1).trim();
      if (!process.env[key]) process.env[key] = val;
    }
  }
} catch { /* no .env, that's fine */ }

const BASE_URL = process.env.BITDEX_URL || 'http://localhost:3001';
const DEFAULT_INDEX = process.env.BITDEX_INDEX || null;
const BITDEX_DIR = process.env.BITDEX_DIR || 'C:\\Dev\\Repos\\open-source\\bitdex-v2';
const DATA_DIR = process.env.BITDEX_DATA_DIR || './data';
const ADMIN_TOKEN = process.env.BITDEX_ADMIN_TOKEN || null;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function die(msg, code = 1) {
  console.error(JSON.stringify({ error: msg }));
  process.exit(code);
}

function json(obj) {
  console.log(JSON.stringify(obj, null, 2));
}

async function req(method, path, body = null, { admin = false } = {}) {
  const url = `${BASE_URL}${path}`;
  const headers = { 'Content-Type': 'application/json' };
  if (admin && ADMIN_TOKEN) {
    headers['Authorization'] = `Bearer ${ADMIN_TOKEN}`;
  }
  const opts = { method, headers };
  if (body !== null) opts.body = JSON.stringify(body);

  let res;
  try {
    res = await fetch(url, opts);
  } catch (e) {
    if (e.cause?.code === 'ECONNREFUSED') {
      die(`BitDex server not reachable at ${BASE_URL}. Start it with: node ~/.claude/skills/bitdex/cli.mjs start`);
    }
    die(`Network error: ${e.message}`);
  }

  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = text;
  }

  if (!res.ok) {
    console.error(JSON.stringify({ error: `HTTP ${res.status}`, detail: data }, null, 2));
    process.exit(1);
  }
  return data;
}

/** Resolve index name: explicit arg > env > auto-detect (first index) */
async function resolveIndex(explicit) {
  if (explicit) return explicit;
  if (DEFAULT_INDEX) return DEFAULT_INDEX;
  // Auto-detect: list indexes, use the first one
  const data = await req('GET', '/api/indexes');
  if (!data.indexes?.length) die('No indexes found. Create one first.');
  return data.indexes[0].name;
}

function parseArgs(argv) {
  const flags = {};
  const positional = [];
  let i = 0;
  while (i < argv.length) {
    const arg = argv[i];
    if (arg.startsWith('--')) {
      const key = arg.slice(2);
      // Boolean flags (no value after them)
      const boolFlags = ['rebuild', 'include-docs', 'save-snapshot', 'no-save', 'json', 'follow'];
      if (boolFlags.includes(key)) {
        flags[key] = true;
      } else if (i + 1 < argv.length) {
        flags[key] = argv[i + 1];
        i++;
      } else {
        flags[key] = true;
      }
    } else {
      positional.push(arg);
    }
    i++;
  }
  return { flags, positional };
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

const commands = {};

// --- Server lifecycle ---

commands.start = {
  help: 'Start the BitDex server (cargo run)',
  usage: 'start [--port 3001] [--data-dir ./data] [--rebuild]',
  async run(flags) {
    const { execSync } = await import('child_process');

    // Check if already running
    try {
      const res = await fetch(`${BASE_URL}/api/health`);
      if (res.ok) {
        json({ status: 'already_running', url: BASE_URL });
        return;
      }
    } catch { /* not running, proceed */ }

    const port = flags.port || '3001';
    const dataDir = flags['data-dir'] || DATA_DIR;
    const rebuild = flags.rebuild ? ' --rebuild' : '';

    const cmd = `cd "${BITDEX_DIR}" && cargo run --release --features server --bin bitdex-server -- --port ${port} --data-dir "${dataDir}"${rebuild}`;
    json({
      status: 'starting',
      command: cmd,
      note: 'Run this in a separate terminal — the server runs in the foreground.',
    });
  },
};

commands.health = {
  help: 'Check if the BitDex server is running',
  usage: 'health',
  async run() {
    try {
      const res = await fetch(`${BASE_URL}/api/health`);
      const text = await res.text();
      json({ status: 'ok', url: BASE_URL, response: text });
    } catch (e) {
      json({ status: 'unreachable', url: BASE_URL, error: e.message });
    }
  },
};

commands.status = {
  help: 'Full server status: health + index info + active tasks',
  usage: 'status [--index NAME]',
  async run(flags) {
    // Health
    let healthy = false;
    try {
      const res = await fetch(`${BASE_URL}/api/health`);
      healthy = res.ok;
    } catch { /* unreachable */ }

    if (!healthy) {
      json({ status: 'unreachable', url: BASE_URL });
      return;
    }

    // Indexes
    const indexes = await req('GET', '/api/indexes');

    // Per-index details
    const details = [];
    for (const idx of indexes.indexes || []) {
      const stats = await req('GET', `/api/indexes/${idx.name}/stats`);
      const tasks = await req('GET', `/api/indexes/${idx.name}/tasks`);
      details.push({
        name: idx.name,
        alive_count: idx.alive_count,
        stats,
        active_task: tasks.active || null,
        recent_tasks: (tasks.history || []).slice(0, 3),
      });
    }

    json({ status: 'ok', url: BASE_URL, indexes: details });
  },
};

// --- Index management ---

commands['index-create'] = {
  help: 'Create a new index',
  usage: 'index-create --name NAME --config-file PATH',
  async run(flags) {
    if (!flags.name) die('--name required');
    if (!flags['config-file']) die('--config-file required (JSON file with config + data_schema)');

    const { readFileSync } = await import('fs');
    const body = JSON.parse(readFileSync(flags['config-file'], 'utf8'));
    body.name = flags.name;
    const data = await req('POST', '/api/indexes', body);
    json(data);
  },
};

commands['index-list'] = {
  help: 'List all indexes',
  usage: 'index-list',
  async run() {
    json(await req('GET', '/api/indexes'));
  },
};

commands['index-get'] = {
  help: 'Get index details (config, schema, stats)',
  usage: 'index-get [--index NAME]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    json(await req('GET', `/api/indexes/${idx}`));
  },
};

commands['index-delete'] = {
  help: 'Delete an index',
  usage: 'index-delete --index NAME',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    json(await req('DELETE', `/api/indexes/${idx}`, null, { admin: true }));
  },
};

// --- Data loading ---

commands.load = {
  help: 'Load NDJSON data into an index (async task)',
  usage: 'load --path /path/to/file.ndjson [--index NAME] [--limit N] [--threads N] [--chunk-size N]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    if (!flags.path) die('--path required');

    const body = { path: flags.path };
    if (flags.limit) body.limit = parseInt(flags.limit);
    if (flags.threads) body.threads = parseInt(flags.threads);
    if (flags['chunk-size']) body.chunk_size = parseInt(flags['chunk-size']);
    if (flags['docstore-batch-size']) body.docstore_batch_size = parseInt(flags['docstore-batch-size']);

    const data = await req('POST', `/api/indexes/${idx}/load`, body);

    if (flags.follow) {
      json({ ...data, note: 'Polling task progress...' });
      await pollTask(data.task_id, idx);
    } else {
      json({ ...data, poll: `node ~/.claude/skills/bitdex/cli.mjs task ${data.task_id}` });
    }
  },
};

// --- Querying ---

commands.query = {
  help: 'Execute a filter+sort query',
  usage: 'query [--index NAME] [--filter JSON] [--sort FIELD] [--dir Asc|Desc] [--limit N] [--offset N] [--cursor JSON] [--include-docs] [--fields f1,f2]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);

    const body = {
      filters: flags.filter ? JSON.parse(flags.filter) : [],
      limit: parseInt(flags.limit || '20'),
    };

    if (flags.sort) {
      body.sort = {
        field: flags.sort,
        direction: flags.dir || 'Desc',
      };
    }
    if (flags.cursor) body.cursor = JSON.parse(flags.cursor);
    if (flags.offset) body.offset = parseInt(flags.offset);
    if (flags['include-docs'] || flags.fields) {
      body.include_docs = flags.fields ? flags.fields.split(',') : true;
    }

    json(await req('POST', `/api/indexes/${idx}/query`, body));
  },
};

commands['query-raw'] = {
  help: 'Execute a query from a raw JSON body (file or inline)',
  usage: 'query-raw [--index NAME] --body JSON_OR_FILE',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    if (!flags.body) die('--body required (inline JSON or path to .json file)');

    let body;
    try {
      // Try parsing as inline JSON first
      body = JSON.parse(flags.body);
    } catch {
      // Try as file path
      const { readFileSync } = await import('fs');
      body = JSON.parse(readFileSync(flags.body, 'utf8'));
    }

    json(await req('POST', `/api/indexes/${idx}/query`, body));
  },
};

// --- Documents ---

commands['doc-get'] = {
  help: 'Get a single document by slot ID',
  usage: 'doc-get --id SLOT_ID [--index NAME] [--fields f1,f2]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    if (!flags.id) die('--id required');

    const body = { slot_id: parseInt(flags.id) };
    if (flags.fields) body.fields = flags.fields.split(',');
    else body.fields = true;

    json(await req('POST', `/api/indexes/${idx}/document`, body));
  },
};

commands['doc-batch'] = {
  help: 'Get multiple documents by slot IDs',
  usage: 'doc-batch --ids 1,2,3 [--index NAME] [--fields f1,f2]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    if (!flags.ids) die('--ids required (comma-separated)');

    const body = { slot_ids: flags.ids.split(',').map(Number) };
    if (flags.fields) body.fields = flags.fields.split(',');
    else body.fields = true;

    json(await req('POST', `/api/indexes/${idx}/documents`, body));
  },
};

commands.upsert = {
  help: 'Upsert documents (insert or update)',
  usage: 'upsert --docs JSON_ARRAY_OR_FILE [--index NAME]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    if (!flags.docs) die('--docs required (inline JSON array or path to .json file)');

    let documents;
    try {
      documents = JSON.parse(flags.docs);
    } catch {
      const { readFileSync } = await import('fs');
      documents = JSON.parse(readFileSync(flags.docs, 'utf8'));
    }

    if (!Array.isArray(documents)) documents = [documents];

    json(await req('POST', `/api/indexes/${idx}/documents/upsert`, { documents }, { admin: true }));
  },
};

commands.delete = {
  help: 'Delete documents by ID',
  usage: 'delete --ids 1,2,3 [--index NAME]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    if (!flags.ids) die('--ids required (comma-separated)');

    const ids = flags.ids.split(',').map(Number);
    json(await req('DELETE', `/api/indexes/${idx}/documents`, { ids }, { admin: true }));
  },
};

// --- Operations ---

commands.stats = {
  help: 'Get index stats (counts, cache info, memory)',
  usage: 'stats [--index NAME]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    json(await req('GET', `/api/indexes/${idx}/stats`));
  },
};

commands.traces = {
  help: 'Get recent query traces (requires --enable-traces on server)',
  usage: 'traces [--last N] [--index NAME]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    const last = flags.last || 10;
    json(await req('GET', `/api/indexes/${idx}/traces?last=${last}`));
  },
};

commands['cache-clear'] = {
  help: 'Clear the unified cache',
  usage: 'cache-clear [--index NAME]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    json(await req('DELETE', `/api/indexes/${idx}/cache`, null, { admin: true }));
  },
};

commands.warm = {
  help: 'Pre-populate the unified cache with specified queries',
  usage: 'warm --body JSON_OR_FILE [--index NAME]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    if (!flags.body) die('--body required (inline JSON or path to .json file with { "queries": [...] })');

    let body;
    try {
      body = JSON.parse(flags.body);
    } catch {
      const { readFileSync } = await import('fs');
      body = JSON.parse(readFileSync(flags.body, 'utf8'));
    }

    json(await req('POST', `/api/indexes/${idx}/warm`, body, { admin: true }));
  },
};

commands.snapshot = {
  help: 'Save bitmap snapshot to disk (synchronous, blocks until done)',
  usage: 'snapshot [--index NAME]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    json(await req('POST', `/api/indexes/${idx}/snapshot`, null, { admin: true }));
  },
};

commands.rebuild = {
  help: 'Rebuild bitmaps from docstore (async task)',
  usage: 'rebuild [--index NAME] [--sort-fields f1,f2] [--filter-fields f1,f2] [--no-save] [--follow]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    const body = {};
    if (flags['sort-fields']) body.sort_fields = flags['sort-fields'].split(',');
    if (flags['filter-fields']) body.filter_fields = flags['filter-fields'].split(',');
    body.save_snapshot = !flags['no-save'];

    const data = await req('POST', `/api/indexes/${idx}/rebuild`, body, { admin: true });

    if (flags.follow) {
      json({ ...data, note: 'Polling task progress...' });
      await pollTask(data.task_id, idx);
    } else {
      json({ ...data, poll: `node ~/.claude/skills/bitdex/cli.mjs task ${data.task_id}` });
    }
  },
};

commands['fields-add'] = {
  help: 'Hot-add new fields to a running index',
  usage: 'fields-add --body JSON [--index NAME] [--follow]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    if (!flags.body) die('--body required (JSON with filter_fields/sort_fields arrays)');

    const body = JSON.parse(flags.body);
    if (flags['no-save']) body.save_snapshot = false;

    const data = await req('POST', `/api/indexes/${idx}/fields`, body, { admin: true });

    if (flags.follow) {
      json({ ...data, note: 'Polling task progress...' });
      await pollTask(data.task_id, idx);
    } else {
      json({ ...data, poll: `node ~/.claude/skills/bitdex/cli.mjs task ${data.task_id}` });
    }
  },
};

commands['fields-remove'] = {
  help: 'Remove fields from a running index',
  usage: 'fields-remove --body JSON [--index NAME]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    if (!flags.body) die('--body required (JSON with filter_fields/sort_fields string arrays)');

    const body = JSON.parse(flags.body);
    if (flags['no-save']) body.save_snapshot = false;

    json(await req('DELETE', `/api/indexes/${idx}/fields`, body, { admin: true }));
  },
};

// --- Tasks ---

commands.task = {
  help: 'Get task status by ID',
  usage: 'task TASK_ID [--follow]',
  async run(flags, positional) {
    const taskId = positional[0] || flags.id;
    if (!taskId) die('Task ID required');

    if (flags.follow) {
      await pollTask(parseInt(taskId));
    } else {
      json(await req('GET', `/api/tasks/${taskId}`));
    }
  },
};

commands.tasks = {
  help: 'List tasks for an index (active + history)',
  usage: 'tasks [--index NAME]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    json(await req('GET', `/api/indexes/${idx}/tasks`));
  },
};

async function pollTask(taskId, indexName) {
  const interval = 3000;
  while (true) {
    const data = await req('GET', `/api/tasks/${taskId}`);
    const status = data.status;
    const progress = data.progress?.records_processed || 0;
    const elapsed = data.elapsed_secs?.toFixed(1) || '?';

    console.error(`[${elapsed}s] ${status} — ${progress.toLocaleString()} records`);

    if (status === 'complete' || status === 'error') {
      json(data);
      return;
    }

    await new Promise(r => setTimeout(r, interval));
  }
}

// --- Cursors ---

commands['cursor-list'] = {
  help: 'List named cursors for an index',
  usage: 'cursor-list [--index NAME]',
  async run(flags) {
    const idx = await resolveIndex(flags.index);
    json(await req('GET', `/api/indexes/${idx}/cursors`));
  },
};

commands['cursor-get'] = {
  help: 'Get a named cursor value',
  usage: 'cursor-get CURSOR_NAME [--index NAME]',
  async run(flags, positional) {
    const idx = await resolveIndex(flags.index);
    const name = positional[0] || flags.name;
    if (!name) die('Cursor name required');
    json(await req('GET', `/api/indexes/${idx}/cursors/${name}`));
  },
};

// --- Metrics ---

commands.metrics = {
  help: 'Get Prometheus metrics (raw text)',
  usage: 'metrics',
  async run() {
    const res = await fetch(`${BASE_URL}/metrics`);
    const text = await res.text();
    console.log(text);
  },
};

// --- Help ---

commands.help = {
  help: 'Show this help',
  usage: 'help [command]',
  async run(flags, positional) {
    const cmd = positional[0];
    if (cmd && commands[cmd]) {
      json({ command: cmd, help: commands[cmd].help, usage: `bitdex ${commands[cmd].usage}` });
      return;
    }

    const table = {};
    for (const [name, c] of Object.entries(commands)) {
      table[name] = c.help;
    }
    json({
      usage: 'node ~/.claude/skills/bitdex/cli.mjs <command> [options]',
      commands: table,
      env: {
        BITDEX_URL: `Server URL (default: ${BASE_URL})`,
        BITDEX_INDEX: 'Default index name (auto-detected if unset)',
        BITDEX_DIR: 'Path to bitdex-v2 repo (for start command)',
        BITDEX_DATA_DIR: 'Data directory (for start command)',
      },
    });
  },
};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
const { flags, positional } = parseArgs(args);
const command = positional.shift();

if (!command || !commands[command]) {
  if (!command) {
    commands.help.run({}, []);
  } else {
    die(`Unknown command: ${command}. Run 'help' for available commands.`);
  }
} else {
  commands[command].run(flags, positional).catch(e => die(e.message));
}
