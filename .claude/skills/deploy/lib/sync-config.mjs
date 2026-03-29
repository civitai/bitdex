/**
 * Reads the V2 sync config YAML and extracts dump phase COPY queries.
 * Source of truth: config/sync-civitai.yaml
 */
import { readFileSync, existsSync } from 'fs';
import { resolve } from 'path';
import { execSync } from 'child_process';

/**
 * Extract all COPY queries from the sync config, including enrichment lookups.
 * Returns an ordered array of { name, query, file } objects.
 *
 * Uses regex-based extraction: finds all copy_query blocks and associates them
 * with the nearest preceding `- name:` or `- lookup:` line.
 *
 * @param {string} [configPath] - Path to sync config YAML. Defaults to config/sync-civitai.yaml.
 * @returns {Array<{ name: string, query: string, file: string }>}
 */
export function loadDumpQueries(configPath) {
  // Find repo root — handles worktrees where cwd != repo root
  let repoRoot = process.cwd();
  try {
    repoRoot = execSync('git rev-parse --show-toplevel', { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }).trim();
  } catch {}

  // Try cwd first, then repo root, then hardcoded fallback
  const candidates = [
    configPath,
    resolve(process.cwd(), 'config', 'sync-civitai.yaml'),
    resolve(repoRoot, 'config', 'sync-civitai.yaml'),
  ].filter(Boolean);

  const yamlPath = candidates.find(p => existsSync(p));
  if (!yamlPath) {
    throw new Error(`Sync config not found. Tried:\n${candidates.join('\n')}`);
  }
  const content = readFileSync(yamlPath, 'utf8');
  const lines = content.split('\n');

  const queries = [];

  for (let i = 0; i < lines.length; i++) {
    const trimmed = lines[i].trimStart();

    // Look for copy_query: lines
    if (!trimmed.startsWith('copy_query:')) continue;

    const copyQueryIndent = lines[i].length - trimmed.length;

    // Find the name for this query by scanning backwards
    let name = null;
    for (let j = i - 1; j >= 0; j--) {
      const prev = lines[j].trimStart();
      const lookupMatch = prev.match(/^- lookup:\s*(\S+)/);
      if (lookupMatch) {
        name = lookupMatch[1].replace(/\.csv$/, '');
        break;
      }
      const nameMatch = prev.match(/^- name:\s*(\S+)/);
      if (nameMatch) {
        name = nameMatch[1];
        break;
      }
    }

    if (!name) continue;

    // Extract the query content
    const afterColon = trimmed.slice('copy_query:'.length).trim();

    let query;
    if (afterColon === '>' || afterColon === '|') {
      // Multi-line folded/literal scalar — collect indented lines
      const queryLines = [];
      for (let j = i + 1; j < lines.length; j++) {
        const line = lines[j];
        const lineIndent = line.length - line.trimStart().length;

        // Empty lines within the block are fine
        if (line.trim() === '') {
          queryLines.push('');
          continue;
        }

        // Lines must be indented more than copy_query:
        if (lineIndent <= copyQueryIndent) break;

        queryLines.push(line.trim());
      }
      query = queryLines.filter(Boolean).join(' ');
    } else if (afterColon) {
      // Inline value
      query = afterColon;
    } else {
      continue;
    }

    if (query) {
      queries.push({ name, query, file: `${name}.csv` });
    }
  }

  return queries;
}

/**
 * Known expected sizes (uncompressed) for progress estimation.
 * Updated from Aidan's 2026-03-28 dump session.
 */
export const EXPECTED_SIZES = {
  tags: 75_000_000_000,        // ~75 GB
  images: 15_000_000_000,      // ~15 GB
  resources: 820_000_000,      // ~820 MB
  model_versions: 12_000_000,  // ~12 MB
  models: 12_000_000,          // ~12 MB
  tools: 50_000_000,           // ~50 MB
  techniques: 71_000_000,      // ~71 MB
  posts: 610_000_000,          // ~610 MB
  collections: 1_300_000_000,  // ~1.3 GB
};
