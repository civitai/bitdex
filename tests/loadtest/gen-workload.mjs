#!/usr/bin/env node
/**
 * Generate a workload.json file for the Rust loadtest binary
 * from real Civitai traffic CSV data.
 */
import { readFileSync, writeFileSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

function loadCsv(filename, idCol = 0, viewCol = 1) {
  const lines = readFileSync(join(__dirname, filename), 'utf8').trim().split('\n').slice(1);
  return lines.map(l => {
    const cols = l.split(',');
    return { id: Number(cols[idCol]), views: Number(cols[viewCol]) };
  }).filter(d => !isNaN(d.id) && !isNaN(d.views));
}

const users = loadCsv('user_profile_views_24h.csv').slice(0, 500);
const mvs = loadCsv('model_version_views_24h.csv').slice(0, 500);
const tags = loadCsv('tag_views_24h.csv', 0, 2).slice(0, 500);

const queries = [];

// Feed queries (20 variants)
const sorts = ['sortAt', 'reactionCount', 'commentCount', 'collectedCount'];
const nsfwLevels = [1, 2, 4];
for (const sort of sorts) {
  queries.push({
    label: `feed_${sort}`,
    filters: [],
    sort: { field: sort, direction: 'Desc' },
  });
  for (const nsfw of nsfwLevels) {
    queries.push({
      label: `feed_nsfw${nsfw}_${sort}`,
      filters: [{ Eq: ['nsfwLevel', { Integer: nsfw }] }],
      sort: { field: sort, direction: 'Desc' },
    });
  }
}

// User queries — top 500 users × 2 sorts
for (const u of users) {
  queries.push({
    label: `user_${u.id}_reactions`,
    filters: [{ Eq: ['userId', { Integer: u.id }] }],
    sort: { field: 'reactionCount', direction: 'Desc' },
  });
  queries.push({
    label: `user_${u.id}_sortAt`,
    filters: [{ Eq: ['userId', { Integer: u.id }] }],
    sort: { field: 'sortAt', direction: 'Desc' },
  });
}

// Model version queries — top 500 mvs
for (const mv of mvs) {
  queries.push({
    label: `mv_${mv.id}_reactions`,
    filters: [{ In: ['modelVersionIdsManual', [{ Integer: mv.id }]] }],
    sort: { field: 'reactionCount', direction: 'Desc' },
  });
}

// Tag queries — top 500 tags × with/without nsfw filter
for (const t of tags) {
  queries.push({
    label: `tag_${t.id}_sortAt`,
    filters: [{ In: ['tagIds', [{ Integer: t.id }]] }],
    sort: { field: 'sortAt', direction: 'Desc' },
  });
  queries.push({
    label: `tag_${t.id}_sfw_reactions`,
    filters: [{ In: ['tagIds', [{ Integer: t.id }]] }, { Eq: ['nsfwLevel', { Integer: 1 }] }],
    sort: { field: 'reactionCount', direction: 'Desc' },
  });
}

const output = { queries };
writeFileSync(join(__dirname, 'workload.json'), JSON.stringify(output, null, 2));
console.log(`Generated ${queries.length} queries → tests/loadtest/workload.json`);
console.log(`  Feed variants: 16`);
console.log(`  User queries: ${users.length * 2}`);
console.log(`  MV queries: ${mvs.length}`);
console.log(`  Tag queries: ${tags.length * 2}`);
