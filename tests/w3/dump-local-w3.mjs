#!/usr/bin/env node
// W3 dump driver — matches deploy/configs/sync-config-civitai.yaml (#326):
//   images phase: +sortAt column (idx10) + sortAt->sortAt field; posts enrichment +model3dId.
// Feeds the *.w3.csv prepped files. Serial PUT /dumps per phase, polls to completion.
//   node dump-local-w3.mjs <full|small>   BITDEX_URL=http://localhost:3007
import path from 'node:path';
import fs from 'node:fs';

const SERVER_URL = process.env.BITDEX_URL || 'http://localhost:3007';
const INDEX = 'civitai';
const STAGE = process.env.BITDEX_STAGE_DIR || 'C:/Dev/Repos/open-source/bitdex-v2/data/load_stage';
const MODE = process.argv[2] || 'small';
const SMALL = MODE === 'small';
const only = (process.argv.find(a=>a.startsWith('--phases=')) || '').slice('--phases='.length);
const PHASES_FILTER = only ? only.split(',') : null;

const isuf = SMALL ? '-small' : '';
const csv = (n, w3) => path.join(STAGE, `${n}${w3?'.w3':''}.csv`);
const tsv = (n) => path.join(STAGE, `${n}${SMALL?'-small':''}.tsv`);

const PHASES = [
  { name: 'images', body: {
      name: `images-${Date.now()}`, csv_path: path.join(STAGE, `images${isuf}.w3d.csv`), format: 'csv',
      slot_field: 'id', sets_alive: true,
      columns: ['id','url','nsfwLevel','hash','flags','type','userId','blockedFor','scannedAtSecs','createdAtSecs','sortAt','postId','width','height'],
      fields: ['nsfwLevel', {column:'type',target:'type'}, 'userId', 'postId', 'blockedFor',
               {column:'url',target:'url'}, {column:'hash',target:'hash'}, 'width', 'height',
               {column:'sortAt',target:'sortAt'}],
      computed_fields: [
        {target:'hasMeta', expression:'(flags >> 13) & 1 == 1 && (flags >> 2) & 1 == 0'},
        {target:'onSite',  expression:'(flags >> 14) & 1 == 1'},
        {target:'minor',   expression:'(flags >> 3) & 1 == 1'},
        {target:'poi',     expression:'(flags >> 4) & 1 == 1'},
        {target:'existedAt', expression:'max(scannedAtSecs, createdAtSecs)'},
        {target:'id',        expression:'id'},
      ],
      enrichment: [ {
        csv_path: path.join(STAGE, 'posts.w3.csv'), key: 'id', join_on: 'postId',
        columns: ['id','publishedAtSecs','availability','modelVersionId','model3dId'],
        fields: [ {column:'publishedAtSecs',target:'publishedAt'}, {column:'availability',target:'availability'},
                  {column:'modelVersionId',target:'postedToId'}, {column:'model3dId',target:'model3dId'} ],
        computed_fields: [ {target:'isPublished', expression:'publishedAtSecs != null'} ],
      } ],
  } },
  { name: 'tags', body: { name:`tags-${Date.now()}`, csv_path: csv('tags'), format:'csv', slot_field:'imageId',
      sets_alive:false, columns:['tagId','imageId','attributes'], fields:[{column:'tagId',target:'tagIds'}],
      filter:'(attributes >> 10) & 1 = 0', streaming_merge: !SMALL } },
  { name: 'resources', body: { name:`resources-${Date.now()}`, csv_path: csv('resources'), format:'csv', slot_field:'imageId',
      sets_alive:false, columns:['imageId','modelVersionId','detected'], fields:[{column:'modelVersionId',target:'modelVersionIds'}],
      computed_fields:[{target:'modelVersionIdsManual',expression:'detected == false',value:'modelVersionId'}],
      enrichment:[{ csv_path: path.join(STAGE,'model_versions.csv'), key:'id', join_on:'modelVersionId',
        columns:['id','baseModel','modelId'], fields:[{column:'baseModel',target:'baseModel'}],
        enrichment:[{ csv_path: path.join(STAGE,'models.csv'), key:'id', join_on:'modelId',
          columns:['id','poi','type'], fields:[{column:'poi',target:'poi'}], filter:"type = 'Checkpoint'" }] }] } },
  { name: 'tools', body: { name:`tools-${Date.now()}`, csv_path: csv('tools'), format:'csv', slot_field:'imageId',
      sets_alive:false, columns:['toolId','imageId'], fields:[{column:'toolId',target:'toolIds'}] } },
  { name: 'techniques', body: { name:`techniques-${Date.now()}`, csv_path: csv('techniques'), format:'csv', slot_field:'imageId',
      sets_alive:false, columns:['techniqueId','imageId'], fields:[{column:'techniqueId',target:'techniqueIds'}] } },
  { name: 'metrics', body: { name:`metrics-${Date.now()}`, csv_path: tsv('metrics'), format:'tsv', slot_field:'imageId',
      sets_alive:false, columns:['imageId','reactionCount','commentCount','collectedCount'],
      fields:['reactionCount','commentCount','collectedCount'] } },
];

const fmt = (ms)=>{const s=Math.round(ms/1000);return s<60?`${s}s`:`${Math.floor(s/60)}m${s%60}s`;};
async function health(dl=30000){const t=Date.now();while(Date.now()-t<dl){try{const r=await fetch(`${SERVER_URL}/api/health`);if(r.ok)return;}catch{}await new Promise(r=>setTimeout(r,500));}throw new Error('unhealthy');}
async function put(b){const r=await fetch(`${SERVER_URL}/api/indexes/${INDEX}/dumps`,{method:'PUT',headers:{'content-type':'application/json'},body:JSON.stringify(b)});const t=await r.text();if(!r.ok)throw new Error(`PUT ${r.status}: ${t}`);return JSON.parse(t);}
async function list(){const r=await fetch(`${SERVER_URL}/api/indexes/${INDEX}/dumps`);if(!r.ok)throw new Error(`GET ${r.status}`);return r.json();}
async function poll(name){let last=0,lt=Date.now();while(true){await new Promise(r=>setTimeout(r,3000));const d=await list();const e=d.dumps?.[name];if(!e)continue;const st=e.status;const rows=e.records_processed??e.ops_processed??0;const el=e.elapsed_secs??0;if(rows!==last||Date.now()-lt>15000){console.log(`  [${name}] rows=${rows.toLocaleString()} el=${el}s rate=${el>0?Math.round(rows/el).toLocaleString():0}/s status=${typeof st==='string'?st:JSON.stringify(st)}`);last=rows;lt=Date.now();}if(st==='Complete'||(typeof st==='object'&&st&&'Failed'in st))return e;}}

async function main(){
  console.log(`Target ${SERVER_URL} index ${INDEX} mode ${MODE}`);
  await health(); console.log('healthy\n');
  const sel = PHASES.filter(p=>!PHASES_FILTER||PHASES_FILTER.includes(p.name));
  const sum=[]; const t0=Date.now();
  for(const ph of sel){
    if(!fs.existsSync(ph.body.csv_path)){console.error(`  MISSING ${ph.body.csv_path} — skip ${ph.name}`);continue;}
    const gb=(fs.statSync(ph.body.csv_path).size/1e9).toFixed(2);
    console.log(`\n=== ${ph.name} (${gb} GB) ===`); const s0=Date.now();
    const resp=await put(ph.body); console.log(`  dump=${resp.name}`);
    const res=await poll(resp.name); const el=Date.now()-s0;
    if(typeof res.status==='object'&&'Failed'in res.status){console.error(`  FAILED: ${res.status.Failed}`);sum.push({p:ph.name,ok:false,el,e:res.status.Failed});break;}
    console.log(`  done ${fmt(el)} ops=${(res.ops_processed??res.records_processed??0).toLocaleString()}`);
    sum.push({p:ph.name,ok:true,el,rows:res.ops_processed??res.records_processed??0});
  }
  console.log(`\n=== Summary (${fmt(Date.now()-t0)}) ===`);
  for(const s of sum) console.log(`  ${s.ok?'OK':'XX'} ${s.p.padEnd(11)} ${fmt(s.el).padStart(8)} ${s.ok?s.rows.toLocaleString()+' rows':s.e}`);
  process.exit(sum.length===sel.length&&sum.every(s=>s.ok)?0:1);
}
main().catch(e=>{console.error('FATAL',e);process.exit(2);});
