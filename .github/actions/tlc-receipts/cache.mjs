// Restores and saves TLC verdict receipts (contract C5 of #5148, issue #5164)
// as GitHub Actions cache entries, one entry per key. Called by action.yml
// beside this file.
//
// WHY JAVASCRIPT. actions/cache restores or saves ONE key per step, and a run
// needs one entry per specification module (plus per-item partial receipts),
// so a fixed list of steps cannot express it. The cache runtime token exists
// only inside an action step, never in a `run:` step, so action.yml calls these
// two functions from actions/github-script with the npm @actions/cache client.
// Every decision about WHICH keys to read or write is made by
// .github/workflows/tlc-receipts.py; this file only moves the bytes named in
// its manifest.
//
// The manifest is a JSON array of { key, path }, where path is the
// workspace-relative `.tlc-receipts/<key>.json`. Restore and save must use the
// same path string, because the cache version hashes it.
//
// Failure is never fatal here, and never optimistic. A failed restore is a
// miss (the module runs); a failed or refused save loses only a future saving.

import { existsSync, readFileSync } from 'node:fs';

const CONCURRENCY = 6;

function readManifest(manifest) {
  if (!existsSync(manifest)) {
    return [];
  }
  const entries = JSON.parse(readFileSync(manifest, 'utf8'));
  if (!Array.isArray(entries)) {
    throw new Error(`${manifest} is not a JSON array of { key, path }`);
  }
  return entries;
}

async function pool(entries, work) {
  let next = 0;
  const workers = Array.from({ length: Math.min(CONCURRENCY, entries.length) }, async () => {
    while (next < entries.length) {
      const entry = entries[next++];
      await work(entry);
    }
  });
  await Promise.all(workers);
}

export async function restore({ cache, core, manifest }) {
  const entries = readManifest(manifest);
  let hits = 0;
  await pool(entries, async ({ key, path }) => {
    try {
      const matched = await cache.restoreCache([path], key, []);
      if (matched === key) {
        hits++;
      } else {
        core.info(`No cache entry restored for ${key}; treating it as absent.`);
      }
    } catch (error) {
      core.warning(`Restoring ${key} failed; treating it as absent: ${error.message}`);
    }
  });
  core.info(`Restored ${hits} of ${entries.length} TLC receipt cache entr(ies).`);
}

export async function save({ cache, core, manifest }) {
  const entries = readManifest(manifest);
  let saved = 0;
  await pool(entries, async ({ key, path }) => {
    if (!existsSync(path)) {
      core.warning(`Receipt file ${path} for ${key} is missing; not saved.`);
      return;
    }
    try {
      const id = await cache.saveCache([path], key);
      if (typeof id === 'number' && id > 0) {
        saved++;
      } else {
        core.info(`${key} was not saved (it most likely exists already, written by an earlier run).`);
      }
    } catch (error) {
      if (error && error.name === 'ReserveCacheError') {
        core.info(`${key} exists already; cache entries are immutable, so the earlier receipt stands.`);
      } else {
        core.warning(`Saving ${key} failed: ${error.message}`);
      }
    }
  });
  core.info(`Saved ${saved} of ${entries.length} TLC receipt cache entr(ies).`);
}
