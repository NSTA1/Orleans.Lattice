// The production lease: one run at a time makes the series. A scheduled run
// and a hand-started one could otherwise both pick up the next item, speak it
// and open two pull requests for it. Only this machine has the series voice,
// so the lease is a file in the per-user state directory (layout.js,
// stateDir) rather than anything shared: a run takes it before it touches the
// queue, renews it while it works, and releases it when it stops. A lease that
// is not renewed lapses on its own, so a run that crashed does not block the
// next one for longer than its lease.
//
// Each grant carries a token one higher than the last, so a run that lost its
// lease (it lapsed, and another run took it) can tell: its renewals fail.
import { existsSync, mkdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import path from "node:path";
import { stateDir } from "./layout.js";

/** Where the lease is kept. */
export const leaseFile = (state = stateDir) => path.join(state, "production-lease.json");

const sleep = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);

// A read-modify-write of the lease happens under a lock directory, which only
// one process can create; one left behind by a crash is cleared after 30 s.
function locked(file, update) {
  mkdirSync(path.dirname(file), { recursive: true });
  const lock = `${file}.lock`;
  for (let attempt = 0; ; attempt++) {
    try {
      mkdirSync(lock);
      break;
    } catch (error) {
      if (error.code !== "EEXIST") throw error;
      try {
        if (Date.now() - statSync(lock).mtimeMs > 30_000) rmSync(lock, { recursive: true, force: true });
      } catch {
        // Removed by the process that held it; try again.
      }
      if (attempt >= 100) throw new Error(`lease: could not lock ${file}`);
      sleep(50);
    }
  }
  try {
    const current = existsSync(file) ? JSON.parse(readFileSync(file, "utf8")) : null;
    const { next, result } = update(current);
    if (next) {
      const partial = `${file}.${process.pid}.partial`;
      writeFileSync(partial, `${JSON.stringify(next, null, 2)}\n`);
      renameSync(partial, file);
    }
    return result;
  } finally {
    rmSync(lock, { recursive: true, force: true });
  }
}

const live = (lease, now) => Boolean(lease && !lease.released && Date.parse(lease.expiresAt) > now);
const minutesFrom = (now, minutes) => new Date(now + minutes * 60_000).toISOString();

function checkMinutes(minutes) {
  if (!Number.isFinite(minutes) || minutes <= 0 || minutes > 24 * 60) throw new Error("lease: minutes must be a number from 1 to 1440");
}

/**
 * Takes the lease for `owner` for `minutes`. Resolves with { granted: true,
 * lease } or, when another run holds it, { granted: false, lease } naming the
 * holder and when its lease lapses.
 */
export function takeLease({ owner, minutes = 90, now = Date.now(), file = leaseFile() }) {
  if (typeof owner !== "string" || !owner) throw new Error("lease: name an owner, such as the session that takes it");
  checkMinutes(minutes);
  return locked(file, (current) => {
    if (live(current, now)) return { result: { granted: false, lease: current } };
    const lease = {
      owner,
      token: (current?.token ?? 0) + 1,
      takenAt: new Date(now).toISOString(),
      expiresAt: minutesFrom(now, minutes),
      released: false,
    };
    return { next: lease, result: { granted: true, lease } };
  });
}

/**
 * Extends a lease the caller holds, by its token. { granted: false } means the
 * token no longer holds it - it lapsed and was taken, or was released - and
 * the caller must stop and leave the work to the run that holds it.
 */
export function renewLease({ token, minutes = 90, now = Date.now(), file = leaseFile() }) {
  checkMinutes(minutes);
  return locked(file, (current) => {
    if (!live(current, now) || current.token !== token) return { result: { granted: false, lease: current } };
    const lease = { ...current, expiresAt: minutesFrom(now, minutes) };
    return { next: lease, result: { granted: true, lease } };
  });
}

/** Releases a lease by its token; releasing one that is no longer held is a no-op. */
export function releaseLease({ token, now = Date.now(), file = leaseFile() }) {
  return locked(file, (current) => {
    if (!current || current.token !== token || current.released) return { result: { released: false, lease: current } };
    const lease = { ...current, released: true, releasedAt: new Date(now).toISOString() };
    return { next: lease, result: { released: true, lease } };
  });
}

/** The lease as it stands: who holds it and until when, or that nobody does. */
export function leaseStatus({ now = Date.now(), file = leaseFile() } = {}) {
  const lease = existsSync(file) ? JSON.parse(readFileSync(file, "utf8")) : null;
  return { held: live(lease, now), lease };
}
