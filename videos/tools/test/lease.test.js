import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { after, test } from "node:test";
import { leaseStatus, releaseLease, renewLease, takeLease } from "../lib/lease.js";

const scratch = mkdtempSync(path.join(tmpdir(), "videos-lease-"));
after(() => rmSync(scratch, { recursive: true, force: true }));
let n = 0;
const fresh = () => path.join(scratch, `lease-${++n}.json`);
const T0 = Date.parse("2026-09-28T03:00:00Z");
const minute = 60_000;

test("one run holds the lease at a time, and each grant has a higher token", () => {
  const file = fresh();
  const first = takeLease({ owner: "run-a", minutes: 60, now: T0, file });
  assert.equal(first.granted, true);
  assert.equal(first.lease.token, 1);
  const second = takeLease({ owner: "run-b", minutes: 60, now: T0 + minute, file });
  assert.equal(second.granted, false);
  assert.equal(second.lease.owner, "run-a", "a refusal names the holder");
  assert.equal(releaseLease({ token: 1, now: T0 + 2 * minute, file }).released, true);
  const third = takeLease({ owner: "run-b", minutes: 60, now: T0 + 3 * minute, file });
  assert.equal(third.granted, true);
  assert.equal(third.lease.token, 2);
});

test("renewing extends the lease; a lease that is not renewed lapses and can be taken", () => {
  const file = fresh();
  takeLease({ owner: "run-a", minutes: 30, now: T0, file });
  const renewed = renewLease({ token: 1, minutes: 30, now: T0 + 20 * minute, file });
  assert.equal(renewed.granted, true);
  assert.equal(renewed.lease.expiresAt, new Date(T0 + 50 * minute).toISOString());
  assert.equal(leaseStatus({ now: T0 + 49 * minute, file }).held, true);
  assert.equal(leaseStatus({ now: T0 + 51 * minute, file }).held, false);
  const taken = takeLease({ owner: "run-b", minutes: 30, now: T0 + 51 * minute, file });
  assert.equal(taken.granted, true);
  assert.equal(renewLease({ token: 1, minutes: 30, now: T0 + 52 * minute, file }).granted, false, "the lapsed holder is fenced out");
  assert.equal(releaseLease({ token: 1, file }).released, false, "and cannot release the new holder's lease");
});

test("a lease names its owner and a sensible length", () => {
  const file = fresh();
  assert.throws(() => takeLease({ owner: "", file }), /name an owner/);
  assert.throws(() => takeLease({ owner: "run-a", minutes: 0, file }), /minutes must be/);
  assert.equal(leaseStatus({ file }).held, false);
  assert.equal(leaseStatus({ file }).lease, null);
});
