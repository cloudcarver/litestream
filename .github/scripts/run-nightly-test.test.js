'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');
const { runNightlyTest } = require('./run-nightly-test');

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'nightly-resources-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  return { root, options: { artifacts: root, filesystem: root, minimumFreeBytes: 100, reserveBytes: 20,
    intervalMs: 10, graceMs: 100, measure: () => ({ freeBytes: 200 }) } };
}

test('disk preflight preserves evidence and never starts an underprovisioned test', async t => {
  const { root, options } = fixture(t);
  const marker = path.join(root, 'started');
  const code = await runNightlyTest([process.execPath, '-e', 'require("node:fs").writeFileSync(process.argv[1], "started")', marker],
    { ...options, measure: () => ({ freeBytes: 50 }) });
  assert.equal(code, 1);
  assert.equal(fs.existsSync(marker), false);
  assert.match(fs.readFileSync(path.join(root, 'resource-result.json'), 'utf8'), /Insufficient runner disk/);
  assert.match(fs.readFileSync(path.join(root, 'resources.jsonl'), 'utf8'), /"freeBytes":50/);
});

for (const exitCode of [0, 7]) {
  test(`preserves the test exit code ${exitCode}`, async t => {
    const { root, options } = fixture(t);
    assert.equal(await runNightlyTest([process.execPath, '-e', `process.exit(${exitCode})`], options), exitCode);
    const result = JSON.parse(fs.readFileSync(path.join(root, 'resource-result.json'), 'utf8'));
    assert.equal(result.started, true);
    assert.equal(result.childExit.code, exitCode);
    assert.equal(result.reason, '');
  });
}

for (const cooperative of [true, false]) {
  test(`disk exhaustion fails even when the test ${cooperative ? 'exits zero on termination' : 'ignores termination'}`, { timeout: 5000 }, async t => {
    const { root, options } = fixture(t);
    const ready = path.join(root, 'ready');
    const script = [
      `process.on('SIGTERM', () => { ${cooperative ? 'process.exit(0);' : ''} });`,
      'require("node:fs").writeFileSync(process.argv[1], "ready");',
      'setInterval(() => {}, 1000);',
    ].join('\n');
    const code = await runNightlyTest([process.execPath, '-e', script, ready], {
      ...options, measure: () => ({ freeBytes: fs.existsSync(ready) ? 10 : 200 }),
    });
    assert.equal(code, 1);
    const result = JSON.parse(fs.readFileSync(path.join(root, 'resource-result.json'), 'utf8'));
    assert.match(result.reason, /Runner disk reserve exhausted/);
    if (cooperative) assert.equal(result.childExit.code, 0);
    else assert.equal(result.childExit.signal, 'SIGKILL');
  });
}

test('resource sampling errors stop the test and remain in the saved result', { timeout: 5000 }, async t => {
  const { root, options } = fixture(t);
  const ready = path.join(root, 'ready');
  const code = await runNightlyTest([process.execPath, '-e',
    'require("node:fs").writeFileSync(process.argv[1], "ready"); setInterval(() => {}, 1000);', ready], {
    ...options,
    measure: () => {
      if (fs.existsSync(ready)) throw new Error('statfs unavailable');
      return { freeBytes: 200 };
    },
  });
  assert.equal(code, 1);
  const result = JSON.parse(fs.readFileSync(path.join(root, 'resource-result.json'), 'utf8'));
  assert.match(result.reason, /resource sampling failed: statfs unavailable/);
  assert.equal(result.finalSampleError, 'statfs unavailable');
});
