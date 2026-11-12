'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');
const { collectArtifacts } = require('./collect-nightly-artifacts');

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'nightly-artifacts-test-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const sourceDir = path.join(root, 'tmp');
  const outputDir = path.join(root, 'reports');
  fs.mkdirSync(sourceDir);
  fs.mkdirSync(outputDir);
  return { sourceDir, outputDir, prefix: 'litestream-soak-', status: 'success' };
}

function write(root, relativePath, content) {
  const destination = path.join(root, relativePath);
  fs.mkdirSync(path.dirname(destination), { recursive: true });
  fs.writeFileSync(destination, content);
}

test('success keeps logs and metadata while leaving payloads intact for diagnostics', t => {
  const options = fixture(t);
  const payloads = {
    'litestream-soak-a/source.db': 'database',
    'litestream-soak-a/source.db-wal': 'wal',
    'litestream-soak-a/source.db-shm': 'shm',
    'litestream-soak-a/restored.db': 'restore',
    'litestream-soak-a/replica/0001.ltx': 'replica',
    'litestream-soak-a/.source.db-litestream/ltx/0001.ltx': 'local ltx',
    'litestream-soak-a/litestream.yml': 'config',
  };
  const logs = {
    'litestream-soak-a/litestream.log': 'first log',
    'litestream-soak-b/litestream.log': 'second log',
    'litestream-soak-a/.diagnostics/monitor.log': 'hidden log',
  };
  for (const [name, content] of Object.entries({ ...payloads, ...logs })) {
    write(options.sourceDir, name, content);
  }
  write(options.sourceDir, 'unrelated/litestream.log', 'unrelated log');
  write(options.outputDir, 'go-test.log', '=== RUN   TestSoak\n--- PASS: TestSoak (1.00s)\nPASS\nok\tpackage\t1.0s\n');

  const summary = collectArtifacts(options);
  assert.deepEqual(fs.readdirSync(options.outputDir).sort(), ['files.jsonl', 'go-test.log', 'logs', 'summary.md']);
  assert.deepEqual(fs.readdirSync(path.join(options.outputDir, 'logs')).sort(), ['litestream-soak-a', 'litestream-soak-b']);
  assert.deepEqual(fs.readdirSync(path.join(options.outputDir, 'logs/litestream-soak-a')).sort(), ['.diagnostics', 'litestream.log']);
  for (const [name, content] of Object.entries(logs)) {
    assert.equal(fs.readFileSync(path.join(options.outputDir, 'logs', name), 'utf8'), content);
  }
  const manifest = fs.readFileSync(path.join(options.outputDir, 'files.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
  assert.equal(manifest.length, Object.keys(payloads).length + Object.keys(logs).length);
  for (const [name, content] of Object.entries(payloads)) {
    assert.equal(fs.readFileSync(path.join(options.sourceDir, name), 'utf8'), content);
    assert.equal(fs.existsSync(path.join(options.outputDir, 'logs', name)), false);
    const record = manifest.find(file => file.path === name);
    assert.equal(record.bytes, Buffer.byteLength(content));
    assert.equal(record.modified, fs.statSync(path.join(options.sourceDir, name)).mtime.toISOString());
  }
  assert.match(summary, /Test directories: 2/);
  assert.match(summary, /--- PASS: TestSoak/);
  assert.match(summary, /logs, summary, and file manifest/);
});

test('records symlinks without following them into payloads or unrelated directories', t => {
  const options = fixture(t);
  write(options.sourceDir, 'unrelated/source.db', 'database');
  fs.mkdirSync(path.join(options.sourceDir, 'litestream-soak-a'));
  fs.symlinkSync('../unrelated/source.db', path.join(options.sourceDir, 'litestream-soak-a/payload.log'));
  fs.symlinkSync('../unrelated', path.join(options.sourceDir, 'litestream-soak-a/linked-dir'));

  collectArtifacts(options);

  const manifest = fs.readFileSync(path.join(options.outputDir, 'files.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
  assert.equal(manifest.length, 2);
  assert.ok(manifest.every(file => file.type === 'symlink'));
  assert.equal(fs.existsSync(path.join(options.outputDir, 'logs')), false);
});

test('reports setup failures even when no test directory or log was created', t => {
  const options = { ...fixture(t), status: 'failure' };

  const summary = collectArtifacts(options);

  assert.equal(fs.readFileSync(path.join(options.outputDir, 'files.jsonl'), 'utf8'), '');
  assert.match(summary, /Job status before artifact collection: failure/);
  assert.match(summary, /Test directories: 0/);
  assert.match(summary, /full test directories and reports/);
  assert.match(summary, /No Go test result lines were captured/);
});
