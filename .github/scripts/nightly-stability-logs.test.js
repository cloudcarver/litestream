'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { extractRace, extractTestFailure, extractInfrastructureFailure } = require('./nightly-stability-logs');

const race = [
  '==================',
  'WARNING: DATA RACE',
  'Write at 0x00c000ae2750 by goroutine 8609:',
  '  github.com/benbjohnson/litestream.(*DB).Close()',
  '      /home/runner/work/litestream/litestream/db.go:860 +0x3f4',
  '',
  'Previous read at 0x00c000ae2750 by goroutine 9786:',
  '  github.com/benbjohnson/litestream.(*DB).writeLTXFromDB()',
  '      /home/runner/work/litestream/litestream/db.go:2324 +0x6f1',
  '  github.com/benbjohnson/litestream.(*DB).snapshotReader.func1()',
  '      /home/runner/work/litestream/litestream/db.go:2884 +0x10e6',
  '==================',
];

test('extracts both race access stacks despite unrelated expected errors', () => {
  for (const prefix of ['', '2026-10-26T08:45:50.7823556Z ', 'Race Detector Sweep\tRun tests\t2026-10-26T08:45:50Z\t']) {
    const result = extractRace([
      ...race.map(line => prefix + line),
      'level=WARN msg="shutdown sync failed" error="write ltx file: persistent error"',
      '--- FAIL: TestStore_Integration (10.06s)',
    ].join('\n'));
    assert.equal(result.signature, 'data race: litestream.(*DB).Close vs litestream.(*DB).writeLTXFromDB');
    assert.ok(result.snippets.some(line => line.includes('db.go:860')));
    assert.ok(result.snippets.some(line => line.includes('db.go:2324')));
    assert.ok(result.snippets.some(line => line.includes('snapshotReader.func1')));
    assert.ok(result.snippets.every(line => !line.includes('persistent error')));
  }
});

test('keeps non-race failures on the general failure classification path', () => {
  assert.equal(extractRace('--- FAIL: TestOther\nerror="disk full"'), null);
});

test('retains a truncated race report without inventing missing frames', () => {
  const result = extractRace('WARNING: DATA RACE\nWrite at 0x12 by goroutine 1:');
  assert.equal(result.signature, 'data race');
  assert.equal(result.snippets.length, 2);
});

test('retains the failed Go test assertion despite later successful cleanup logs', () => {
  const result = extractTestFailure([
    '2026-11-13T18:10:00.000Z === RUN   TestDB_LockExecDoesNotStarveQueuedWaiter',
    '2026-11-13T18:10:00.001Z     db_internal_test.go:271: later lock acquired before queued waiter; err=context deadline exceeded',
    '2026-11-13T18:10:00.002Z --- FAIL: TestDB_LockExecDoesNotStarveQueuedWaiter (0.16s)',
    '2026-11-13T18:10:01.000Z === RUN   TestRestore_MultipleSyncs',
    '2026-11-13T18:10:02.000Z level=DEBUG msg="db close" error=<nil>',
    '2026-11-13T18:10:02.001Z --- PASS: TestRestore_MultipleSyncs (1.00s)',
  ].join('\n'));
  assert.match(result.signature, /TestDB_LockExecDoesNotStarveQueuedWaiter/);
  assert.ok(result.snippets.some(line => line.includes('later lock acquired before queued waiter')));
  assert.ok(result.snippets.every(line => !line.includes('<nil>')));
});

test('does not invent a Go failure from a successful test or nil error', () => {
  assert.equal(extractTestFailure('=== RUN   TestOK\nerror=<nil>\n--- PASS: TestOK (0.1s)'), null);
});

test('recovers disk exhaustion from check annotations when job logs are unavailable', () => {
  const result = extractInfrastructureFailure([
    { annotation_level: 'notice', message: 'ubuntu-latest will migrate' },
    { annotation_level: 'failure', message: "System.IO.IOException: No space left on device : '/home/runner/actions-runner/_diag/Worker.log'\n at StreamWriter.Flush()" },
  ]);
  assert.equal(result.signature, 'runner disk exhausted');
  assert.match(result.snippets[0], /No space left on device/);
  assert.equal(extractInfrastructureFailure([{ annotation_level: 'notice', message: 'No space left on device' }]), null);
});
