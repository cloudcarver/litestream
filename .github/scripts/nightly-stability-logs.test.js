'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { extractRace } = require('./nightly-stability-logs');

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
