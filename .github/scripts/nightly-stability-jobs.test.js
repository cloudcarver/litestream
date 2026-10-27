'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');
const { classifyJobs } = require('./nightly-stability-jobs');

test('classifies malformed failed jobs without throwing', () => {
  const failures = classifyJobs([
    {
      id: 1,
      name: 'Race Detector Sweep',
      conclusion: 'failure',
      steps: {},
    },
  ]);

  assert.equal(failures.length, 1);
  assert.match(failures[0].classificationError, /steps must be an array/);
});

test('classifies malformed jobs alongside recognized failures', () => {
  const failures = classifyJobs([
    {
      id: 1,
      name: 'Race Detector Sweep',
      conclusion: 'failure',
      steps: [],
    },
    {
      id: 2,
      name: 'MinIO Soak',
      steps: [],
    },
  ]);

  assert.equal(failures.length, 2);
  assert.equal(failures[0].name, 'Race Detector Sweep');
  assert.equal(failures[1].name, 'MinIO Soak');
  assert.match(failures[1].classificationError, /conclusion must be a non-empty string/);
});

test('ignores successful and notifier jobs', () => {
  const failures = classifyJobs([
    {
      id: 1,
      name: 'Race Detector Sweep',
      conclusion: 'success',
      steps: [],
    },
    {
      id: 2,
      name: 'Notify on Failure',
      conclusion: '',
      steps: [],
    },
  ]);

  assert.deepEqual(failures, []);
});

test('retains failed steps when another step has no conclusion', () => {
  const failures = classifyJobs([
    {
      id: 1,
      name: 'LTX Behavioral Soak (high-volume)',
      conclusion: 'failure',
      steps: [
        { name: 'Run LTX behavioral test', status: 'completed', conclusion: 'failure' },
        { name: 'Upload test artifacts', status: 'queued', conclusion: null },
      ],
    },
  ]);

  assert.equal(failures.length, 1);
  assert.equal(failures[0].classificationError, '');
  assert.deepEqual(failures[0].failedSteps, ['Run LTX behavioral test']);
});

test('reports cancelled jobs whose steps have no conclusion', () => {
  const failures = classifyJobs([
    {
      id: 1,
      name: 'MinIO Soak',
      conclusion: 'cancelled',
      steps: [{ name: 'Run MinIO soak test', status: 'in_progress', conclusion: null }],
    },
  ]);

  assert.equal(failures.length, 1);
  assert.equal(failures[0].classificationError, '');
  assert.deepEqual(failures[0].failedSteps, []);
});

test('continues to reject missing or invalid step conclusions', () => {
  for (const conclusion of [undefined, '', ' ', false, 0, {}, []]) {
    const failures = classifyJobs([
      {
        id: 1,
        name: 'MinIO Soak',
        conclusion: 'failure',
        steps: [{ name: 'Run MinIO soak test', conclusion }],
      },
    ]);

    assert.equal(failures.length, 1);
    assert.match(failures[0].classificationError, /steps\[0\].conclusion/);
  }
});

test('null step conclusions do not hide malformed job conclusions', () => {
  const failures = classifyJobs([
    {
      id: 1,
      name: 'MinIO Soak',
      conclusion: null,
      steps: [{ name: 'Run MinIO soak test', conclusion: null }],
    },
  ]);

  assert.equal(failures.length, 1);
  assert.match(failures[0].classificationError, /: conclusion must be a non-empty string/);
});

test('downloads failure logs when a later step has a null conclusion', async () => {
  const jobs = [
    {
      id: 1,
      name: 'LTX Behavioral Soak (high-volume)',
      conclusion: 'failure',
      steps: [
        { name: 'Run LTX behavioral test', status: 'completed', conclusion: 'failure' },
        { name: 'Upload test artifacts', status: 'queued', conclusion: null },
      ],
    },
  ];
  const createdIssues = [];
  const logURLs = [];
  const github = {
    paginate: async route => (typeof route === 'string' ? jobs : []),
    rest: {
      issues: {
        listForRepo: () => {},
        create: async input => {
          createdIssues.push(input);
          return { data: { number: 123, html_url: 'https://github.com/benbjohnson/litestream/issues/123' } };
        },
      },
    },
  };

  const originalWorkspace = process.env.GITHUB_WORKSPACE;
  process.env.GITHUB_WORKSPACE = process.cwd();
  try {
    await runNotifierScript({
      context: {
        repo: { owner: 'benbjohnson', repo: 'litestream' },
        runId: 456,
        serverUrl: 'https://github.com',
        ref: 'refs/heads/test',
        sha: '1234567890abcdef',
      },
      github,
      core: {},
      fetch: async url => {
        logURLs.push(url);
        return { ok: true, text: async () => '--- FAIL: TestLTXBehavior (1.00s)' };
      },
    });
  } finally {
    if (originalWorkspace === undefined) {
      delete process.env.GITHUB_WORKSPACE;
    } else {
      process.env.GITHUB_WORKSPACE = originalWorkspace;
    }
  }

  assert.deepEqual(logURLs, ['https://api.github.com/repos/benbjohnson/litestream/actions/jobs/1/logs']);
  assert.equal(createdIssues.length, 1);
  assert.match(createdIssues[0].body, /Run LTX behavioral test/);
  assert.match(createdIssues[0].body, /--- FAIL: TestLTXBehavior/);
  assert.doesNotMatch(createdIssues[0].body, /Malformed job record/);
});

test('creates a tracking issue for malformed job objects', async () => {
  const jobs = [
    {
      id: 1,
      name: 'Race Detector Sweep',
      conclusion: 'failure',
      steps: {},
    },
    {
      id: 2,
      name: 'MinIO Soak',
      steps: [],
    },
  ];
  const createdIssues = [];
  const listForRepo = () => {};
  const github = {
    paginate: async route => (typeof route === 'string' ? jobs : []),
    rest: {
      issues: {
        listForRepo,
        create: async input => {
          createdIssues.push(input);
          return {
            data: {
              number: 123,
              html_url: 'https://github.com/benbjohnson/litestream/issues/123',
            },
          };
        },
      },
    },
  };
  const context = {
    repo: { owner: 'benbjohnson', repo: 'litestream' },
    runId: 456,
    serverUrl: 'https://github.com',
    ref: 'refs/heads/test',
    sha: '1234567890abcdef',
  };

  const originalWorkspace = process.env.GITHUB_WORKSPACE;
  process.env.GITHUB_WORKSPACE = process.cwd();
  try {
    await runNotifierScript({
      context,
      github,
      core: {},
      fetch: async () => {
        throw new Error('malformed records must not fetch job logs');
      },
    });
  } finally {
    if (originalWorkspace === undefined) {
      delete process.env.GITHUB_WORKSPACE;
    } else {
      process.env.GITHUB_WORKSPACE = originalWorkspace;
    }
  }

  assert.equal(createdIssues.length, 1);
  assert.match(createdIssues[0].body, /Race Detector Sweep/);
  assert.match(createdIssues[0].body, /steps must be an array/);
  assert.match(createdIssues[0].body, /MinIO Soak/);
  assert.match(createdIssues[0].body, /conclusion must be a non-empty string/);
});

async function runNotifierScript({ context, github, core, fetch }) {
  const workflowPath = path.join(process.cwd(), '.github/workflows/nightly-stability.yml');
  const lines = fs.readFileSync(workflowPath, 'utf8').split('\n');
  const markerIndex = lines.findIndex(line => line.trim() === 'script: |');
  assert.notEqual(markerIndex, -1);

  const markerIndent = lines[markerIndex].length - lines[markerIndex].trimStart().length;
  const scriptLines = [];
  for (const line of lines.slice(markerIndex + 1)) {
    const indent = line.length - line.trimStart().length;
    if (line.trim() !== '' && indent <= markerIndent) {
      break;
    }
    scriptLines.push(line.slice(markerIndent + 2));
  }

  const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
  const execute = new AsyncFunction('require', 'context', 'github', 'core', 'fetch', scriptLines.join('\n'));
  await execute(require, context, github, core, fetch);
}

for (const failureStage of ['list', 'create', 'none']) {
  test(`preserves race evidence when Issues ${failureStage === 'none' ? 'succeeds' : failureStage + ' fails'}`, async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nightly-summary-'));
    const summaryPath = path.join(dir, 'summary.md');
    const prior = { workspace: process.env.GITHUB_WORKSPACE, summary: process.env.GITHUB_STEP_SUMMARY };
    process.env.GITHUB_WORKSPACE = process.cwd();
    process.env.GITHUB_STEP_SUMMARY = summaryPath;
    const failures = [];
    const created = [];
    const disabled = Object.assign(new Error('Issues has been disabled in this repository.'), { status: 410 });
    const raceLog = [
      '2026-10-26T08:45:50.7823556Z WARNING: DATA RACE',
      '2026-10-26T08:45:50.7823556Z Write at 0x123 by goroutine 1:',
      '2026-10-26T08:45:50.7823556Z   github.com/benbjohnson/litestream.(*DB).Close()',
      '2026-10-26T08:45:50.7823556Z     /work/litestream/db.go:860 +0x3f4',
      '2026-10-26T08:45:50.7823556Z Previous read at 0x123 by goroutine 2:',
      '2026-10-26T08:45:50.7823556Z   github.com/benbjohnson/litestream.(*DB).writeLTXFromDB()',
      '2026-10-26T08:45:50.7823556Z     /work/litestream/db.go:2324 +0x6f1',
      '2026-10-26T08:45:50.7823556Z ==================',
      'level=WARN msg="shutdown sync failed, retrying" error="write ltx file: persistent error"',
    ].join('\n');
    const jobs = [{
      id: 1,
      name: 'Race Detector Sweep',
      conclusion: 'failure',
      steps: [{ name: 'Run tests with race detector (3x)', conclusion: 'failure' }],
    }];
    const assertEvidenceSaved = () => {
      const summary = fs.readFileSync(summaryPath, 'utf8');
      assert.match(summary, /data race: litestream\.\(\*DB\)\.Close vs litestream\.\(\*DB\)\.writeLTXFromDB/);
      assert.match(summary, /db.go:860/);
      assert.match(summary, /db.go:2324/);
      assert.doesNotMatch(summary, /persistent error/);
    };
    try {
      await runNotifierScript({
        context: {
          repo: { owner: 'cloudcarver', repo: 'litestream' }, runId: 456,
          serverUrl: 'https://github.com', ref: 'refs/heads/main', sha: '5ffc3766d7c4af8a',
        },
        github: {
          paginate: async route => {
            if (typeof route === 'string') return jobs;
            assertEvidenceSaved();
            if (failureStage === 'list') throw disabled;
            return [];
          },
          rest: { issues: {
            listForRepo: () => {},
            create: async input => {
              assertEvidenceSaved();
              if (failureStage === 'create') throw disabled;
              created.push(input);
              return { data: { number: 42, html_url: 'https://github.com/cloudcarver/litestream/issues/42' } };
            },
          } },
        },
        core: { setFailed: message => failures.push(message) },
        fetch: async () => ({ ok: true, text: async () => raceLog }),
      });
      assertEvidenceSaved();
      const summary = fs.readFileSync(summaryPath, 'utf8');
      if (failureStage === 'none') {
        assert.deepEqual(failures, []);
        assert.equal(created.length, 1);
        assert.match(created[0].title, /data race/);
        assert.match(summary, /Tracking issue: #42/);
      } else {
        assert.equal(created.length, 0);
        assert.equal(failures.length, 1);
        assert.match(failures[0], /HTTP 410/);
        assert.match(summary, /Notification failed:.*HTTP 410/);
      }
    } finally {
      for (const [key, value] of [['GITHUB_WORKSPACE', prior.workspace], ['GITHUB_STEP_SUMMARY', prior.summary]]) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
      fs.rmSync(dir, { recursive: true });
    }
  });
}
