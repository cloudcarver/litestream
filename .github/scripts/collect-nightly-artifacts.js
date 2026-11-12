'use strict';

const fs = require('node:fs');
const path = require('node:path');

// Inspect file metadata only: successful runs must not read/copy database, WAL,
// or LTX payloads. Leave the source directories intact for failure diagnostics.
function collectArtifacts({ sourceDir, prefix, outputDir, status }) {
  fs.mkdirSync(outputDir, { recursive: true });
  const directories = fs.readdirSync(sourceDir, { withFileTypes: true })
    .filter(entry => entry.isDirectory() && entry.name.startsWith(prefix))
    .map(entry => entry.name)
    .sort();
  const files = [];
  let logBytes = 0;

  function visit(directory) {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const source = path.join(directory, entry.name);
      if (entry.isDirectory()) {
        visit(source);
        continue;
      }

      const stat = fs.lstatSync(source);
      const relativePath = path.relative(sourceDir, source);
      files.push({
        path: relativePath,
        bytes: stat.size,
        modified: stat.mtime.toISOString(),
        type: entry.isFile() ? 'file' : entry.isSymbolicLink() ? 'symlink' : 'other',
      });

      // Do not follow symlinks or copy arbitrary text/config files: only logs
      // belong in the small success artifact, including logs in hidden folders.
      if (entry.isFile() && entry.name.endsWith('.log')) {
        const destination = path.join(outputDir, 'logs', relativePath);
        fs.mkdirSync(path.dirname(destination), { recursive: true });
        fs.copyFileSync(source, destination);
        logBytes += stat.size;
      }
    }
  }

  for (const directory of directories) {
    visit(path.join(sourceDir, directory));
  }
  files.sort((a, b) => a.path.localeCompare(b.path));
  fs.writeFileSync(path.join(outputDir, 'files.jsonl'), files.map(file => `${JSON.stringify(file)}\n`).join(''));

  const testLog = path.join(outputDir, 'go-test.log');
  const results = fs.existsSync(testLog)
    ? fs.readFileSync(testLog, 'utf8').split('\n')
      .filter(line => /^(\s*--- (PASS|FAIL|SKIP):|PASS$|FAIL(\s|$)|ok\s|\?\s)/.test(line))
    : [];
  const summary = [
    '## Nightly test artifacts',
    '',
    `- Job status before artifact collection: ${status}`,
    `- Test directories: ${directories.length}`,
    `- Files inventoried: ${files.length}`,
    `- Source file bytes: ${files.reduce((total, file) => total + file.bytes, 0)}`,
    `- Copied log bytes: ${logBytes}`,
    `- Upload policy: ${status === 'success' ? 'logs, summary, and file manifest' : 'full test directories and reports'}`,
    '',
    'See `go-test.log` for test reports and assertions, `logs/` for process logs,',
    'and `files.jsonl` for original paths, byte sizes, and modification times.',
    '',
    '### Go test results',
    '',
    '```text',
    ...(results.length > 0 ? results : ['No Go test result lines were captured.']),
    '```',
    '',
  ].join('\n');
  fs.writeFileSync(path.join(outputDir, 'summary.md'), summary);
  return summary;
}

if (require.main === module) {
  const summary = collectArtifacts({
    sourceDir: process.argv[2],
    prefix: process.argv[3],
    outputDir: path.join(process.env.RUNNER_TEMP, 'nightly-artifacts'),
    status: process.env.NIGHTLY_JOB_STATUS,
  });
  fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, summary);
}

module.exports = { collectArtifacts };
