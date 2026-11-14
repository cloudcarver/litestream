'use strict';

// Keep the race detector's access stacks together. Ranking individual lines
// otherwise selects expected error logs from unrelated, successful tests.
function logLines(logText) {
  return logText.split('\n').map(line => {
    const clean = line.replace(/\u001b\[[0-9;]*m/g, '').replace(/\r/g, '');
    const parts = clean.split('\t');
    return (parts.length >= 4 ? parts.slice(3).join('\t') : clean)
      .replace(/^\uFEFF?\d{4}-\d{2}-\d{2}T[\d:.]+Z\s*/, '');
  });
}

function extractRace(logText) {
  const lines = logLines(logText);
  const start = lines.findIndex(line => line.includes('WARNING: DATA RACE'));
  if (start < 0) return null;

  let end = start + 1;
  while (end < lines.length && !/^=+$/.test(lines[end].trim()) && end - start < 120) end += 1;
  const block = lines.slice(start, end);
  const accesses = [];
  for (let index = 0; index < block.length; index += 1) {
    if (!/^(?:Previous )?(?:Read|Write) at /i.test(block[index].trim())) continue;
    const frame = block.slice(index + 1).find(line => line.trim());
    if (frame) accesses.push(frame.trim().replace(/^.*\//, '').replace(/\(\)$/, ''));
  }
  return {
    signature: accesses.length > 0 ? `data race: ${accesses.join(' vs ')}` : 'data race',
    snippets: block.filter(line => line.trim()),
  };
}

function extractTestFailure(logText) {
  const lines = logLines(logText);
  const end = lines.findIndex(line => /^\s*--- FAIL: /u.test(line));
  if (end < 0) return null;
  const name = lines[end].match(/--- FAIL: (\S+)/u)[1];
  let start = end - 1;
  while (start >= 0) {
    const boundary = lines[start].match(/^=== (?:RUN|CONT)\s+(\S+)/u);
    if (boundary && boundary[1] === name) break;
    start -= 1;
  }
  // Preserve test assertions even if verbose runtime shutdown logs followed
  // them. Without a matching test boundary, report only the known failure.
  const assertions = start >= 0
    ? lines.slice(start + 1, end).filter(line => /\S+_test\.go:\d+:/u.test(line)).slice(-3)
    : [];
  return { signature: `test failed: ${name}`, snippets: [...assertions, lines[end]] };
}

function extractInfrastructureFailure(annotations) {
  const failures = annotations.filter(item => item.annotation_level === 'failure');
  const disk = failures.find(item => /no space left on device|disk quota exceeded|database or disk is full/iu.test(item.message));
  const selected = disk || failures[0];
  if (!selected) return null;
  const snippets = selected.message.split('\n').filter(line => line.trim()).slice(0, 5);
  return { signature: disk ? 'runner disk exhausted' : snippets[0], snippets };
}

module.exports = { extractRace, extractTestFailure, extractInfrastructureFailure };
