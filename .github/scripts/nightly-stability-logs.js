'use strict';

// Keep the race detector's access stacks together. Ranking individual lines
// otherwise selects expected error logs from unrelated, successful tests.
function extractRace(logText) {
  const lines = logText.split('\n').map(line => {
    const clean = line.replace(/\u001b\[[0-9;]*m/g, '').replace(/\r/g, '');
    const parts = clean.split('\t');
    return (parts.length >= 4 ? parts.slice(3).join('\t') : clean)
      .replace(/^\uFEFF?\d{4}-\d{2}-\d{2}T[\d:.]+Z\s*/, '');
  });
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

module.exports = { extractRace };
