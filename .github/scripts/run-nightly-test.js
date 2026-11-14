'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawn } = require('node:child_process');

const GiB = 1024 ** 3;

function diskSample(directory) {
  const disk = fs.statfsSync(directory);
  return { freeBytes: disk.bavail * disk.bsize, freeMemoryBytes: os.freemem(), cpus: os.availableParallelism() };
}

async function runNightlyTest(command, {
  artifacts, filesystem, minimumFreeBytes, reserveBytes = 8 * GiB,
  intervalMs = 10000, graceMs = 10000, measure = diskSample,
}) {
  fs.mkdirSync(artifacts, { recursive: true });
  const resourceLog = path.join(artifacts, 'resources.jsonl');
  const record = () => {
    const sample = { at: new Date().toISOString(), ...measure(filesystem) };
    fs.appendFileSync(resourceLog, JSON.stringify(sample) + '\n');
    return sample;
  };
  const initial = record();
  const result = { command, minimumFreeBytes, reserveBytes, started: false, returncode: 1, reason: '' };
  const saveResult = () => fs.writeFileSync(path.join(artifacts, 'resource-result.json'), JSON.stringify(result, null, 2) + '\n');
  if (initial.freeBytes < minimumFreeBytes) {
    result.reason = `Insufficient runner disk: need ${(minimumFreeBytes / GiB).toFixed(1)} GiB free, have ${(initial.freeBytes / GiB).toFixed(1)} GiB. Select a larger runner with LITESTREAM_SOAK_RUNNER or soak_runner.`;
    console.error(result.reason);
    saveResult();
    return 1;
  }

  let timer;
  let forceTimer;
  let child;
  const signalGroup = signal => {
    try {
      process.kill(-child.pid, signal);
    } catch (error) {
      // An exited process group is already stopped. Other signal failures
      // must remain visible in the diagnostic result and job log.
      if (error.code !== 'ESRCH') {
        const message = `Could not send ${signal} to test process group: ${error.message}`;
        console.error(message);
        result.signalError = message;
      }
    }
  };
  const stop = reason => {
    if (result.reason) return;
    result.reason = reason;
    console.error(reason);
    signalGroup('SIGTERM');
    forceTimer = setTimeout(() => signalGroup('SIGKILL'), graceMs);
  };
  const interrupted = signal => stop(`Nightly test interrupted by ${signal}`);
  const onInterrupt = () => interrupted('SIGINT');
  const onTerminate = () => interrupted('SIGTERM');
  try {
    const exit = await new Promise((resolve, reject) => {
      child = spawn(command[0], command.slice(1), { stdio: 'inherit', detached: true });
      child.once('error', reject);
      child.once('spawn', () => {
        result.started = true;
        process.on('SIGINT', onInterrupt);
        process.on('SIGTERM', onTerminate);
        timer = setInterval(() => {
          try {
            const sample = record();
            if (sample.freeBytes < reserveBytes) {
              stop(`Runner disk reserve exhausted: ${sample.freeBytes} bytes free, ${reserveBytes} required. Stopping the test to preserve failure diagnostics.`);
            }
          } catch (error) {
            stop(`Runner resource sampling failed: ${error.message}`);
          }
        }, intervalMs);
      });
      child.once('exit', (code, signal) => resolve({ code, signal }));
    });
    result.childExit = exit;
    result.returncode = result.reason ? 1 : (exit.code ?? 1);
  } catch (error) {
    result.reason = `Could not run nightly test: ${error.message}`;
    console.error(result.reason);
  } finally {
    clearInterval(timer);
    clearTimeout(forceTimer);
    process.removeListener('SIGINT', onInterrupt);
    process.removeListener('SIGTERM', onTerminate);
    if (result.reason && result.started) signalGroup('SIGKILL');
    try {
      record();
    } catch (error) {
      result.returncode = 1;
      result.finalSampleError = error.message;
      console.error(`Final resource sample failed: ${error.message}`);
    }
    saveResult();
  }
  return result.returncode;
}

if (require.main === module) {
  const [minimum, ...command] = process.argv.slice(2);
  const minimumGiB = Number(minimum);
  if (!Number.isFinite(minimumGiB) || minimumGiB <= 8 || command.length === 0) {
    console.error('Usage: run-nightly-test.js <minimum-free-GiB greater than 8> <command> [args...]');
    process.exitCode = 1;
  } else {
    runNightlyTest(command, {
      artifacts: path.join(process.env.RUNNER_TEMP || os.tmpdir(), 'nightly-artifacts'),
      filesystem: os.tmpdir(), minimumFreeBytes: minimumGiB * GiB,
    }).then(code => { process.exitCode = code; }).catch(error => {
      console.error(error);
      process.exitCode = 1;
    });
  }
}

module.exports = { runNightlyTest };
