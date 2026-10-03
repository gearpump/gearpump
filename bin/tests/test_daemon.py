# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Executable daemon safety regressions; no installed Gearpump cluster required."""
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest


class DaemonTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        source = Path(__file__).resolve().parents[1]
        for name in ('gear-daemon.sh', 'config.sh'):
            shutil.copy(source / name, self.bin / name)
        launcher = self.bin / 'master'
        launcher.write_text('#!/usr/bin/env bash\nexec "' + sys.executable +
                            '" -c "import time; time.sleep(120)" '
                            'io.gearpump.cluster.main.Master\n')
        launcher.chmod(0o700)
        self.pid_dir = self.root / 'run'
        self.log_dir = self.root / 'logs'
        self.pid = self.pid_dir / 'gear-regression-master.pid'
        self.environment = dict(os.environ, GEARPUMP_PID_DIR=str(self.pid_dir),
                                GEARPUMP_LOG_DIR=str(self.log_dir),
                                GEARPUMP_IDENT_STRING='regression')
        self.children = []
        self.processes = []

    def tearDown(self):
        if self.pid.is_file() and not self.pid.is_symlink():
            for record in self.pid.read_text().splitlines():
                value = record.split('|')[0]
                if value.isdecimal():
                    self.children.append(int(value))
        for child in self.children:
            try:
                os.kill(child, signal.SIGTERM)
            except ProcessLookupError:
                pass
        for process in self.processes:
            process.wait(timeout=5)
        self.temporary.cleanup()

    def run_daemon(self, operation):
        return subprocess.run(['bash', str(self.bin / 'gear-daemon.sh'),
                               operation, 'master'], env=self.environment,
                              capture_output=True, text=True, timeout=10)

    def test_private_start_stop_and_unique_logs(self):
        self.assertEqual(self.run_daemon('start').returncode, 0)
        self.assertEqual(self.run_daemon('start').returncode, 0)
        records = self.pid.read_text().splitlines()
        self.children.extend(int(record.split('|')[0]) for record in records)
        self.assertEqual(len(records), 2)
        self.assertEqual(self.pid.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.pid_dir.stat().st_mode & 0o777, 0o700)
        logs = list(self.log_dir.iterdir())
        self.assertEqual(len(logs), 2)
        self.assertTrue(all(p.stat().st_mode & 0o777 == 0o600 for p in logs))
        self.assertEqual(self.run_daemon('stop').returncode, 0)
        self.assertEqual(len(self.pid.read_text().splitlines()), 1)
        self.assertEqual(self.run_daemon('stop-all').returncode, 0)
        self.assertFalse(self.pid.exists())

    def test_symlink_pid_rejected_without_writing_target(self):
        self.pid_dir.mkdir(mode=0o700)
        target = self.root / 'target'
        target.write_text('untouched')
        self.pid.symlink_to(target)
        self.assertNotEqual(self.run_daemon('start').returncode, 0)
        self.assertEqual(target.read_text(), 'untouched')

    def test_unrelated_process_is_not_signaled(self):
        process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'])
        self.children.append(process.pid)
        self.processes.append(process)
        stamp = subprocess.check_output(['ps', '-p', str(process.pid),
                                         '-o', 'lstart='], text=True).strip()
        self.pid_dir.mkdir(mode=0o700)
        self.pid.write_text(f'{process.pid}|{os.getuid()}|{stamp}|master\n')
        self.pid.chmod(0o600)
        result = self.run_daemon('stop-all')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('identity mismatch', result.stderr)
        self.assertIsNone(process.poll())

    def test_shared_directory_rejected(self):
        self.pid_dir.mkdir(mode=0o755)
        self.pid_dir.chmod(0o755)
        self.assertNotEqual(self.run_daemon('start').returncode, 0)
        self.assertFalse(self.pid.exists())

    def test_concurrent_operation_lock(self):
        self.pid_dir.mkdir(mode=0o700)
        self.log_dir.mkdir(mode=0o700)
        Path(str(self.pid) + '.lock').mkdir(mode=0o700)
        result = self.run_daemon('start')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('locked', result.stderr)
        self.assertFalse(self.pid.exists())


if __name__ == '__main__':
    unittest.main()
