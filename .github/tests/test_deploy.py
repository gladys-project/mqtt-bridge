"""Deployment orchestration checks; no Docker daemon or server secrets required."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/deploy.sh'


class DeploymentTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = self.root / 'server settings.env'
        self.config.write_text('UNRELATED_SECRET=private-value\n')
        self.log = self.root / 'docker.jsonl'
        docker = self.root / 'docker'
        docker.write_text('''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
with open(os.environ['DOCKER_LOG'], 'a') as out:
    out.write(json.dumps({'args': args, 'image': os.environ.get('BRIDGE_IMAGE')}) + '\\n')
if args[0] == os.environ.get('FAIL_COMMAND'):
    sys.exit(9)
if args[0] == 'run':
    print('UNRELATED_SECRET=private-value')
''')
        docker.chmod(0o755)
        self.env = dict(os.environ, PATH=str(self.root) + ':' + os.environ['PATH'],
                        DOCKER_LOG=str(self.log), GITHUB_SHA='a' * 40,
                        GLADYS_ENV_FILE=str(self.config), TMPDIR=str(self.root))

    def run_deploy(self, **changes):
        result = subprocess.run(['bash', str(SCRIPT)], env=self.env | changes,
                                capture_output=True, text=True)
        self.assertNotIn('private-value', result.stdout + result.stderr)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return result, calls

    def test_readable_file_updates_only_bridge_with_commit_image(self):
        result, calls = self.run_deploy()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any(c['args'][0] == 'run' for c in calls))
        up = next(c for c in calls if 'up' in c['args'])
        self.assertEqual(up['args'], ['compose', '--env-file', str(self.config),
                         '--project-name', 'mqtt-kafka-bridge', 'up', '--detach',
                         '--no-build', '--pull', 'never', '--no-deps', '--wait',
                         '--wait-timeout', '60', 'mqtt-kafka-bridge'])
        self.assertEqual(up['image'], 'mqtt-kafka-bridge:' + 'a' * 40)
        self.assertFalse(any('down' in c['args'] or 'infra/compose.yml' in c['args'] for c in calls))

    def test_host_file_can_be_read_without_runner_mount(self):
        host_path = '/etc/gladys/not-mounted-in-test.env'
        result, calls = self.run_deploy(GLADYS_ENV_FILE=host_path)
        self.assertEqual(result.returncode, 0, result.stderr)
        reader = next(c['args'] for c in calls if c['args'][0] == 'run')
        self.assertIn('type=bind,src=' + host_path + ',dst=/run/gladys.env,readonly', reader)
        self.assertIn('--rm', reader)
        self.assertIn('--read-only', reader)
        self.assertIn('--log-driver', reader)
        self.assertEqual(reader[reader.index('--log-driver') + 1], 'none')
        self.assertEqual(reader[reader.index('--network') + 1], 'none')
        compose = next(c['args'] for c in calls if c['args'][0] == 'compose')
        temporary_env = Path(compose[compose.index('--env-file') + 1])
        self.assertFalse(temporary_env.exists(), 'temporary secrets must be removed')

    def test_failed_host_read_prevents_build_and_deployment(self):
        result, calls = self.run_deploy(GLADYS_ENV_FILE='/etc/gladys/not-mounted-in-test.env', FAIL_COMMAND='run')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(c['args'][0] == 'build' or 'up' in c['args'] for c in calls))
        self.assertEqual(list(self.root.glob('gladys-deploy.*')), [])

    def test_failed_build_prevents_replacing_service(self):
        result, calls = self.run_deploy(FAIL_COMMAND='build')
        self.assertEqual(result.returncode, 9)
        self.assertFalse(any('up' in c['args'] for c in calls))

    def test_invalid_compose_prevents_build(self):
        result, calls = self.run_deploy(FAIL_COMMAND='compose')
        self.assertEqual(result.returncode, 9)
        self.assertFalse(any(c['args'][0] == 'build' for c in calls))


if __name__ == '__main__':
    unittest.main()
