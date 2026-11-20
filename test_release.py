import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import release

# global git hooks must not run against the fixture repositories
GIT_ENV = {
    "GIT_CONFIG_COUNT": "3",
    "GIT_CONFIG_KEY_0": "core.hooksPath",
    "GIT_CONFIG_VALUE_0": "/dev/null",
    "GIT_CONFIG_KEY_1": "user.name",
    "GIT_CONFIG_VALUE_1": "test",
    "GIT_CONFIG_KEY_2": "user.email",
    "GIT_CONFIG_VALUE_2": "test@example.com",
}


def git(cwd: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=cwd, text=True, stderr=subprocess.STDOUT).strip()


class ReleaseTest(unittest.TestCase):
    def setUp(self) -> None:
        env = mock.patch.dict(os.environ, GIT_ENV)
        env.start()
        self.addCleanup(env.stop)

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)

        self.seed = self.tmp / "seed"
        self.seed.mkdir()
        git(self.seed, "init", "-q", "-b", "main")
        (self.seed / "go.mod").write_text("module example.com/m\n")
        git(self.seed, "add", "go.mod")
        git(self.seed, "commit", "-qm", "one")
        git(self.seed, "tag", "v1.0.0")

        self.remote = self.tmp / "remote.git"
        git(self.tmp, "clone", "-q", "--bare", str(self.seed), str(self.remote))
        self.work = self.tmp / "work"
        git(self.tmp, "clone", "-q", str(self.remote), str(self.work))

    def release(self, *extra: str) -> bool:
        with mock.patch.object(sys, "argv", ["release.py", "-m", str(self.work / "go.mod"), *extra]):
            return release.main()

    def remote_ref(self, ref: str) -> str:
        """Returns the commit ref points to on the remote, or "" when the remote has no such ref."""
        result = subprocess.run(["git", "rev-parse", "--verify", "--quiet", ref + "^{commit}"],
                                cwd=self.remote, capture_output=True, text=True, check=False)
        return result.stdout.strip()

    def test_tags_head_and_pushes_only_the_new_tag(self) -> None:
        git(self.work, "commit", "-q", "--allow-empty", "-m", "two")
        # a local tag that differs from the remote one must not make the push fail
        git(self.work, "tag", "-f", "v1.0.0", "HEAD")

        self.assertTrue(self.release())

        head = git(self.work, "rev-parse", "HEAD")
        self.assertEqual(head, self.remote_ref("refs/tags/v1.0.1"))
        self.assertEqual(head, self.remote_ref("refs/heads/main"))
        self.assertNotEqual(head, self.remote_ref("refs/tags/v1.0.0"))

    def test_refuses_a_head_behind_origin_main(self) -> None:
        git(self.seed, "commit", "-q", "--allow-empty", "-m", "two")
        git(self.seed, "push", "-q", str(self.remote), "main")

        self.assertFalse(self.release())

        self.assertEqual("", self.remote_ref("refs/tags/v1.0.1"))
        self.assertEqual("", git(self.work, "tag", "-l", "v1.0.1"))

    def test_refuses_a_dirty_workspace(self) -> None:
        (self.work / "go.mod").write_text("module example.com/changed\n")

        self.assertFalse(self.release())

        self.assertEqual("", git(self.work, "tag", "-l", "v1.0.1"))

    def test_rejected_push_leaves_neither_remote_main_nor_local_tag(self) -> None:
        # another release pushed v1.0.1 on a commit that git fetch does not follow
        git(self.seed, "checkout", "-q", "-b", "side")
        git(self.seed, "commit", "-q", "--allow-empty", "-m", "side")
        git(self.seed, "tag", "v1.0.1")
        git(self.seed, "push", "-q", str(self.remote), "v1.0.1")
        git(self.work, "commit", "-q", "--allow-empty", "-m", "two")
        remote_main = self.remote_ref("refs/heads/main")

        with self.assertRaises(subprocess.CalledProcessError):
            self.release()

        self.assertEqual(remote_main, self.remote_ref("refs/heads/main"))
        self.assertEqual("", git(self.work, "tag", "-l", "v1.0.1"))


if __name__ == "__main__":
    unittest.main()
