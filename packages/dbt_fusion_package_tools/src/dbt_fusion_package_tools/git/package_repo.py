import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from git import PathLike, Repo, TagReference
from git.exc import GitCommandError
from git.repo.fun import is_git_dir
from git.util import IterableList

from dbt_fusion_package_tools.exceptions import GitOperationError


@dataclass
class DbtPackageRepo:
    repo_name: Optional[str] = None
    git_clone_url: Optional[PathLike] = None
    github_organization: Optional[str] = None
    github_repo_name: Optional[str] = None
    git_repo: Repo = field(init=False)
    local_path: Optional[PathLike] = None
    overwrite_local_path: bool = False

    def __post_init__(self):
        # set repo name
        if not self.repo_name:
            if self.github_repo_name:
                self.repo_name = self.github_repo_name
            else:
                self.repo_name = "default_repo_name"
        # use repo name for local path if undefined
        if not self.local_path:
            self.local_path = Path.cwd() / self.repo_name
        # if path doesn't exist, try to create
        self._check_dir_and_create_if_needed(self.local_path)
        # if directory is already the repo, just represent it as Repo
        if self._check_if_directory_contains_repo(self.local_path):
            self.git_repo = Repo(path=self.local_path)
            return

        # if not, clone from URL
        # if no URL defined, try to construct from github
        if not self.git_clone_url:
            self.git_clone_url = self.github_repo_url()
        # make sure path ends in git
        if self.git_clone_url:
            self.git_clone_url = self.git_url(self.git_clone_url)
        else:
            return
        # now clone
        self._clone_repo()

        # if we don't have a local repo at this point, error
        if not self.git_repo:
            raise GitOperationError("Git repo not created, check parameters")

    def _check_if_directory_contains_repo(self, path: Optional[PathLike]):
        return path and is_git_dir(path)

    def git_url(self, path: PathLike) -> str:
        if str(path)[-4:] == ".git":
            return str(path)
        else:
            return str(path) + str(".git")

    def _check_dir_and_create_if_needed(self, path: PathLike) -> bool:
        exists = Path(path).exists()
        is_dir = Path(path).is_dir()
        # if it's not a directory, something's wrong - exit
        if exists and not is_dir:
            return False
        # if overwriting, delete contents
        elif exists and is_dir and self.overwrite_local_path:
            try:
                shutil.rmtree(Path(path))
            except:
                return False
        # now create directory (or leave alone if exists)
        try:
            Path(path).mkdir(exist_ok=True)
            return True
        except:
            return False

    def _clone_repo(self):
        """Clone down a github repo to a path and a reference to that directory"""
        if not self.local_path or not self.git_clone_url:
            raise GitOperationError("No local path or git clone URL defined")
        try:
            self.git_repo = Repo.clone_from(url=self.git_clone_url, to_path=self.local_path)

        except GitCommandError as e:
            if "Repository not found" in str(e):
                raise GitOperationError(f"Repository not found: {self.git_clone_url}")
            elif "Authentication failed" in str(e):
                raise GitOperationError(f"Authentication failed for repository: {self.git_clone_url}")
            elif "Permission denied" in str(e):
                raise GitOperationError(f"Permission denied for repository: {self.git_clone_url}")
            else:
                raise GitOperationError(f"Git command failed for {self.git_clone_url}: {e!s}")
        except (OSError, IOError) as e:
            raise GitOperationError(f"File system error cloning {self.git_clone_url}: {e!s}")
        except Exception as e:
            raise GitOperationError(f"Unexpected error cloning {self.git_clone_url}: {e!s}")

    def github_repo_url(self) -> Optional[str]:
        if self.github_organization and self.github_repo_name:
            return f"https://github.com/{self.github_organization}/{self.github_repo_name}"

    def get_tags(self) -> IterableList[TagReference]:
        return self.git_repo.tags

    def checkout_branch_name(self, branch_name: str, stash_changes: bool = False) -> bool:
        if stash_changes:
            try:
                self.git_repo.git.stash("--all")
            except:
                pass  # okay if we don't stash
        try:
            branch = self.git_repo.heads[branch_name]
            branch.checkout(force=stash_changes)
            assert self.git_repo.active_branch == branch
            return True
        except Exception as e:
            raise GitOperationError(f"Could not check out {branch_name}: {e}")

    def checkout_tag(self, tag: TagReference, stash_changes: bool = False) -> bool:
        if stash_changes:
            try:
                self.git_repo.git.stash("--all")
            except:
                pass  # okay if we don't stash
        try:
            self.git_repo.head.reference = tag.commit
            return True
        except Exception as e:
            raise GitOperationError(f"Could not check out {tag.name}: {e}")

    def checkout_tag_name(self, tag_name: str, stash_changes: bool = False) -> bool:
        try:
            tag = self.git_repo.tag(tag_name)
            return self.checkout_tag(tag, stash_changes)
        except Exception as e:
            raise GitOperationError(f"Could not check out {tag_name}: {e}")
