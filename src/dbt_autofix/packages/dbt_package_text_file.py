from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Any, Optional
from rich.console import Console

from dbt_fusion_package_tools.fusion_version_compatibility_output import FUSION_VERSION_COMPATIBILITY_OUTPUT

console = Console()
error_console = Console(stderr=True)

VERSION_PREFIX = re.compile(r"^\s*(?:-\s*)?version:\s*")
PACKAGE_PREFIX = re.compile(r"^\s*(?:-\s*)?package:\s*")
KEY_PATTERN = re.compile(r"^\s*-")
PACKAGE_PATTERN = re.compile(r"^\s*(?:-\s*)?package:")
VERSION_PATTERN = re.compile(r"^\s*(?:-\s*)?version:")
VERSION_MATCH_STRING = re.compile(r"\s*(?P<version>[^\s#\r\n]+)")
VERSION_MATCH_LIST = re.compile(r"(?P<version>\[[^\]]*\])")
PACKAGE_MATCH = re.compile(r"\s*(?P<pkg>[^\s#\r\n]+)")


@dataclass
class DbtPackageTextFileLine:
    line: str
    modified: bool = False

    def extract_version_from_line(self) -> list[str]:
        """Extracts a version string while retaining the key and line ending.

        If a line contains a version string, this function will always return a list
        of length 3 where the extracted version is the second entry in the list.
        This makes it easy to replace a version only while not altering the rest of the line
        (which retains any inline comments and original line endings).

        Example:
            The line " - version: 0.1.1 # inline comment" is deconstructed
            into three parts: [" - version: ", "0.1.1", " # inline comment"].

        Returns:
            list[str]: the deconstructed line or [] if no package name found
        Returns:
            list[str]: [beginning of line, version, end of line] or [] if no package name found
        """
        if not self.line_contains_version():
            return []
        m = VERSION_PREFIX.match(self.line)
        if not m:
            return []

        rest = self.line[m.end() :]
        if len(rest) == 0:
            return []
        if rest[0] == "[":  # version is a list
            version_match = VERSION_MATCH_LIST.match(rest)
        # Extract version up to first whitespace, '#' or line ending
        else:
            version_match = VERSION_MATCH_STRING.match(rest)
        if not version_match:
            return []
        version = version_match.group("version")
        eol = rest[version_match.end("version") :]
        return [self.line[: m.end()], version, eol]

    def extract_package_from_line(self) -> list[str]:
        """Extracts a package string while retaining the key and line ending.

        If a line contains a package name, this function will always return a list
        of length 3 where the extracted package name is the second entry in the list.
        This makes it easy to replace a package name only while not altering the rest of the line
        (which retains any inline comments and original line endings).

        Example:
            The line " - package: dbt-labs/dbt-utils # inline comment" is deconstructed
            into three parts: [" - package: ", "dbt-labs/dbt-utils", " # inline comment"].

        Returns:
            list[str]: the deconstructed line or [] if no package name found
        """
        if not self.line_contains_package():
            return []
        m = PACKAGE_PREFIX.match(self.line)
        if not m:
            return []

        rest = self.line[m.end() :]
        # Extract package id up to first whitespace, '#' or line ending
        pkg_match = PACKAGE_MATCH.match(rest)
        if not pkg_match:
            return []
        pkg = pkg_match.group("pkg")
        if pkg is not None:
            pkg = pkg.strip('"')
            pkg = pkg.strip("'")
        eol = rest[pkg_match.end("pkg") :]
        return [self.line[: m.end()], pkg, eol]

    def extract_package_name_from_line(self) -> str:
        """Extract the package name from a line containing a `package:` key.

        Returns:
            str: package ID
        """
        if not self.line_contains_package():
            return ""
        extracted_line: list[str] = self.extract_package_from_line()
        if len(extracted_line) < 3:
            return ""
        else:
            return extracted_line[1]

    def replace_package_name_in_line(self, new_string: str) -> bool:
        if not self.line_contains_package():
            return False
        extracted_version = self.extract_package_from_line()
        if len(extracted_version) != 3:
            return False
        self.line = f"{extracted_version[0]}{new_string}{extracted_version[2]}"
        self.modified = True
        return True

    def replace_version_string_in_line(self, new_string: str) -> bool:
        if not self.line_contains_version():
            return False
        extracted_version = self.extract_version_from_line()
        if len(extracted_version) != 3:
            return False
        self.line = f"{extracted_version[0]}{new_string}{extracted_version[2]}"
        self.modified = True
        return True

    def line_contains_key(self) -> bool:
        return bool(KEY_PATTERN.match(self.line))

    def line_contains_package(self) -> bool:
        return bool(PACKAGE_PATTERN.match(self.line))

    def line_contains_version(self) -> bool:
        return bool(VERSION_PATTERN.match(self.line))


@dataclass
class DbtPackageTextFileBlock:
    start_line: int
    end_line: int = -1
    package_line: int = -1
    version_line: int = -1


@dataclass
class DbtPackageTextFile:
    file_path: Path
    lines: list[DbtPackageTextFileLine] = field(init=False, default_factory=list)
    lines_with_package: list[int] = field(init=False, default_factory=list)
    lines_with_version: list[int] = field(init=False, default_factory=list)
    lines_with_new_key: list[int] = field(init=False, default_factory=list)
    key_blocks: list[DbtPackageTextFileBlock] = field(init=False, default_factory=list)
    key_blocks_by_start: dict[int, int] = field(init=False, default_factory=dict)
    key_blocks_by_end: dict[int, int] = field(init=False, default_factory=dict)
    lines_modified: set[int] = field(init=False, default_factory=set)
    packages_by_line: dict[str, int] = field(init=False, default_factory=dict)
    packages_by_block: dict[str, int] = field(init=False, default_factory=dict)
    blocks_by_line: list[int] = field(init=False, default_factory=list)

    def __post_init__(self):
        self.parse_file_as_text_by_line()
        self.packages_by_line = {}
        self.packages_by_block = {}
        self.extract_packages_from_lines()

    def parse_file_as_text_by_line(self) -> int:
        current_line: int = -1
        self.lines = []
        key_block = DbtPackageTextFileBlock(0)
        try:
            with open(self.file_path, "r") as file:
                for line in file:
                    current_line += 1
                    new_line = DbtPackageTextFileLine(line)
                    # if line contains "-", start a new block
                    if new_line.line_contains_key():
                        self.lines_with_new_key.append(current_line)
                        key_block.end_line = current_line - 1
                        self.key_blocks.append(key_block)
                        key_block = DbtPackageTextFileBlock(current_line)
                    if new_line.line_contains_package():
                        self.lines_with_package.append(current_line)
                        key_block.package_line = current_line
                    elif new_line.line_contains_version():
                        self.lines_with_version.append(current_line)
                        key_block.version_line = current_line
                    self.blocks_by_line.append(len(self.key_blocks))
                    self.lines.append(DbtPackageTextFileLine(line))
                if key_block.end_line == -1:
                    key_block.end_line = current_line - 1
                    self.key_blocks.append(key_block)
        except FileNotFoundError:
            error_console.print(f"Error: The file '{self.file_path}' was not found.")
        except Exception as e:
            error_console.print(f"An error occurred: {e}")
        return current_line + 1

    def extract_packages_from_lines(self):
        for i, line in enumerate(self.lines):
            if line.line_contains_package():
                package_name = line.extract_package_name_from_line()
                self.packages_by_line[package_name] = i
                self.packages_by_block[package_name] = self.blocks_by_line[i]

    def find_package_in_file(self, package_name: str) -> list[int]:
        lines_with_package_name: list[int] = []
        for line_number in self.lines_with_package:
            if package_name in self.lines[line_number].line:
                lines_with_package_name.append(line_number)
        return lines_with_package_name

    def find_key_blocks_for_packages(self, package_names: list[str]) -> list[int]:
        blocks_for_packages: list[int] = []
        for package in package_names:
            package_block: int = self.packages_by_block.get(package, -1)
            blocks_for_packages.append(package_block)
        return blocks_for_packages

    def change_package_version_in_block(self, block_number: int, new_version_string: str) -> int:
        if block_number < 0 or block_number > len(self.key_blocks):
            return -1
        block_version_line = self.key_blocks[block_number].version_line
        if block_version_line == -1:
            return -1
        result: bool = self.lines[block_version_line].replace_version_string_in_line(new_version_string)
        if result:
            self.lines_modified.add(block_version_line)
            return block_version_line
        else:
            return -1

    def write_output_to_file(self) -> int:
        lines_written: int = 0
        try:
            with open(self.file_path, "w") as file:
                for file_line in self.lines:
                    file.write(file_line.line)
                    lines_written += 1
        except Exception as e:
            error_console.print(f"An error occurred: {e}")
        return lines_written

    def update_package_name_if_redirect(self, block_number: int, current_name: str) -> bool:
        """Replace a package name if that package has been renamed according to Package Hub.

        Args:
            block_number (int): the block in the parsed file that contains the package
            current_name (str): the package name currently specified in the config

        Returns:
            bool: True if the package name has been updated, otherwise False
        """
        updated_name: Optional[str] = (FUSION_VERSION_COMPATIBILITY_OUTPUT.get(current_name, {})).get(
            "package_redirect_id"
        )
        if updated_name is None:
            return False

        if block_number < 0 or block_number > len(self.key_blocks):
            return False
        block_package_line = self.key_blocks[block_number].package_line
        if block_package_line == -1:
            return False
        result: bool = self.lines[block_package_line].replace_package_name_in_line(updated_name)
        if result:
            self.lines_modified.add(block_package_line)
            return True
        else:
            return False

    def update_config_file(
        self, packages_with_versions: dict[str, str], dry_run: bool = False, print_to_console: bool = True
    ) -> set[str]:
        if len(packages_with_versions) == 0:
            return set()

        packages_to_update: list[str] = [x for x in packages_with_versions]
        updated_packages: set[str] = set()
        unchanged_packages: set[str] = set()
        key_blocks: list[int] = self.find_key_blocks_for_packages(packages_to_update)
        for i, block in enumerate(key_blocks):
            package_name = packages_to_update[i]
            package_version = packages_with_versions[package_name]
            if block == -1:
                unchanged_packages.add(package_name)
                continue

            block_version_line = self.change_package_version_in_block(block, package_version)
            if block_version_line > -1 and block_version_line < len(self.lines):
                # only update package name if version has changed
                self.update_package_name_if_redirect(block, package_name)
                updated_packages.add(package_name)
            else:
                unchanged_packages.add(package_name)
        if len(updated_packages) == 0:
            return updated_packages
        if dry_run and print_to_console:
            console.print(
                f"\nDRY RUN - CHANGES NOT APPLIED TO {self.file_path.name}",
                style="green",
            )
            for line in self.lines:
                if line.modified:
                    console.print(line.line, style="green")
                else:
                    console.print(line.line)
        else:
            lines_written = self.write_output_to_file()
            if lines_written == 0 and print_to_console:
                console.print(f"Error: No output written to {self.file_path.name}")
        return updated_packages
