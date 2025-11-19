from dataclasses import dataclass, field
from pathlib import Path
import re


@dataclass
class DbtPackageTextFileLine:
    line: str
    line_with_package: bool = field(init=False)
    line_with_version: bool = field(init=False)
    line_with_key: bool = field(init=False)
    modified: bool = False

    def __post_init__(self):
        self.line_with_key = "-" in self.line
        self.line_with_package = "package" in self.line
        self.line_with_version = "version" in self.line

    def replace_string_in_line(self, old_string: str, new_string: str) -> int:
        _, sub_count = re.subn(r"{original_version_string}", new_string, old_string)
        if sub_count > 0:
            self.modified = True
        return sub_count


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

    def __post_init__(self):
        self.parse_file_as_text_by_line()

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
                    if new_line.line_with_key:
                        self.lines_with_new_key.append(lines_parsed)
                        key_block.end_line = current_line - 1
                        self.key_blocks.append(key_block)
                        key_block = DbtPackageTextFileBlock(current_line)
                    if new_line.line_with_package:
                        self.lines_with_package.append(lines_parsed)
                        key_block.package_line = current_line
                    if new_line.line_with_version:
                        self.lines_with_version.append(lines_parsed)
                        key_block.version_line = current_line
                    self.lines.append(DbtPackageTextFileLine(line))
                    lines_parsed += 1
        except FileNotFoundError:
            print(f"Error: The file '{self.file_path}' was not found.")
        except Exception as e:
            print(f"An error occurred: {e}")
        return lines_parsed

    def find_package_in_file(self, package_name: str) -> list[int]:
        lines_with_package_name: list[int] = []
        for line_number in self.lines_with_package:
            if package_name in self.lines[line_number].line:
                lines_with_package_name.append(line_number)
        return lines_with_package_name

    def find_key_blocks_for_packages(self, package_names: list[str]) -> list[int]:
        # Create a set of blocks to check so we don't check ones already identiifed
        candidates: set[int] = set()
        blocks_for_packages: list[int] = [-1 * len(package_names)]
        for i, block in enumerate(self.key_blocks):
            if block.package_line > -1:
                candidates.add(i)

        # TODO: this is O(n^2) which sucks so make it better
        for i, package_name in enumerate(package_names):
            for candidate in candidates:
                candidate_package_line = self.key_blocks[candidate].package_line
                if package_name in self.lines[candidate_package_line].line:
                    blocks_for_packages[i] = candidate
                    break
            package_block = blocks_for_packages[i]
            if package_block > -1:
                candidates.remove(package_block)

        return blocks_for_packages

    def change_package_version_in_block(
        self, block_number: int, original_version_string: str, new_version_string: str
    ) -> int:
        if block_number < 0 or block_number > len(self.key_blocks):
            return -1
        block_version_line = self.key_blocks[block_number].version_line
        if block_version_line == -1:
            return -1
        result = self.lines[block_version_line].replace_string_in_line(original_version_string, new_version_string)
        if result > 0:
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
            print(f"An error occurred: {e}")
        return lines_written
