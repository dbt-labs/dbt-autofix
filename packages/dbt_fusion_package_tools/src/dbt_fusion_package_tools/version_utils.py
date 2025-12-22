import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, List, Optional, Union

from mashumaro import DataClassDictMixin

from dbt_fusion_package_tools.exceptions import SemverError, VersionsNotCompatibleError


class Matchers(str, Enum):
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL = "<="
    EXACT = "="


@dataclass
class VersionSpecification(DataClassDictMixin):
    major: Optional[str] = None
    minor: Optional[str] = None
    patch: Optional[str] = None
    prerelease: Optional[str] = None
    build: Optional[str] = None
    matcher: Matchers = Matchers.EXACT


_MATCHERS = r"(?P<matcher>\>=|\>|\<|\<=|=)?"
_NUM_NO_LEADING_ZEROS = r"(0|[1-9]\d*)"
_ALPHA = r"[0-9A-Za-z-]*"
_ALPHA_NO_LEADING_ZEROS = r"(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"

_BASE_VERSION_REGEX = r"""
(?P<major>{num_no_leading_zeros})\.
(?P<minor>{num_no_leading_zeros})\.
(?P<patch>{num_no_leading_zeros})
""".format(num_no_leading_zeros=_NUM_NO_LEADING_ZEROS)

_VERSION_EXTRA_REGEX = r"""
(\-?
  (?P<prerelease>
    {alpha_no_leading_zeros}(\.{alpha_no_leading_zeros})*))?
(\+
  (?P<build>
    {alpha}(\.{alpha})*))?
""".format(alpha_no_leading_zeros=_ALPHA_NO_LEADING_ZEROS, alpha=_ALPHA)


_VERSION_REGEX_PAT_STR = r"""
^
{matchers}
{base_version_regex}
{version_extra_regex}
$
""".format(
    matchers=_MATCHERS,
    base_version_regex=_BASE_VERSION_REGEX,
    version_extra_regex=_VERSION_EXTRA_REGEX,
)

_VERSION_REGEX = re.compile(_VERSION_REGEX_PAT_STR, re.VERBOSE)


def _cmp(a: Any, b: Any) -> int:
    """Return negative if a<b, zero if a==b, positive if a>b."""
    return int((a > b) - (a < b))


@dataclass
class VersionSpecifier(VersionSpecification):
    def to_version_string(self, skip_matcher: bool = False) -> str:
        prerelease = ""
        build = ""
        matcher = ""

        if self.prerelease:
            prerelease = "-" + self.prerelease

        if self.build:
            build = "+" + self.build

        if not skip_matcher:
            matcher = self.matcher.value
        return "{}{}.{}.{}{}{}".format(matcher, self.major, self.minor, self.patch, prerelease, build)

    @classmethod
    def from_version_string(cls, version_string: str) -> "VersionSpecifier":
        match = _VERSION_REGEX.match(version_string)

        if not match:
            raise SemverError(f'"{version_string}" is not a valid semantic version.')

        matched = {k: str(v) for k, v in match.groupdict().items() if v is not None}
        spec = cls.from_dict(matched)
        assert isinstance(spec, VersionSpecifier)
        return spec

    def __str__(self) -> str:
        return self.to_version_string()

    def to_range(self) -> "VersionRange":
        range_start: VersionSpecifier = UnboundedVersionSpecifier()
        range_end: VersionSpecifier = UnboundedVersionSpecifier()

        if self.matcher == Matchers.EXACT:
            range_start = self
            range_end = self

        elif self.matcher in [Matchers.GREATER_THAN, Matchers.GREATER_THAN_OR_EQUAL]:
            range_start = self

        elif self.matcher in [Matchers.LESS_THAN, Matchers.LESS_THAN_OR_EQUAL]:
            range_end = self

        return VersionRange(start=range_start, end=range_end)

    def compare(self, other: "VersionSpecifier") -> int:
        if self.is_unbounded or other.is_unbounded:
            return 0

        for key in ["major", "minor", "patch", "prerelease"]:
            (a, b) = (getattr(self, key), getattr(other, key))
            if key == "prerelease":
                if a is None and b is None:
                    continue
                if a is None:
                    if self.matcher == Matchers.LESS_THAN:
                        # If 'a' is not a pre-release but 'b' is, and b must be
                        # less than a, return -1 to prevent installations of
                        # pre-releases with greater base version than a
                        # maximum specified non-pre-release version.
                        return -1
                    # Otherwise, stable releases are considered greater than
                    # pre-release
                    return 1
                if b is None:
                    return -1

                # Check the prerelease component only
                prcmp = self._nat_cmp(a, b)
                if prcmp != 0:  # either -1 or 1
                    return prcmp
                # else is equal and will fall through

            elif int(a) > int(b):
                return 1
            elif int(a) < int(b):
                return -1
                # else is equal and will fall through

        equal = (self.matcher == Matchers.GREATER_THAN_OR_EQUAL and other.matcher == Matchers.LESS_THAN_OR_EQUAL) or (
            self.matcher == Matchers.LESS_THAN_OR_EQUAL and other.matcher == Matchers.GREATER_THAN_OR_EQUAL
        )
        if equal:
            return 0

        lt = (
            (self.matcher == Matchers.LESS_THAN and other.matcher == Matchers.LESS_THAN_OR_EQUAL)
            or (other.matcher == Matchers.GREATER_THAN and self.matcher == Matchers.GREATER_THAN_OR_EQUAL)
            or (self.is_upper_bound and other.is_lower_bound)
        )
        if lt:
            return -1

        gt = (
            (other.matcher == Matchers.LESS_THAN and self.matcher == Matchers.LESS_THAN_OR_EQUAL)
            or (self.matcher == Matchers.GREATER_THAN and other.matcher == Matchers.GREATER_THAN_OR_EQUAL)
            or (self.is_lower_bound and other.is_upper_bound)
        )
        if gt:
            return 1

        return 0

    def __lt__(self, other: "VersionSpecifier") -> bool:
        return self.compare(other) == -1

    def __gt__(self, other: "VersionSpecifier") -> bool:
        return self.compare(other) == 1

    def __eq__(self, other: object) -> bool:
        assert isinstance(other, VersionSpecifier)
        return self.compare(other) == 0

    def __cmp__(self, other: "VersionSpecifier") -> int:
        return self.compare(other)

    @property
    def is_unbounded(self) -> bool:
        return False

    @property
    def is_lower_bound(self) -> bool:
        return self.matcher in [Matchers.GREATER_THAN, Matchers.GREATER_THAN_OR_EQUAL]

    @property
    def is_upper_bound(self) -> bool:
        return self.matcher in [Matchers.LESS_THAN, Matchers.LESS_THAN_OR_EQUAL]

    @property
    def is_exact(self) -> bool:
        return self.matcher == Matchers.EXACT

    @classmethod
    def _nat_cmp(cls, a: str, b: str) -> int:
        def cmp_prerelease_tag(a: Union[str, int], b: Union[str, int]) -> int:
            if isinstance(a, int) and isinstance(b, int):
                return _cmp(a, b)
            elif isinstance(a, int):
                return -1
            elif isinstance(b, int):
                return 1
            else:
                return _cmp(a, b)

        a, b = a or "", b or ""
        a_parts, b_parts = a.split("."), b.split(".")
        a_parts_2 = [int(x) if re.match(r"^\d+$", x) else x for x in a_parts]
        b_parts_2 = [int(x) if re.match(r"^\d+$", x) else x for x in b_parts]
        for sub_a, sub_b in zip(a_parts_2, b_parts_2):
            cmp_result = cmp_prerelease_tag(sub_a, sub_b)  # type: ignore
            if cmp_result != 0:
                return cmp_result
        return _cmp(len(a), len(b))


@dataclass
class VersionRange:
    start: VersionSpecifier
    end: VersionSpecifier

    def _try_combine_exact(self, a: VersionSpecifier, b: VersionSpecifier) -> VersionSpecifier:
        if a.compare(b) == 0:
            return a
        else:
            raise VersionsNotCompatibleError()

    def _try_combine_lower_bound_with_exact(self, lower: VersionSpecifier, exact: VersionSpecifier) -> VersionSpecifier:
        comparison = lower.compare(exact)

        if comparison < 0 or (comparison == 0 and lower.matcher == Matchers.GREATER_THAN_OR_EQUAL):
            return exact

        raise VersionsNotCompatibleError()

    def _try_combine_lower_bound(self, a: VersionSpecifier, b: VersionSpecifier) -> VersionSpecifier:
        if b.is_unbounded:
            return a
        elif a.is_unbounded:
            return b

        if not (a.is_exact or b.is_exact):
            comparison = a.compare(b) < 0

            if comparison:
                return b
            else:
                return a

        elif a.is_exact:
            return self._try_combine_lower_bound_with_exact(b, a)

        else:
            return self._try_combine_lower_bound_with_exact(a, b)

    def _try_combine_upper_bound_with_exact(self, upper: VersionSpecifier, exact: VersionSpecifier) -> VersionSpecifier:
        comparison = upper.compare(exact)

        if comparison > 0 or (comparison == 0 and upper.matcher == Matchers.LESS_THAN_OR_EQUAL):
            return exact

        raise VersionsNotCompatibleError()

    def _try_combine_upper_bound(self, a: VersionSpecifier, b: VersionSpecifier) -> VersionSpecifier:
        if b.is_unbounded:
            return a
        elif a.is_unbounded:
            return b

        if not (a.is_exact or b.is_exact):
            comparison = a.compare(b) > 0

            if comparison:
                return b
            else:
                return a

        elif a.is_exact:
            return self._try_combine_upper_bound_with_exact(b, a)

        else:
            return self._try_combine_upper_bound_with_exact(a, b)

    def reduce(self, other: "VersionRange") -> "VersionRange":
        start = None

        if self.start.is_exact and other.start.is_exact:
            start = end = self._try_combine_exact(self.start, other.start)
        else:
            start = self._try_combine_lower_bound(self.start, other.start)
            end = self._try_combine_upper_bound(self.end, other.end)

        if start.compare(end) > 0:
            raise VersionsNotCompatibleError()

        return VersionRange(start=start, end=end)

    def __str__(self) -> str:
        result = []

        if self.start.is_unbounded and self.end.is_unbounded:
            return "ANY"

        if not self.start.is_unbounded:
            result.append(self.start.to_version_string())

        if not self.end.is_unbounded:
            result.append(self.end.to_version_string())

        return ", ".join(result)

    def to_version_string_pair(self) -> List[str]:
        to_return = []

        if not self.start.is_unbounded:
            to_return.append(self.start.to_version_string())

        if not self.end.is_unbounded:
            to_return.append(self.end.to_version_string())

        return to_return


class UnboundedVersionSpecifier(VersionSpecifier):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(matcher=Matchers.EXACT, major=None, minor=None, patch=None, prerelease=None, build=None)

    def __str__(self) -> str:
        return "*"

    @property
    def is_unbounded(self) -> bool:
        return True

    @property
    def is_lower_bound(self) -> bool:
        return False

    @property
    def is_upper_bound(self) -> bool:
        return False

    @property
    def is_exact(self) -> bool:
        return False


def reduce_versions(*args: Union[VersionSpecifier, VersionRange, str]) -> VersionRange:
    version_specifiers = []

    for version in args:
        if isinstance(version, UnboundedVersionSpecifier) or version is None:
            continue

        elif isinstance(version, VersionSpecifier):
            version_specifiers.append(version)

        elif isinstance(version, VersionRange):
            if not isinstance(version.start, UnboundedVersionSpecifier):
                version_specifiers.append(version.start)

            if not isinstance(version.end, UnboundedVersionSpecifier):
                version_specifiers.append(version.end)

        else:
            version_specifiers.append(VersionSpecifier.from_version_string(version))

    for version_specifier in version_specifiers:
        if not isinstance(version_specifier, VersionSpecifier):
            raise Exception(version_specifier)

    if not version_specifiers:
        return VersionRange(start=UnboundedVersionSpecifier(), end=UnboundedVersionSpecifier())

    try:
        to_return = version_specifiers.pop().to_range()

        for version_specifier in version_specifiers:
            to_return = to_return.reduce(version_specifier.to_range())
    except VersionsNotCompatibleError:
        raise VersionsNotCompatibleError(
            "Could not find a satisfactory version from options: {}".format([str(a) for a in args])
        )

    return to_return


def versions_compatible(*args: Union[VersionSpecifier, VersionRange, str]) -> bool:
    if len(args) == 1:
        return True

    try:
        reduce_versions(*args)
        return True
    except VersionsNotCompatibleError:
        return False


def find_possible_versions(requested_range: VersionRange, available_versions: Iterable[str]) -> List[str]:
    possible_versions = []

    for version_string in available_versions:
        version = VersionSpecifier.from_version_string(version_string)

        if versions_compatible(version, requested_range.start, requested_range.end):
            possible_versions.append(version)

    sorted_versions = sorted(possible_versions, reverse=True)
    return [v.to_version_string(skip_matcher=True) for v in sorted_versions]


def resolve_to_specific_version(requested_range: VersionRange, available_versions: Iterable[str]) -> Optional[str]:
    max_version = None
    max_version_string = None

    for version_string in available_versions:
        version = VersionSpecifier.from_version_string(version_string)

        if versions_compatible(version, requested_range.start, requested_range.end) and (
            max_version is None or max_version.compare(version) < 0
        ):
            max_version = version
            max_version_string = version_string

    return max_version_string


def filter_installable(versions: List[str], install_prerelease: bool) -> List[str]:
    installable = []
    installable_dict = {}
    for version_string in versions:
        version = VersionSpecifier.from_version_string(version_string)
        if install_prerelease or not version.prerelease:
            installable.append(version)
            installable_dict[str(version)] = version_string
    sorted_installable = sorted(installable)
    sorted_installable_original_versions = [str(installable_dict.get(str(version))) for version in sorted_installable]
    return sorted_installable_original_versions


FUSION_COMPATIBLE_VERSION: VersionSpecifier = VersionSpecifier.from_version_string("2.0.0")
# `float` also allows `int`, according to PEP484 (and jsonschema!)
RawVersion = Union[str, float]


def get_versions(version: Union[RawVersion, list[RawVersion]]) -> list[str]:
    if isinstance(version, list):
        return [str(v) for v in version]
    else:
        return [str(version)]


def construct_version_list(raw_versions: Union[str, list[str], None]) -> list[str]:
    if raw_versions is None:
        return []
    elif type(raw_versions) == str:
        return [raw_versions]
    elif type(raw_versions) == list:
        return raw_versions
    else:
        return []


def construct_version_list_from_raw(raw_versions: Any) -> list[str]:
    if raw_versions is None:
        return []
    # isinstance is needed here because when ruyaml parses the YAML,
    # sometimes it stores it as an instance of a class that extend str or list
    elif isinstance(raw_versions, str):
        return [str(raw_versions)]
    elif isinstance(raw_versions, list):
        versions = []
        for version in raw_versions:
            if isinstance(version, str) or isinstance(version, float):
                versions.append(str(version))
        return versions
    else:
        return []


def get_version_specifiers(raw_version: list[str]) -> list[VersionSpecifier]:
    return [VersionSpecifier.from_version_string(v) for v in raw_version]


def convert_version_specifiers_to_range(specs: list[VersionSpecifier]) -> VersionRange:
    if len(specs) == 0 or len(specs) > 2:
        # assume any version compatible
        any_version = VersionSpecifier.from_version_string(">=0.0.0")
        return VersionRange(any_version, any_version)
    elif len(specs) == 1:
        return VersionRange(specs[0], specs[0])
    elif specs[0] < specs[1]:
        return VersionRange(specs[0], specs[1])
    else:
        return VersionRange(specs[1], specs[0])


def convert_optional_version_string_to_spec(version_string: Optional[str]) -> Optional[VersionSpecifier]:
    try:
        if type(version_string) == str:
            return VersionSpecifier.from_version_string(version_string)
        else:
            return None
    except:
        return None


def convert_version_string_list_to_spec(version_string: list[str]) -> list[VersionSpecifier]:
    if len(version_string) == 0:
        return []
    else:
        return [VersionSpecifier.from_version_string(x) for x in version_string]
