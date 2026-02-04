from enum import Enum


class DeprecationType(str, Enum):
    """String enum for deprecation types used in DbtDeprecationRefactor."""

    UNEXPECTED_JINJA_BLOCK_DEPRECATION = "UnexpectedJinjaBlockDeprecation"
    RESOURCE_NAMES_WITH_SPACES_DEPRECATION = "ResourceNamesWithSpacesDeprecation"
    PROPERTY_MOVED_TO_CONFIG_DEPRECATION = "PropertyMovedToConfigDeprecation"
    CUSTOM_KEY_IN_OBJECT_DEPRECATION = "CustomKeyInObjectDeprecation"
    MISSING_GENERIC_TEST_ARGUMENTS_PROPERTY_DEPRECATION = "MissingGenericTestArgumentsPropertyDeprecation"
    DUPLICATE_YAML_KEYS_DEPRECATION = "DuplicateYAMLKeysDeprecation"
    EXPOSURE_NAME_DEPRECATION = "ExposureNameDeprecation"
    CONFIG_LOG_PATH_DEPRECATION = "ConfigLogPathDeprecation"
    CONFIG_TARGET_PATH_DEPRECATION = "ConfigTargetPathDeprecation"
    CONFIG_DATA_PATH_DEPRECATION = "ConfigDataPathDeprecation"
    CONFIG_SOURCE_PATH_DEPRECATION = "ConfigSourcePathDeprecation"
    MISSING_PLUS_PREFIX_DEPRECATION = "MissingPlusPrefixDeprecation"
    CUSTOM_TOP_LEVEL_KEY_DEPRECATION = "CustomTopLevelKeyDeprecation"
    CUSTOM_KEY_IN_CONFIG_DEPRECATION = "CustomKeyInConfigDeprecation"
