from dbt_autofix.deprecations import DeprecationType
def main() -> None:
    print("Hello from dbt-fusion-package-tools!")
    print(DeprecationType.CONFIG_DATA_PATH_DEPRECATION)
