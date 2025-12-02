from dbt_autofix.deprecations import DeprecationType
def main() -> None:
    print("Hello from dbt-fusion-package-tools!")
    print(DeprecationType.UNEXPECTED_JINJA_BLOCK_DEPRECATION)
