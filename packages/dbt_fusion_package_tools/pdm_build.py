from pdm.backend.hooks.version import SCMVersion


def format_version(version: SCMVersion) -> str:
    if version.distance is None:
        return str(version.version)
    else:
        return f"{version.version}.post{version.distance}"
