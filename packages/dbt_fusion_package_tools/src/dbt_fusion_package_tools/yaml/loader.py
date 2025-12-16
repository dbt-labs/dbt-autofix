from yaml.reader import Reader
from yaml.scanner import Scanner
from yaml.parser import Parser
from yaml.composer import Composer
from yaml.constructor import SafeConstructor
from yaml.resolver import Resolver


class SafeConstructorWithOutput(SafeConstructor):
    def get_single_data(self):
        # Ensure that the stream contains a single document and construct it.
        node = self.get_single_node()
        if node is not None:
            document = self.construct_document(node)
            return document
        return None

    def get_single_data_and_document(self):
        node = self.get_single_node()
        if node is not None:
            return (node, self.construct_document(node))
        return None


class SafeLoaderWithOutput(Reader, Scanner, Parser, Composer, SafeConstructorWithOutput, Resolver):
    def __init__(self, stream):
        Reader.__init__(self, stream)
        Scanner.__init__(self)
        Parser.__init__(self)
        Composer.__init__(self)
        SafeConstructor.__init__(self)
        Resolver.__init__(self)


def load(stream, Loader):
    """
    Parse the first YAML document in a stream
    and produce the corresponding Python object.
    """
    loader = Loader(stream)

    try:
        return loader.get_single_data_and_document()
    finally:
        loader.dispose()


def safe_load(stream):
    return load(stream, SafeLoaderWithOutput)
