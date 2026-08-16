class ExecutionError(RuntimeError):
    """A claimed Source, Stage, or Delivery attempt failed."""


class DefinitionError(ValueError):
    """The YAML definition is malformed or violates authoring rules."""


class GraphError(DefinitionError):
    """The Pipeline node graph is malformed or contains an invalid edge."""
