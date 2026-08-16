class ExecutionError(RuntimeError):
    """A claimed Source, Stage, or Delivery attempt failed."""


class InvalidStructuredOutput(ExecutionError):
    """An LLM response did not match the configured structured shape."""


class DefinitionError(ValueError):
    """The YAML definition is malformed or violates authoring rules."""


class GraphError(DefinitionError):
    """The Pipeline node graph is malformed or contains an invalid edge."""
