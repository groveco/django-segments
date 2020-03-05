class SegmentException(Exception):
    """
    Base Segment Exception
    """
    pass


class SegmentExecutionError(SegmentException):
    """
    Any SQL issues encountered when Segments executing their SQL definitions will raise this exception.
    """
    pass


class SegmentDefinitionUnescaped(SegmentException):
    """
    Raised when an unescaped percent sign is encountered
    """
    pass
