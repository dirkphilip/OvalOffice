class LASIFError(Exception):
    """
    Base exception class for LASIF.
    """
    pass

class LASIFAdjointSourceCalculationError(LASIFError):

    """
    Raised when something goes wrong when calculating an adjoint source.
    """
    pass
