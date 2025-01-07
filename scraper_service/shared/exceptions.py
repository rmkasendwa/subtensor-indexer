class ShovelException(Exception):
    """Base exception for all shovel-related errors"""
    pass

class ShovelProcessingError(ShovelException):
    """Fatal error that should crash the process"""
    pass

class DatabaseConnectionError(ShovelException):
    """Retryable error for database connection issues"""
    pass
