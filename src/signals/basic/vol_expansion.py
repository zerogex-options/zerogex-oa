"""Compatibility shim for moved advanced signal module."""
from src.signals.advanced.vol_expansion import VolExpansionSignal as VolExpansionComponent

__all__ = ["VolExpansionComponent"]
