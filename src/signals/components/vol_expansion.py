"""Compatibility shim for moved independent signal module."""
from src.signals.independent.vol_expansion import VolExpansionSignal as VolExpansionComponent

__all__ = ["VolExpansionComponent"]
