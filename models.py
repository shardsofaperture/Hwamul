"""Typed models and core planning math."""
from __future__ import annotations

from dataclasses import dataclass
from math import ceil


@dataclass
class PackagingRule:
    units_per_pack: float
    kg_per_unit: float
    pack_tare_kg: float
    dim_l_m: float
    dim_w_m: float
    dim_h_m: float
    min_order_packs: int = 1
    increment_packs: int = 1
    stackable: bool = True
    max_stack: int | None = None
    part_number: str = ""

    @staticmethod
    def _to_meters(dimension: float) -> float:
        """Normalize input dimension to meters.

        Legacy data uses meters while new import templates use centimeters.
        We treat values above 3 as centimeters to support pallet/crate inputs.
        """
        return dimension / 100.0 if dimension > 3 else dimension

    @property
    def dim_l_norm_m(self) -> float:
        return self._to_meters(self.dim_l_m)

    @property
    def dim_w_norm_m(self) -> float:
        return self._to_meters(self.dim_w_m)

    @property
    def dim_h_norm_m(self) -> float:
        return self._to_meters(self.dim_h_m)

    @property
    def pack_cube_m3(self) -> float:
        return self.dim_l_norm_m * self.dim_w_norm_m * self.dim_h_norm_m

    @property
    def gross_pack_weight_kg(self) -> float:
        return self.units_per_pack * self.kg_per_unit + self.pack_tare_kg


@dataclass
class Equipment:
    name: str
    mode: str
    length_m: float
    width_m: float
    height_m: float
    max_payload_kg: float
    volumetric_factor: float | None = None

    @property
    def volume_m3(self) -> float:
        return self.length_m * self.width_m * self.height_m


def rounded_order_packs(required_units: float, rule: PackagingRule) -> int:
    """Convert requested units to packs using MOQ and increment rules.

    Algorithm:
    1) Convert units to raw packs.
    2) Round up to whole packs.
    3) Enforce MOQ.
    4) Round up again to increment multiple.
    """
    if rule.units_per_pack <= 0:
        raise ValueError("units_per_pack must be greater than 0")
    raw_packs = required_units / rule.units_per_pack
    packs = max(ceil(raw_packs), rule.min_order_packs)
    inc = max(1, rule.increment_packs)
    return ceil(packs / inc) * inc



def chargeable_air_weight_kg(actual_weight_kg: float, volume_m3: float, volumetric_factor: float) -> float:
    """Chargeable air weight is max(actual gross kg, volumetric kg)."""
    return max(actual_weight_kg, volume_m3 * volumetric_factor)



def estimate_equipment_count(total_volume_m3: float, total_weight_kg: float, equipment: Equipment) -> int:
    """Estimate count needed using the tighter of cube and payload constraints.

    We compute count by volume and by weight separately and choose the max.
    """
    if total_volume_m3 <= 0 and total_weight_kg <= 0:
        return 0
    by_volume = ceil(total_volume_m3 / equipment.volume_m3) if equipment.volume_m3 else 0
    by_weight = ceil(total_weight_kg / equipment.max_payload_kg) if equipment.max_payload_kg else 0
    return max(1, by_volume, by_weight)
