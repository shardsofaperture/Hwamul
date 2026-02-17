"""Typed models and core planning math."""
from __future__ import annotations

from dataclasses import dataclass
from math import ceil


@dataclass
class PackagingRule:
    part_number: str
    units_per_pack: float
    kg_per_unit: float
    pack_tare_kg: float
    pack_length_m: float
    pack_width_m: float
    pack_height_m: float
    min_order_packs: int = 1
    increment_packs: int = 1
    stackable: bool = True

    @property
    def pack_cube_m3(self) -> float:
        return self.pack_length_m * self.pack_width_m * self.pack_height_m

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
