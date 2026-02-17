# Quick Start

Use this sequence when configuring a fresh environment.

## Step-by-step setup
1. **Admin → Suppliers**
   - Add all suppliers (`supplier_code`, `supplier_name`).
2. **Admin → SKUs**
   - Add supplier-specific SKUs (`part_number + supplier_id`).
3. **Admin → Pack rules**
   - Add at least one default pack rule per SKU.
4. **Admin → Lead times**
   - Define COO + mode lead days.
   - Add SKU-level overrides only when needed.
5. **Admin → Carriers**
   - Create carriers used by your rate cards.
6. **Admin → Rate cards / Rates**
   - Build lane/scope/equipment pricing and charges.
7. **Admin → Customs / HTS**
   - Maintain HTS records, tariff rates, section flags, and required docs.
8. **Admin → Demand entry**
   - Enter demand manually or import from CSV.
9. **Planner**
   - Use **Allocation**, **Recommendations**, **Shipment Builder**, and **Export** tabs.

## Recommended operating rhythm
- Update demand and urgent lead-time exceptions daily.
- Review and version rate-card effective date changes before go-live dates.
- Keep customs/tariff notes current whenever rates or section applicability change.
- Export periodic snapshots for planning review and audit trails.

## If you're stuck
- Open **Docs → FAQ/Troubleshooting** for common data validation issues.
- Use **Docs → Import Templates** to ensure CSV headers and sample values are correct.
