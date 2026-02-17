import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from acceptance_pipeline import run_acceptance_pipeline


def main() -> None:
    result = run_acceptance_pipeline()

    assert result["phase_cost_summary"], "Missing phase cost summary"
    assert result["shipment_plan"], "Missing shipment plan"
    assert result["itemized_rate_breakdown"], "Missing itemized rate breakdown"
    assert result["customs_report"], "Missing customs report"

    print("ACCEPTANCE TEST PASSED")
    print(f"Overall total USD: {result['overall_total_usd']}")


if __name__ == "__main__":
    main()
