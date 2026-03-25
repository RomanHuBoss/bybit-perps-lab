from __future__ import annotations

import argparse
import json
from pathlib import Path

from services.research_runner import AutoResearchRunner, get_research_presets


def load_plan(plan_path: str | None, preset_name: str | None) -> tuple[dict, str]:
    if plan_path:
        path = Path(plan_path)
        return json.loads(path.read_text(encoding='utf-8')), path.stem
    presets = {preset['name']: preset for preset in get_research_presets()}
    name = preset_name or 'reversion_60d_auto'
    if name not in presets:
        raise SystemExit(f'Unknown preset: {name}. Available: {", ".join(sorted(presets))}')
    return presets[name], name


def main() -> None:
    parser = argparse.ArgumentParser(description='Run automated optimizer research plan.')
    parser.add_argument('--symbol', required=True, help='Single symbol, e.g. SOLUSDT')
    parser.add_argument('--plan', help='Path to JSON research plan')
    parser.add_argument('--preset', help='Built-in preset name')
    parser.add_argument('--note', help='Optional note added to manifest/report')
    args = parser.parse_args()

    plan, plan_name = load_plan(args.plan, args.preset)
    manifest = AutoResearchRunner().run(symbols=[args.symbol], plan=plan, plan_name=plan_name, note=args.note)
    print(json.dumps({
        'research_run_id': manifest['research_run_id'],
        'plan_name': manifest['plan_name'],
        'winner': manifest.get('winner'),
        'paper_config': manifest.get('paper_config'),
    }, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
