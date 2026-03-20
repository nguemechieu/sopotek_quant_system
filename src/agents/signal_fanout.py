import asyncio


def _merge_assignment_rows(existing_rows, new_rows):
    merged_rows = [dict(row) for row in list(existing_rows or []) if isinstance(row, dict)]
    fingerprints = {
        (
            str(row.get("strategy_name") or "").strip(),
            str(row.get("timeframe") or "").strip(),
        )
        for row in merged_rows
    }
    for row in list(new_rows or []):
        if not isinstance(row, dict):
            continue
        fingerprint = (
            str(row.get("strategy_name") or "").strip(),
            str(row.get("timeframe") or "").strip(),
        )
        if fingerprint in fingerprints:
            continue
        merged_rows.append(dict(row))
        fingerprints.add(fingerprint)
    return merged_rows


def merge_signal_agent_results(context, results):
    working = dict(context or {})
    merged_assignments = _merge_assignment_rows(working.get("assigned_strategies") or [], [])
    merged_candidates = [dict(candidate) for candidate in list(working.get("signal_candidates") or []) if isinstance(candidate, dict)]
    blocked_reasons = []

    for result in list(results or []):
        if not isinstance(result, dict):
            continue
        merged_assignments = _merge_assignment_rows(merged_assignments, result.get("assigned_strategies") or [])
        for candidate in list(result.get("signal_candidates") or []):
            if isinstance(candidate, dict):
                merged_candidates.append(dict(candidate))
        if result.get("blocked_by_news_bias"):
            reason = str(result.get("news_bias_reason") or "").strip()
            if reason:
                blocked_reasons.append(reason)

    working["assigned_strategies"] = merged_assignments
    working["signal_candidates"] = merged_candidates
    working.pop("signal", None)
    working.pop("display_signal", None)
    if merged_candidates:
        working.pop("blocked_by_news_bias", None)
        working.pop("news_bias_reason", None)
    elif blocked_reasons:
        working["blocked_by_news_bias"] = True
        unique_reasons = []
        for reason in blocked_reasons:
            if reason not in unique_reasons:
                unique_reasons.append(reason)
        working["news_bias_reason"] = " | ".join(unique_reasons)
    else:
        working.pop("blocked_by_news_bias", None)
        working.pop("news_bias_reason", None)
    return working


async def run_signal_agents_parallel(signal_agents, context):
    agents = list(signal_agents or [])
    if not agents:
        return dict(context or {})
    if len(agents) == 1:
        return await agents[0].process(dict(context or {}))

    base_context = dict(context or {})
    results = await asyncio.gather(
        *(agent.process(dict(base_context)) for agent in agents),
    )
    return merge_signal_agent_results(base_context, results)
