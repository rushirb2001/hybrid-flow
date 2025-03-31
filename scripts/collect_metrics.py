#!/usr/bin/env python3
"""Collect and display system metrics and performance benchmarks."""

import statistics
import time
from datetime import datetime

from hybridflow.api import HybridFlowAPI


def benchmark(func, name: str, iterations: int = 10) -> dict:
    """Run benchmark and return stats."""
    times = []
    errors = 0
    for _ in range(iterations):
        try:
            start = time.perf_counter()
            func()
            elapsed = (time.perf_counter() - start) * 1000
            times.append(elapsed)
        except Exception:
            errors += 1
    if not times:
        return None
    return {
        "name": name,
        "p50": statistics.median(times),
        "p95": sorted(times)[int(len(times) * 0.95)] if len(times) >= 20 else max(times),
        "mean": statistics.mean(times),
        "min": min(times),
        "max": max(times),
        "std": statistics.stdev(times) if len(times) > 1 else 0,
        "iterations": len(times),
        "errors": errors,
    }


def collect_scale_metrics(api: HybridFlowAPI) -> dict:
    """Collect scale metrics from all storage systems."""
    stats = api.get_stats()
    vector = stats["vector"]
    graph = stats["graph"]
    metadata = stats["metadata"]

    return {
        "paragraphs_indexed": vector.get("point_count", 0),
        "knowledge_graph_nodes": graph.get("total_nodes", 0),
        "graph_relationships": graph.get("total_relationships", 0),
        "chapters_processed": metadata.get("total_chapters", 0),
        "figures_indexed": graph.get("nodes", {}).get("Figure", 0),
        "tables_indexed": graph.get("nodes", {}).get("Table", 0),
        "node_distribution": graph.get("nodes", {}),
        "relationship_types": graph.get("relationships", {}),
        "textbook_distribution": vector.get("textbook_distribution", {}),
        "chapters_by_textbook": metadata.get("chapters_by_textbook", {}),
        "avg_text_length": vector.get("avg_text_length", 0),
    }


def collect_performance_metrics(api: HybridFlowAPI) -> dict:
    """Collect performance benchmarks."""
    # Get sample chunk_id from Neo4j
    with api.neo4j.driver.session() as session:
        result = session.run(
            "MATCH (p:Paragraph) RETURN p.chunk_id as chunk_id LIMIT 1"
        ).single()
        sample_chunk_id = result["chunk_id"] if result else "bailey:ch60:1.1"

        chap_result = session.run(
            """
            MATCH (c:Chapter)-[:HAS_SECTION]->(s:Section)-[:HAS_PARAGRAPH]->(p:Paragraph)
            RETURN c.id as chapter_id, count(p) as para_count
            ORDER BY para_count DESC LIMIT 1
            """
        ).single()
        sample_chapter_id = chap_result["chapter_id"] if chap_result else "bailey:ch60"

    benchmarks = {}

    # Graph operations
    benchmarks["context_retrieval"] = benchmark(
        lambda: api.get_context(sample_chunk_id), "get_context()", 30
    )
    benchmarks["sequential_navigation"] = benchmark(
        lambda: api.get_surrounding(sample_chunk_id, before=2, after=2),
        "get_surrounding()",
        30,
    )
    benchmarks["cross_ref_resolution"] = benchmark(
        lambda: api.get_references(sample_chunk_id), "get_references()", 30
    )
    benchmarks["sibling_retrieval"] = benchmark(
        lambda: api.get_siblings(sample_chunk_id), "get_siblings()", 30
    )
    benchmarks["chapter_structure"] = benchmark(
        lambda: api.get_chapter_structure(sample_chapter_id),
        "get_chapter_structure()",
        30,
    )

    # Metadata operations
    benchmarks["metadata_lookup"] = benchmark(
        lambda: api.get_chapter_metadata("bailey", "60"), "get_chapter_metadata()", 30
    )
    benchmarks["aggregate_stats"] = benchmark(
        lambda: api.get_aggregate_stats(), "get_aggregate_stats()", 30
    )

    # System operations
    benchmarks["health_check"] = benchmark(
        lambda: api.health_check(), "health_check()", 20
    )
    benchmarks["full_stats"] = benchmark(lambda: api.get_stats(), "get_stats()", 10)

    # Tool dispatch overhead
    direct_bench = benchmark(
        lambda: api.get_context(sample_chunk_id), "direct_call", 30
    )
    tool_bench = benchmark(
        lambda: api.invoke_tool("get_context", chunk_id=sample_chunk_id),
        "invoke_tool()",
        30,
    )
    benchmarks["tool_dispatch"] = {
        "direct": direct_bench,
        "via_invoke": tool_bench,
        "overhead_ms": tool_bench["mean"] - direct_bench["mean"]
        if tool_bench and direct_bench
        else 0,
    }

    # Connection pool test
    def rapid_calls():
        for _ in range(5):
            api.get_context(sample_chunk_id)
            api.get_chapter_metadata("bailey", "60")

    benchmarks["connection_pool"] = benchmark(rapid_calls, "rapid_sequential", 10)
    if benchmarks["connection_pool"]:
        benchmarks["connection_pool"]["avg_per_call"] = (
            benchmarks["connection_pool"]["p50"] / 10
        )

    return benchmarks


def print_scale_metrics(metrics: dict) -> None:
    """Print scale metrics in formatted output."""
    print("=" * 60)
    print("SCALE METRICS")
    print("=" * 60)
    print()
    print(f"  Paragraphs indexed:      {metrics['paragraphs_indexed']:>10,}")
    print(f"  Knowledge graph nodes:   {metrics['knowledge_graph_nodes']:>10,}")
    print(f"  Graph relationships:     {metrics['graph_relationships']:>10,}")
    print(f"  Chapters processed:      {metrics['chapters_processed']:>10}")
    print(f"  Figures indexed:         {metrics['figures_indexed']:>10,}")
    print(f"  Tables indexed:          {metrics['tables_indexed']:>10,}")
    print(f"  Avg text length:         {metrics['avg_text_length']:>10.0f} chars")
    print()

    print("Node Distribution:")
    for label, count in sorted(
        metrics["node_distribution"].items(), key=lambda x: -x[1]
    ):
        print(f"    {label:20s}: {count:>8,}")
    print()

    print("Relationship Types:")
    for rel, count in sorted(
        metrics["relationship_types"].items(), key=lambda x: -x[1]
    ):
        print(f"    {rel:20s}: {count:>8,}")
    print()

    print("Chapters by Textbook:")
    for tb, count in metrics["chapters_by_textbook"].items():
        name = {
            "bailey": "Bailey & Love",
            "sabiston": "Sabiston",
            "schwartz": "Schwartz",
        }.get(tb, tb)
        print(f"    {name:20s}: {count:>8}")


def print_performance_metrics(benchmarks: dict) -> None:
    """Print performance metrics in formatted output."""
    print()
    print("=" * 60)
    print("PERFORMANCE METRICS")
    print("=" * 60)
    print()

    print("GRAPH QUERY LATENCY (Neo4j):")
    print("-" * 60)
    for key in [
        "context_retrieval",
        "sequential_navigation",
        "cross_ref_resolution",
        "sibling_retrieval",
        "chapter_structure",
    ]:
        b = benchmarks.get(key)
        if b:
            print(
                f"  {b['name']:25s} p50={b['p50']:6.1f}ms  p95={b['p95']:6.1f}ms  std={b['std']:5.1f}ms"
            )

    print()
    print("METADATA QUERY LATENCY (SQLite):")
    print("-" * 60)
    for key in ["metadata_lookup", "aggregate_stats"]:
        b = benchmarks.get(key)
        if b:
            print(
                f"  {b['name']:25s} p50={b['p50']:6.1f}ms  p95={b['p95']:6.1f}ms  std={b['std']:5.1f}ms"
            )

    print()
    print("SYSTEM OPERATIONS:")
    print("-" * 60)
    for key in ["health_check", "full_stats"]:
        b = benchmarks.get(key)
        if b:
            print(
                f"  {b['name']:25s} p50={b['p50']:6.1f}ms  p95={b['p95']:6.1f}ms  std={b['std']:5.1f}ms"
            )

    print()
    print("TOOL DISPATCH OVERHEAD:")
    print("-" * 60)
    td = benchmarks.get("tool_dispatch", {})
    if td.get("direct") and td.get("via_invoke"):
        print(f"  Direct method call:      p50={td['direct']['p50']:6.1f}ms")
        print(f"  Via invoke_tool():       p50={td['via_invoke']['p50']:6.1f}ms")
        print(
            f"  Dispatch overhead:       ~{td['overhead_ms']:5.2f}ms ({td['overhead_ms']/td['direct']['mean']*100:.1f}%)"
        )

    print()
    print("CONNECTION POOL PERFORMANCE:")
    print("-" * 60)
    cp = benchmarks.get("connection_pool")
    if cp:
        print(
            f"  10-call sequence:        p50={cp['p50']:6.1f}ms  (avg {cp['avg_per_call']:.1f}ms/call)"
        )


def print_sla_summary(benchmarks: dict) -> None:
    """Print SLA compliance summary."""
    print()
    print("=" * 60)
    print("PERFORMANCE SLA SUMMARY")
    print("=" * 60)
    print()
    print("Operation                       Target    Actual p95   Status")
    print("-" * 60)

    sla_checks = [
        ("context_retrieval", "Graph context retrieval", 100),
        ("sequential_navigation", "Sequential navigation", 150),
        ("cross_ref_resolution", "Cross-reference resolution", 100),
        ("metadata_lookup", "Chapter metadata lookup", 50),
        ("health_check", "Health check (3 backends)", 200),
    ]

    all_pass = True
    for key, name, target in sla_checks:
        b = benchmarks.get(key)
        if b:
            status = "PASS" if b["p95"] < target else "FAIL"
            if status == "FAIL":
                all_pass = False
            print(f"  {name:28s} <{target}ms    {b['p95']:6.1f}ms    {status}")

    td = benchmarks.get("tool_dispatch", {})
    if td.get("overhead_ms") is not None:
        status = "PASS" if td["overhead_ms"] < 5 else "FAIL"
        if status == "FAIL":
            all_pass = False
        print(f"  {'Tool dispatch overhead':28s} <5ms     {td['overhead_ms']:6.2f}ms    {status}")

    print()
    print(f"Overall: {'ALL SLAs PASS' if all_pass else 'SOME SLAs FAILED'}")


def main():
    """Main entry point."""
    print()
    print("=" * 60)
    print(f"HYBRIDFLOW METRICS COLLECTION - {datetime.now().isoformat()}")
    print("=" * 60)
    print()

    print("Initializing HybridFlow API...")
    api = HybridFlowAPI()

    try:
        print("Collecting scale metrics...")
        scale = collect_scale_metrics(api)
        print_scale_metrics(scale)

        print("\nCollecting performance metrics (this may take a minute)...")
        perf = collect_performance_metrics(api)
        print_performance_metrics(perf)
        print_sla_summary(perf)

    finally:
        api.close()
        print("\nAPI closed.")


if __name__ == "__main__":
    main()
