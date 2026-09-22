using System;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Prices the declared-span admission step that issue #2663 moved to the front
/// of <c>BPlusLeafGrain.SetManyWherePredicateAsync</c>, because that method is
/// the conditional batch write path and the fix changed what it scans on every
/// call.
/// <para>
/// <b>What actually moved.</b> The prior shape ran the guard pass first and
/// then scanned the <i>matched</i> subset for an out-of-span key. That scan was
/// structurally unable to see the keys the defect was about: a key whose row a
/// split had moved to a sibling probes absent in this leaf's cache, so it never
/// reached <c>matched</c> in the first place. The shipped shape scans the
/// <i>caller's whole entry list</i> before the guard runs, and - because that
/// scan has then already cleared the batch - skips the post-guard scan
/// entirely. So the two arms are not "old work plus new work": they are one
/// scan traded for another, over a different collection.
/// </para>
/// <para>
/// <b>Both arms call the real shipped comparator.</b> The per-entry cost being
/// measured is <see cref="SplitBoundary.Owns"/>, the same ordinal-compare pair
/// <c>BPlusLeafGrain.DeclaresKey</c> delegates to on the production path, so
/// neither arm models the comparison it is pricing.
/// </para>
/// <para>
/// <b>Two shapes, because they answer different questions.</b> The
/// <c>NoDeclaredSpan</c> pair is the overwhelmingly common production shape - a
/// leaf that has never split declares no bound, so the shipped admission scan
/// returns on its first line and the prior post-guard scan is what disappears.
/// The <c>DeclaredSpan</c> pair is the adversarial shape for the fix: a split
/// leaf whose entries are all in span, where the shipped scan walks the full
/// entry list and the prior scan walked only the matched subset. That is the
/// one case where the fix can cost more, so it is the one worth measuring;
/// <c>MatchRate</c> sets how much smaller the matched subset was.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=condsetmanyadmission</c> (or
/// <c>--suite condsetmanyadmission</c>); see <c>Program.cs</c>. No Orleans silo
/// is involved, so it runs cheaply at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ConditionalSetManyAdmissionBenchmarks
{
    private const int BatchSize = 256;

    /// <summary>
    /// The fraction of the batch that satisfies the guard, expressed as one in
    /// <c>MatchRate</c>. It is the only parameter that separates the two arms
    /// in the declared-span shape: the prior scan walked the matched subset, so
    /// the sparser the matches the larger the entry-list scan looks beside it.
    /// A rate of 1 is the dense case where the two scans walk the same length.
    /// </summary>
    [Params(1, 4)]
    public int MatchRate { get; set; }

    private List<KeyValuePair<string, byte[]>> _entries = null!;
    private List<KeyValuePair<string, byte[]>> _matched = null!;

    private string? _lowInclusive;
    private string? _highExclusive;

    [GlobalSetup]
    public void Setup()
    {
        var value = new byte[] { 1, 2, 3, 4 };
        _entries = new List<KeyValuePair<string, byte[]>>(BatchSize);
        _matched = new List<KeyValuePair<string, byte[]>>(BatchSize);

        for (var i = 0; i < BatchSize; i++)
        {
            var entry = new KeyValuePair<string, byte[]>($"cs-{i:D6}", value);
            _entries.Add(entry);

            // Stands in for the guard pass's output. Building it once in setup
            // keeps the guard's dictionary probes - which the fix does not
            // touch - out of the measured region, so the delta reported is the
            // admission scan and nothing else.
            if (i % MatchRate == 0)
            {
                _matched.Add(entry);
            }
        }

        // The split leaf's declared range, chosen to contain the whole batch:
        // the adversarial shape for the fix is the one where the scan runs to
        // completion without finding an out-of-span key, because an early
        // return would flatter it.
        _lowInclusive = "cs-000000";
        _highExclusive = "cs-999999";
    }

    /// <summary>
    /// Prior shape on a leaf that declares no range: the guard pass has already
    /// run, and the matched subset is scanned for an out-of-span key. The scan
    /// short-circuits on the absent range, so this prices the call itself.
    /// </summary>
    [Benchmark(Description = "Admission/no declared span - prior (scan matched)")]
    public bool NoDeclaredSpan_Prior() => ContainsOutOfSpanKey(_matched, null, null);

    /// <summary>
    /// Shipped shape on the same leaf: the admission scan runs first over the
    /// caller's whole entry list and returns on the absent range, after which
    /// the post-guard scan is skipped outright.
    /// </summary>
    [Benchmark(Baseline = true, Description = "Admission/no declared span - shipped (scan entries, skip post-guard)")]
    public bool NoDeclaredSpan_Shipped() => ContainsOutOfSpanKey(_entries, null, null);

    /// <summary>
    /// Prior shape on a split leaf whose entries are all in span: the matched
    /// subset is walked to completion against the real comparator.
    /// </summary>
    [Benchmark(Description = "Admission/declared span - prior (scan matched)")]
    public bool DeclaredSpan_Prior() => ContainsOutOfSpanKey(_matched, _lowInclusive, _highExclusive);

    /// <summary>
    /// Shipped shape on the same split leaf: the caller's whole entry list is
    /// walked to completion instead. This is the arm that can cost more than
    /// the prior shape, and by how much is the number this lane exists to
    /// report.
    /// </summary>
    [Benchmark(Description = "Admission/declared span - shipped (scan entries)")]
    public bool DeclaredSpan_Shipped() => ContainsOutOfSpanKey(_entries, _lowInclusive, _highExclusive);

    /// <summary>
    /// Transcribes <c>BPlusLeafGrain.ContainsOutOfSpanKey</c>, whose range
    /// bounds are read from grain state and so cannot be driven from here
    /// without a silo. The early return and the loop are identical to the
    /// shipped method and the comparator called per entry is the shipped
    /// <see cref="SplitBoundary.Owns"/>, so the only thing not shared with
    /// production is where the two bounds came from.
    /// </summary>
    private static bool ContainsOutOfSpanKey(
        List<KeyValuePair<string, byte[]>> entries, string? lowInclusive, string? highExclusive)
    {
        if (lowInclusive is null && highExclusive is null)
        {
            return false;
        }

        foreach (var entry in entries)
        {
            if (!SplitBoundary.Owns(entry.Key, lowInclusive, highExclusive))
            {
                return true;
            }
        }

        return false;
    }
}
