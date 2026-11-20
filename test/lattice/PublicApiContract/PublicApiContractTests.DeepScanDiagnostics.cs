using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // Issue #5607: the cold-routing deep scan once returned 248 of 256 keys
    // (k00016..k00023 missing) in a run that took 18 s instead of the usual
    // ~1.3 s, and the run left nothing that could say whether the keys were
    // lost or merely skipped by the scan. A failure now reports enough to tell
    // those apart without a rerun: a point read of every missing key (a miss is
    // lost data, a hit is a scan fault), the leaf each missing key routes to,
    // the leaf sibling chain with every leaf's declared bounds, a page-by-page
    // re-walk of the shard with each page's resume key, a second full scan, the
    // stage timings, and the scan-page signals the tree's shard root emitted
    // while the failing scan ran (ceiling fires, stalls, chain regressions).
    //
    // The leaf layout is not fixed: a leaf reactivated from a snapshot divides
    // at the snapshot frame's median row, not at the median of every row it
    // holds, so a reactivation between two batches turns [k00016, k00040) into
    // [k00016, k00024) - exactly the 8 keys #5607 lost. The report therefore
    // also records the leaf chain as the build left it, before the restart, and
    // how the build's divisions chose their pivots.

    /// <summary>
    /// Counts the measurements one tree's grains emit on a selected set of
    /// instruments, keyed by instrument name and non-tree tags. In-process silos
    /// publish on the same static meter, so the listener sees them.
    /// </summary>
    private sealed class DeepScanSignals : IDisposable
    {
        private readonly string _treeId;
        private readonly Func<Instrument, bool> _include;
        private readonly ConcurrentDictionary<string, long> _counts = new(StringComparer.Ordinal);
        private readonly MeterListener _listener;

        /// <summary>The scan-page instruments a shard root emits while it fills pages.</summary>
        public static bool ScanInstruments(Instrument instrument) =>
            instrument.Name.Contains(".scan", StringComparison.Ordinal);

        /// <summary>
        /// The leaf division outcomes and the bisect refusals (issue #5607). A
        /// division that records no refusal took its pivot from the median of an
        /// attached snapshot frame rather than of every row the leaf held, which is
        /// what turns the second leaf into [k00016, k00024) instead of
        /// [k00016, k00040) after a reactivation.
        /// </summary>
        public static bool SplitInstruments(Instrument instrument) =>
            ReferenceEquals(instrument, LatticeMetrics.LeafSplitAttempts)
            || ReferenceEquals(instrument, LatticeMetrics.LeafBisectRefusals);

        public DeepScanSignals(string treeId)
            : this(treeId, ScanInstruments)
        {
        }

        public DeepScanSignals(string treeId, Func<Instrument, bool> include)
        {
            _treeId = treeId;
            _include = include;
            _listener = MeterListening.StartForMeter(LatticeMetrics.Meter, listener =>
            {
                // Counters are summed, so a series primed with Add(0) does not
                // read as an event; histogram samples are counted.
                listener.SetMeasurementEventCallback<long>(
                    (instrument, value, tags, _) => Record(instrument, value, tags));
                listener.SetMeasurementEventCallback<double>(
                    (instrument, _, tags, _) => Record(instrument, 1, tags));
            });
        }

        public string Describe() =>
            _counts.IsEmpty
                ? "(none)"
                : string.Join("; ", _counts.OrderBy(c => c.Key, StringComparer.Ordinal).Select(c => $"{c.Key} x{c.Value}"));

        public void Dispose() => _listener.Dispose();

        private void Record(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            if (value == 0 || !_include(instrument))
            {
                return;
            }

            string? tree = null;
            var label = new StringBuilder(instrument.Name);
            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, LatticeMetrics.TagTree, StringComparison.Ordinal))
                {
                    tree = tag.Value as string;
                }
                else if (!string.Equals(tag.Key, LatticeMetrics.TagShard, StringComparison.Ordinal))
                {
                    label.Append(' ').Append(tag.Key).Append('=').Append(tag.Value);
                }
            }

            if (string.Equals(tree, _treeId, StringComparison.Ordinal))
            {
                _counts.AddOrUpdate(label.ToString(), value, (_, count) => count + value);
            }
        }
    }

    private async Task<string> DescribeDeepScanMissAsync(
        ILattice tree, IReadOnlyList<string> scanned, IReadOnlyList<string> expected, string stages, DeepScanSignals signals,
        string? buildLayout = null, DeepScanSignals? buildSplits = null)
    {
        var report = new StringBuilder();
        var missing = expected.Except(scanned, StringComparer.Ordinal).ToList();
        var extra = scanned.Except(expected, StringComparer.Ordinal).ToList();
        report.AppendLine($"scan returned {scanned.Count} of {expected.Count} keys; {stages}");
        report.AppendLine($"missing ({missing.Count}): {string.Join(",", missing)}");
        if (extra.Count > 0)
        {
            report.AppendLine($"unexpected ({extra.Count}): {string.Join(",", extra)}");
        }

        report.AppendLine($"scan-page signals during the scan: {signals.Describe()}");
        if (buildLayout is not null)
        {
            report.AppendLine($"leaf chain before the restart:{buildLayout}");
        }

        if (buildSplits is not null)
        {
            report.AppendLine(
                $"leaf divisions during the build: {buildSplits.Describe()} "
                + "(a divided outcome with no matching bisect refusal pivoted on a snapshot frame's median)");
        }
        await AppendDiagnosticAsync(report, "point reads", async () =>
        {
            var reads = new List<string>(missing.Count);
            foreach (var key in missing)
            {
                reads.Add($"{key}={(await tree.GetAsync(key) is null ? "MISS" : "hit")}");
            }

            return string.Join(",", reads) + " (a MISS is lost data, a hit is a scan fault)";
        });

        var routing = await tree.GetRoutingAsync();
        var shard = Client.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/0");
        await AppendDiagnosticAsync(report, "routed owners", async () =>
        {
            var owners = new StringBuilder();
            foreach (var key in missing)
            {
                var owner = await shard.GetLeafIdForKeyAsync(key);
                if (owner is null)
                {
                    owners.Append($"\n  {key} -> (none)");
                    continue;
                }

                var (ownerKeys, ownerLink) = await Client.GetGrain<IBPlusLeafGrain>(owner.Value)
                    .GetKeysAndNextSiblingAsync(null, null);
                owners.Append($"\n  {key} -> {owner} {ownerLink.KeyRange} keys [{string.Join(",", ownerKeys)}]");
            }

            return owners.ToString();
        });

        await AppendDiagnosticAsync(report, "leaf chain", () => DescribeDeepScanLeafChainAsync(shard, withKeys: true));

        await AppendDiagnosticAsync(report, "page re-walk", async () =>
        {
            var pages = new StringBuilder();
            string? continuation = null;
            string? resumeFrom = null;
            for (var page = 0; page < 512; page++)
            {
                var result = await shard.GetSortedKeysBatchAsync(null, null, 64, continuation, null, resumeFrom);
                pages.Append(
                    $"\n  {page}: {result.Keys.Count} keys "
                    + $"[{result.Keys.FirstOrDefault()}..{result.Keys.LastOrDefault()}] "
                    + $"hasMore={result.HasMore} resume={result.ResumeFromKey ?? "-"}");
                if (!result.HasMore)
                {
                    break;
                }

                if (result.ResumeFromKey is not null)
                {
                    resumeFrom = result.ResumeFromKey;
                }
                else if (result.Keys.Count > 0)
                {
                    continuation = result.Keys[^1];
                }
                else
                {
                    break;
                }
            }

            return pages.ToString();
        });

        await AppendDiagnosticAsync(report, "second scan", async () =>
        {
            var rescan = new List<string>();
            await foreach (var key in tree.ScanKeysAsync())
            {
                rescan.Add(key);
            }

            var stillMissing = expected.Except(rescan, StringComparer.Ordinal).ToList();
            return $"{rescan.Count} keys, missing [{string.Join(",", stillMissing)}]";
        });

        return report.ToString();
    }

    /// <summary>
    /// The leaf sibling chain from the leftmost leaf, one line per leaf with its
    /// declared bounds and either its keys or only their count.
    /// </summary>
    private async Task<string> DescribeDeepScanLeafChainAsync(IShardRootGrain shard, bool withKeys)
    {
        var chain = new StringBuilder();
        var leafId = await shard.GetLeftmostLeafIdAsync();
        for (var hop = 0; leafId is { } id && hop < 512; hop++)
        {
            var (leafKeys, link) = await Client.GetGrain<IBPlusLeafGrain>(id).GetKeysAndNextSiblingAsync(null, null);
            chain.Append(withKeys
                ? $"\n  {hop}: {id} {link.KeyRange} keys [{string.Join(",", leafKeys)}]"
                : $"\n  {hop}: {id} {link.KeyRange} {leafKeys.Count} keys");
            leafId = link.NextSibling;
        }

        return chain.ToString();
    }

    private static async Task AppendDiagnosticAsync(StringBuilder report, string section, Func<Task<string>> collect)
    {
        try
        {
            report.AppendLine($"{section}: {await collect()}");
        }
        catch (Exception ex)
        {
            report.AppendLine($"{section}: failed to collect ({ex.GetType().Name}: {ex.Message})");
        }
    }
}
