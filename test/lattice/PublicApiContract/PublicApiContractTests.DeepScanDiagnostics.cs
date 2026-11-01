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

    /// <summary>
    /// Counts the scan-page measurements one tree's shard roots emit, keyed by
    /// instrument name and non-tree tags. In-process silos publish on the same
    /// static meter, so the listener sees them.
    /// </summary>
    private sealed class DeepScanSignals : IDisposable
    {
        private readonly string _treeId;
        private readonly ConcurrentDictionary<string, long> _counts = new(StringComparer.Ordinal);
        private readonly MeterListener _listener;

        public DeepScanSignals(string treeId)
        {
            _treeId = treeId;
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
            if (value == 0 || !instrument.Name.Contains(".scan", StringComparison.Ordinal))
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
        ILattice tree, IReadOnlyList<string> scanned, IReadOnlyList<string> expected, string stages, DeepScanSignals signals)
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

        await AppendDiagnosticAsync(report, "leaf chain", async () =>
        {
            var chain = new StringBuilder();
            var leafId = await shard.GetLeftmostLeafIdAsync();
            for (var hop = 0; leafId is { } id && hop < 512; hop++)
            {
                var (leafKeys, link) = await Client.GetGrain<IBPlusLeafGrain>(id).GetKeysAndNextSiblingAsync(null, null);
                chain.Append($"\n  {hop}: {id} {link.KeyRange} keys [{string.Join(",", leafKeys)}]");
                leafId = link.NextSibling;
            }

            return chain.ToString();
        });

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
