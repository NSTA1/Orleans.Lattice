using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Guards the rule behind issues #4146, #4176 and #4180: a caller of
/// <c>Orleans.Lattice.BPlusTree.ILattice.GetRoutingAsync(CancellationToken)</c>
/// that does not force a refresh reads the routing the tree's stateless worker
/// cached for its activation, and nothing invalidates that cache when a resize,
/// snapshot or restore swaps the alias or a reshard, split or fold changes the
/// map. A routed key operation heals on its first stale-routing exception; a read
/// that enumerates shards, names a shard by index or resolves a physical tree for
/// a non-routed read never receives one, so it keeps the old answer forever.
/// </summary>
/// <remarks>
/// Every unforced call site in <c>src/</c> must appear in
/// <see cref="AllowedUnforcedCallers"/> with the reason a stale route is
/// tolerated there, so a new one cannot be added silently. A site that is no
/// longer present must be removed from the list, which keeps it exact.
/// </remarks>
[TestFixture]
public sealed class RoutingForceRefreshGuardTests
{
    private const string RoutedDataPath =
        "routed data path: every key it sends is checked by the shard that owns it, and a "
        + "StaleShardRoutingException or StaleTreeRoutingException invalidates the cache and retries";

    private const string VersionCheckedFanOut =
        "routed fan-out whose retry loop compares the map version with the registry's after the pass "
        + "and re-resolves on a change, so a stale map costs a retry, never a wrong answer";

    private const string CopyReceiveFenceCheck =
        "restored-copy receive-fence admission check (#4593): it routes nothing itself, and the route the "
        + "apply then uses is checked again on every resolution, where a stale route to a replaced copy hits that "
        + "copy's retained redirect and re-resolves; forcing it would drop the activation's cache on every apply";

    private const string ReconciledScan =
        "routed scan: a stale alias heals on StaleTreeRoutingException, and a shard that gave slots away "
        + "reports them so the scan re-reads them under the registry's current map";

    /// <summary>
    /// Unforced callers, keyed by <c>{repo-relative path}::{enclosing member}</c>,
    /// with the number of unforced calls in that member and why each is tolerated.
    /// </summary>
    private static readonly Dictionary<string, (int Count, string Reason)> AllowedUnforcedCallers = new(StringComparer.Ordinal)
    {
        // The force overload clears the cache and then delegates to the cached read.
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::GetRoutingAsync"] = (1, "the force overload delegates after invalidating the cache"),

        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::GetAsyncCore"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::GetWithVersionCoreAsync"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::ExistsAsyncCore"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::GetManyAsyncCore"] = (2, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::GetOrSetAsyncCore"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::SetAsyncCore"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::SetIfVersionAsyncCore"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::SetAsyncTtlCore"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::ApplyCrdtDeltaAsyncCore"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::DeleteAsyncCore"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::ApplyCrdtDeltaManyAsyncCore"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::SetManyAsyncCore"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::SetManyWhereAsyncCore"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::DeleteRangeAsyncCore"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::RangedCountAsyncCore"] = (2, VersionCheckedFanOut),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.cs::CountPerShardAsyncCore"] = (2, VersionCheckedFanOut),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.Entries.cs::EntriesAsyncCore"] = (2, ReconciledScan),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.Keys.cs::KeysAsyncCore"] = (2, ReconciledScan),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.ReplicationApply.cs::ApplyMergeOneAsync"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.ReplicationApply.cs::ApplyDeleteRangeCoreAsync"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.ReplicationApply.cs::ApplyMergeManyCoreAsync"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.ReplicationApply.cs::ApplyTerminalPostGateAsync"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.ReplicationApply.cs::ApplyTerminalToShardAsync"] = (1, RoutedDataPath),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.ReplicationApply.cs::ApplySetAsync"] = (1, CopyReceiveFenceCheck),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.ReplicationApply.cs::ApplyDeleteAsync"] = (1, CopyReceiveFenceCheck),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.ReplicationApply.cs::ApplyDeleteRangeAsync"] = (1, CopyReceiveFenceCheck),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.ReplicationApply.cs::ApplyMergeManyAsync"] = (1, CopyReceiveFenceCheck),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.ReplicationApply.cs::ApplyCrdtDeltaManyAsync"] = (1, CopyReceiveFenceCheck),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.ReplicationApply.cs::ApplyCrdtDeltaWithExpiryAsync"] = (1, CopyReceiveFenceCheck),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.ReplicationApply.cs::ApplyPreparedSetAsync"] = (1, CopyReceiveFenceCheck),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.ReplicationApply.cs::ApplyPreparedDeleteAsync"] = (1, CopyReceiveFenceCheck),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.ReplicationApply.cs::ApplyTxTerminalAsync"] = (1, CopyReceiveFenceCheck),
        ["src/lattice/BPlusTree/Grains/LatticeGrain.ReplicationApply.cs::FinalizeCrossTreeTerminalAsync"] = (1, CopyReceiveFenceCheck),

        // Bulk load, the backup restore lifecycle and the schema cutover force their
        // refresh (#4206); only the shadow build, which resolves a tree it has just
        // registered, stays unforced.
        ["src/lattice.backup/LatticeBackupRestoreService.cs::BuildShadowCoreAsync"] = (1, "a freshly registered shadow tree that no activation can have cached"),

        ["src/lattice.api.mcp.repocontext/Retrieval/EmbeddingRepoContextVectorIngestor.cs::LogGapShardDistributionAsync"] =
            (1, "diagnostic log line only: a stale map mislabels one log entry and steers nothing"),
    };

    private static readonly Regex MemberDeclaration = new(
        @"^\s{4,8}(?:public|private|internal|protected)\s[^;=]*?\b(\w+)\s*(?:<[^()]*>)?\s*\(",
        RegexOptions.Compiled);

    private static readonly Regex RoutingDeclaration = new(
        @"RoutingInfo>\s+GetRoutingAsync\(",
        RegexOptions.Compiled);

    private const string CallToken = "GetRoutingAsync(";

    /// <summary>One <c>GetRoutingAsync</c> call found in a source file.</summary>
    private readonly record struct RoutingCall(string Member, bool Forced, int Line);

    /// <summary>
    /// Finds every <c>GetRoutingAsync</c> call in <paramref name="lines"/>, skipping
    /// comments and the method's own declarations, and reports for each whether it
    /// passes <c>forceRefresh: true</c> and which member encloses it.
    /// </summary>
    private static List<RoutingCall> FindCalls(IReadOnlyList<string> lines)
    {
        var calls = new List<RoutingCall>();
        for (var i = 0; i < lines.Count; i++)
        {
            var line = lines[i];
            var trimmed = line.TrimStart();
            if (trimmed.StartsWith("//", StringComparison.Ordinal) || trimmed.StartsWith('*'))
            {
                continue;
            }

            if (RoutingDeclaration.IsMatch(line))
            {
                continue;
            }

            var at = line.IndexOf(CallToken, StringComparison.Ordinal);
            while (at >= 0)
            {
                var arguments = ReadArguments(lines, i, at + CallToken.Length);
                var forced = Regex.IsMatch(arguments, @"\bforceRefresh\s*:\s*true\b");
                calls.Add(new RoutingCall(EnclosingMember(lines, i), forced, i + 1));
                at = line.IndexOf(CallToken, at + CallToken.Length, StringComparison.Ordinal);
            }
        }

        return calls;
    }

    private static string ReadArguments(IReadOnlyList<string> lines, int lineIndex, int start)
    {
        var depth = 1;
        var text = new System.Text.StringBuilder();
        for (var i = lineIndex; i < lines.Count && i < lineIndex + 6; i++)
        {
            var line = lines[i];
            for (var c = i == lineIndex ? start : 0; c < line.Length; c++)
            {
                var ch = line[c];
                if (ch == '(') depth++;
                if (ch == ')' && --depth == 0) return text.ToString();
                text.Append(ch);
            }

            text.Append(' ');
        }

        return text.ToString();
    }

    private static string EnclosingMember(IReadOnlyList<string> lines, int lineIndex)
    {
        for (var i = lineIndex; i >= 0; i--)
        {
            var match = MemberDeclaration.Match(lines[i]);
            if (match.Success)
            {
                return match.Groups[1].Value;
            }
        }

        return "<file>";
    }

    [Test]
    public void Every_unforced_GetRoutingAsync_caller_in_src_is_allow_listed_with_a_reason()
    {
        var root = HygieneRepository.FindRepoRoot();
        var src = Path.Combine(root, "src");

        var unforced = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        var forcedCount = 0;
        var filesWithCalls = 0;

        foreach (var file in HygieneRepository.EnumerateFiles(src, "*.cs"))
        {
            var calls = FindCalls(File.ReadAllLines(file));
            if (calls.Count == 0)
            {
                continue;
            }

            filesWithCalls++;
            var relative = Path.GetRelativePath(root, file).Replace('\\', '/');
            foreach (var call in calls)
            {
                if (call.Forced)
                {
                    forcedCount++;
                    continue;
                }

                var key = $"{relative}::{call.Member}";
                if (!unforced.TryGetValue(key, out var found))
                {
                    unforced[key] = found = [];
                }

                found.Add(call.Line);
            }
        }

        Assert.That(filesWithCalls, Is.GreaterThan(10), "the scan must find the routing callers it guards, or it has gone vacuous");
        Assert.That(forcedCount, Is.GreaterThan(10), "the scan must recognise the forced callers, or it has gone vacuous");

        var violations = new List<string>();
        foreach (var (key, found) in unforced.OrderBy(pair => pair.Key, StringComparer.Ordinal))
        {
            if (!AllowedUnforcedCallers.TryGetValue(key, out var allowed))
            {
                violations.Add(
                    $"{key} (lines {string.Join(", ", found)}): unforced GetRoutingAsync. A read that enumerates shards, "
                    + "names a shard by index or resolves a physical tree for a non-routed read must pass forceRefresh: true; "
                    + "otherwise add the site to AllowedUnforcedCallers with the reason a stale route is tolerated.");
            }
            else if (allowed.Count != found.Count)
            {
                violations.Add($"{key}: allow-listed for {allowed.Count} unforced call(s) but has {found.Count} (lines {string.Join(", ", found)}).");
            }
        }

        foreach (var key in AllowedUnforcedCallers.Keys.Where(key => !unforced.ContainsKey(key)).OrderBy(key => key, StringComparer.Ordinal))
        {
            violations.Add($"{key}: allow-listed but has no unforced call any more; remove the entry.");
        }

        Assert.That(violations, Is.Empty, string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void FindCalls_tells_forced_from_unforced_calls_across_lines_and_skips_comments_and_declarations()
    {
        string[] lines =
        [
            "internal sealed class Sample",
            "{",
            "    /// <see cref=\"GetRoutingAsync(CancellationToken)\"/>",
            "    public ValueTask<RoutingInfo> GetRoutingAsync(CancellationToken cancellationToken = default)",
            "    {",
            "        // a plain GetRoutingAsync() would be stale here",
            "        return default;",
            "    }",
            "",
            "    private async Task ReadAsync(CancellationToken cancellationToken)",
            "    {",
            "        var cached = await lattice.GetRoutingAsync(cancellationToken);",
            "        var fresh = await lattice",
            "            .GetRoutingAsync(",
            "                forceRefresh: true,",
            "                cancellationToken);",
            "    }",
            "}",
        ];

        var calls = FindCalls(lines);

        Assert.That(
            calls.Select(call => (call.Member, call.Forced, call.Line)),
            Is.EqualTo(new[] { ("ReadAsync", false, 12), ("ReadAsync", true, 14) }));
    }
}
