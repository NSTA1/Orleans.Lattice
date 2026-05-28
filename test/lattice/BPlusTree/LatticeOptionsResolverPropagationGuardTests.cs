using System.Reflection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Reflective propagation guard for <see cref="LatticeOptionsResolver"/>.
/// Enumerates every public instance property declared on
/// <see cref="LatticeOptions"/> that is also surfaced on
/// <see cref="ResolvedLatticeOptions"/>, and asserts the resolver
/// copies the configured value through (or, for properties the resolver
/// deliberately transforms, copies through the transformed value the
/// transformation should have produced).
/// <para>
/// This regression test exists because the resolver previously dropped
/// <see cref="LatticeOptions.DigestCoalescingWindowMs"/> on the floor:
/// every <see cref="ResolvedLatticeOptions"/> consumer (notably
/// <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>)
/// observed the inherited <see cref="LatticeOptions"/> default (0) even
/// when the operator or bench had set the property to a positive value.
/// The c2-xxviii memo's claimed digest-coalescing win on Azure was a
/// misattribution as a direct consequence. The guard fails loudly when
/// a future property addition forgets to extend the resolver's copy
/// block, and emits the receiver-typed call sites so the fix is
/// pinpoint-localised.
/// </para>
/// </summary>
[TestFixture]
public class LatticeOptionsResolverPropagationGuardTests
{
    /// <summary>
    /// Properties the resolver intentionally transforms before assigning
    /// to <see cref="ResolvedLatticeOptions"/>. The propagation guard
    /// asserts each maps to the documented transformation output rather
    /// than the raw input.
    /// </summary>
    private static readonly Dictionary<string, TransformExpectation> TransformedProperties =
        new(StringComparer.Ordinal)
        {
            // Structural pins: sourced from TreeRegistryEntry, not from
            // LatticeOptions. The bench-baseline (a non-system user tree
            // whose registry entry seeds Default* values) collapses to
            // the LatticeConstants defaults.
            ["MaxLeafKeys"] = new(_ => LatticeConstants.DefaultMaxLeafKeys),
            ["MaxInternalChildren"] = new(_ => LatticeConstants.DefaultMaxInternalChildren),
            ["ShardCount"] = new(_ => LatticeConstants.DefaultShardCount),
            // MaintainProjectionDigest: gated by registry-side latch +
            // per-tree override. Under the propagation guard's plain
            // fixture (no latch, no per-tree override), the resolver
            // returns the configured value unchanged - so the
            // transformation expectation is "value passes through".
            ["MaintainProjectionDigest"] = new(input => input),
            // Compaction floors clamp configured values BELOW the floor
            // up to the floor. Sentinel values used by the propagation
            // guard are deliberately above every floor in the system,
            // so the clamp is a no-op and the expectation is "value
            // passes through".
            ["CompactionShardTickInterval"] = new(input => input),
            ["CompactionLeafBatchSize"] = new(input => input),
        };

    /// <summary>
    /// Properties NOT exposed by <see cref="ResolvedLatticeOptions"/>
    /// despite living on <see cref="LatticeOptions"/>. Each is consumed
    /// downstream directly via <c>IOptionsMonitor&lt;LatticeOptions&gt;.Get(treeId)</c>
    /// (e.g. WAL options consumed inside <c>WalShardGrain</c>) rather
    /// than via the resolver, so the propagation guard does not
    /// fail-build for these - but it does require the operator to
    /// confirm that intentionally-bypassed properties are listed here
    /// so adding a new property without thinking about propagation is
    /// caught by the test.
    /// </summary>
    private static readonly HashSet<string> IntentionallyBypassedProperties =
        new(StringComparer.Ordinal)
        {
            // WAL configuration: WalShardGrain reads these directly from
            // IOptionsMonitor at activation time; the resolver is not
            // on the WAL hot path.
            "WalPartitions",
            "WalMaxBatchEntries",
            "WalMaxBatchBytes",
            "WalMaxPendingBatches",
            // Per-tree-registry-driven gates with their own resolver
            // logic outside LatticeOptionsResolver.
            "PublishEvents",                // PublishEventsGate consults registry entry
            "EventStreamProviderName",      // LatticeEventPublisher reads via IOptionsMonitor
            // Per-tree saga / cursor / snapshot retention that downstream
            // grains read via their own IOptionsMonitor activation cache.
            "TxDecisionRetention",
            "MaxCursorSnapshotPinTtl",
            "MaxPinnedSagaDecisions",
            "MaxSnapshotReplayEntries",
            "SnapshotLeafIdleTtl",
            "PrefetchEntriesScan",
        };

    private sealed record TransformExpectation(Func<object?, object?> Expected);

    [Test]
    public async Task Every_ResolvedLatticeOptions_property_is_propagated_from_baseOptions()
    {
        var failures = new List<string>();

        var resolvedDeclaredProps = typeof(ResolvedLatticeOptions)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.Name)
            .ToHashSet(StringComparer.Ordinal);

        var latticeOptionProps = typeof(LatticeOptions)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.GetSetMethod(nonPublic: false) is not null)
            .ToList();

        foreach (var prop in latticeOptionProps)
        {
            // ResolvedLatticeOptions inherits from LatticeOptions; every
            // LatticeOptions property is therefore reachable on the
            // resolved type via the same name. The test still records
            // whether the resolver explicitly assigned to it.
            if (!resolvedDeclaredProps.Contains(prop.Name)
                && IntentionallyBypassedProperties.Contains(prop.Name))
            {
                // Bypassed-by-design: skip.
                continue;
            }

            if (IntentionallyBypassedProperties.Contains(prop.Name))
            {
                continue;
            }

            var sentinel = PickSentinel(prop);
            if (sentinel is null)
            {
                // Property type we don't know how to sentinel-test
                // (e.g. Func<,> for storage-provider injection). Skip
                // and rely on the operator to add an explicit case
                // when one becomes performance-relevant.
                continue;
            }

            var baseOptions = new LatticeOptions();
            prop.SetValue(baseOptions, sentinel);
            var resolver = BuildResolverFor(baseOptions);

            var resolved = await resolver.ResolveAsync("user-tree-propagation-guard");
            var actual = prop.GetValue(resolved);

            object? expected;
            if (TransformedProperties.TryGetValue(prop.Name, out var transform))
            {
                expected = transform.Expected(sentinel);
            }
            else
            {
                expected = sentinel;
            }

            if (!Equals(actual, expected))
            {
                var sites = FindResolvedConsumers(prop.Name);
                var sitesBlock = sites.Count == 0
                    ? "    (no obvious `resolved.X` / `_options.X` / `opts.X` call sites found - scan src/ manually for direct property reads on ResolvedLatticeOptions instances)"
                    : string.Join("\n", sites.Select(s => "    " + s));
                failures.Add(
                    $"  LatticeOptions.{prop.Name} not propagated by LatticeOptionsResolver.\n" +
                    $"    expected: {expected ?? "<null>"}\n" +
                    $"    actual:   {actual ?? "<null>"}\n" +
                    $"  consumer sites that may be observing the wrong value:\n" +
                    sitesBlock);
            }
        }

        Assert.That(failures, Is.Empty,
            "LatticeOptionsResolver propagation guard failed; the resolver dropped one or more " +
            "LatticeOptions properties on the floor, so downstream consumers of " +
            "ResolvedLatticeOptions observe the LatticeOptions default instead of the operator's " +
            "configured value. Add the missing assignment(s) to ResolveAsync's return-object " +
            "construction block (or, if the property is deliberately consumed via " +
            "IOptionsMonitor.Get(treeId) directly, add it to IntentionallyBypassedProperties " +
            "with a comment explaining the downstream consumer).\n" +
            string.Join("\n", failures));
    }

    private static LatticeOptionsResolver BuildResolverFor(LatticeOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        // Hand the resolver a fully-pinned entry so it does not lazy-
        // register and so the structural-pin fields land on
        // LatticeConstants defaults.
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = LatticeConstants.DefaultShardCount,
            }));

        return new LatticeOptionsResolver(factory, monitor);
    }

    /// <summary>
    /// Pick a sentinel value that is guaranteed to differ from both the
    /// runtime default and any documented compile-time default the
    /// property carries. Returning <see langword="null"/> means the
    /// type is unsupported by the propagation guard and the property
    /// is skipped.
    /// </summary>
    private static object? PickSentinel(PropertyInfo prop)
    {
        var t = prop.PropertyType;
        if (t == typeof(int))
        {
            // Prime well above every documented floor in the system so
            // the compaction-floor clamps are no-ops.
            return 12289;
        }
        if (t == typeof(long))
        {
            return 12289L;
        }
        if (t == typeof(bool))
        {
            // Flip vs the existing default - we read the current
            // default off a fresh LatticeOptions and pick the opposite.
            var def = (bool)prop.GetValue(new LatticeOptions())!;
            return !def;
        }
        if (t == typeof(TimeSpan))
        {
            return TimeSpan.FromMinutes(13) + TimeSpan.FromMilliseconds(37);
        }
        if (t == typeof(double))
        {
            return 0.314159;
        }
        if (t == typeof(string) || t == typeof(string).MakeByRefType())
        {
            return "propagation-guard-sentinel";
        }
        // Anything else (Func<,>, custom enums we don't recognise, etc.)
        // - skip silently. Add an explicit branch here if/when a
        // performance-relevant property of a new type lands.
        return null;
    }

    /// <summary>
    /// Walk src/ for call sites that read <c>(_options|opts|resolved|cached|*Resolved*).PropName</c>
    /// where the receiver is plausibly a <see cref="ResolvedLatticeOptions"/>.
    /// Returns relative paths so the failure message points at
    /// real source lines; empty list means no obvious site was
    /// found and the operator must scan manually.
    /// </summary>
    private static List<string> FindResolvedConsumers(string propName)
    {
        var hits = new List<string>();
        var repoRoot = FindRepoRoot();
        var srcDir = Path.Combine(repoRoot, "src");
        if (!Directory.Exists(srcDir)) return hits;

        // Tolerate any short receiver-identifier shape (resolved,
        // resolvedOpts, opts, _options, cached, _cachedOptions,
        // sourceResolvedOpts, targetResolvedOpts, ...). The regex is
        // deliberately permissive so a future caller naming its local
        // `r` or `cfg` is still caught.
        var pattern = new System.Text.RegularExpressions.Regex(
            $@"(?<![\w])(?<recv>_?\w+)\s*\.\s*{System.Text.RegularExpressions.Regex.Escape(propName)}\b",
            System.Text.RegularExpressions.RegexOptions.Compiled);

        foreach (var file in EnumerateFiles(srcDir, "*.cs"))
        {
            // Skip the resolver itself (it always references the prop
            // name by construction) and the ResolvedLatticeOptions
            // declaration.
            var fileName = Path.GetFileName(file);
            if (string.Equals(fileName, "LatticeOptionsResolver.cs", StringComparison.OrdinalIgnoreCase)
                || string.Equals(fileName, "ResolvedLatticeOptions.cs", StringComparison.OrdinalIgnoreCase)
                || string.Equals(fileName, "LatticeOptions.cs", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            string[] lines;
            try { lines = File.ReadAllLines(file); }
            catch { continue; }

            for (int i = 0; i < lines.Length; i++)
            {
                var line = lines[i];
                // Skip comment-only lines so we don't surface
                // historical commentary in the failure message.
                var trimmed = line.TrimStart();
                if (trimmed.StartsWith("//", StringComparison.Ordinal)
                    || trimmed.StartsWith("///", StringComparison.Ordinal)
                    || trimmed.StartsWith("*", StringComparison.Ordinal))
                {
                    continue;
                }

                var m = pattern.Match(line);
                if (!m.Success) continue;
                var recv = m.Groups["recv"].Value;
                // Filter out call sites where the receiver is plainly
                // NOT a ResolvedLatticeOptions: anything that starts
                // with capital `Default` is a static constant fetch on
                // LatticeOptions; `LatticeOptions` itself is the type
                // reference, not a resolved instance.
                if (recv.StartsWith("Default", StringComparison.Ordinal)
                    || recv.Equals("LatticeOptions", StringComparison.Ordinal)
                    || recv.Equals("ResolvedLatticeOptions", StringComparison.Ordinal))
                {
                    continue;
                }
                var rel = Path.GetRelativePath(repoRoot, file).Replace('\\', '/');
                hits.Add($"{rel}:{i + 1}: {trimmed}");
                // First hit per file is enough to point the operator
                // at the right partial.
                break;
            }
        }
        return hits;
    }

    private static IEnumerable<string> EnumerateFiles(string root, string pattern)
    {
        if (!Directory.Exists(root)) yield break;
        foreach (var file in Directory.EnumerateFiles(root, pattern, SearchOption.AllDirectories))
        {
            var parts = file.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (parts.Any(p => p.Equals("bin", StringComparison.OrdinalIgnoreCase)
                            || p.Equals("obj", StringComparison.OrdinalIgnoreCase)
                            || p.Equals("node_modules", StringComparison.OrdinalIgnoreCase)))
                continue;
            yield return file;
        }
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "README.md"))
                && Directory.Exists(Path.Combine(dir.FullName, "docs"))
                && Directory.Exists(Path.Combine(dir.FullName, "src")))
            {
                return dir.FullName;
            }
            dir = dir.Parent;
        }
        throw new InvalidOperationException(
            "Could not find repository root from " + AppContext.BaseDirectory);
    }
}
