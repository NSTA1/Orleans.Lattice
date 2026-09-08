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
/// <para>
/// Issue #2182 closed the guard's own version of the same hazard. It
/// previously skipped, silently, every property whose type it could not
/// synthesise a sentinel for - which was every nullable value type, every
/// enum, and every delegate or interface option. Eleven properties were
/// therefore never audited at all, and a dropped copy of a nullable option
/// (<see cref="LatticeOptions.MaxCacheValueBytes"/>, say) passed this guard
/// unnoticed. The guard is now total: nullable value types are sentinelled
/// through their underlying type, enums pick a non-default member, and a
/// type that still cannot be sentinelled is a FAILURE demanding an explicit
/// decision rather than a skip nobody sees.
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

    private sealed record TransformExpectation(Func<object?, object?> Expected);

    [Test]
    public async Task Every_ResolvedLatticeOptions_property_is_propagated_from_baseOptions()
    {
        var failures = new List<string>();

        var latticeOptionProps = typeof(LatticeOptions)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.GetSetMethod(nonPublic: false) is not null)
            .ToList();

        foreach (var prop in latticeOptionProps)
        {
            // ResolvedLatticeOptions inherits from LatticeOptions, so every
            // LatticeOptions property is reachable on the resolved instance
            // under the same name whether or not the resolver assigned it.
            // That inheritance is precisely why an omission is silent, and
            // why this guard asserts on the observed VALUE rather than on
            // member presence.
            if (!TryPickSentinel(prop, out var sentinel))
            {
                // The guard cannot synthesise a distinguishable value for this
                // property's type, so it cannot prove the resolver copies it.
                // That is a FAILURE, not a skip: silently ignoring a property
                // the guard cannot check is the very hole issue #2182 closed.
                // Resolve it by teaching TryPickSentinel the type so the guard
                // can synthesise a distinguishable value for it. Every
                // configurable option must round-trip; there is no bypass list.
                failures.Add(
                    $"  LatticeOptions.{prop.Name} ({prop.PropertyType}) cannot be sentinel-tested by this guard,\n" +
                    "    so its propagation through LatticeOptionsResolver is UNVERIFIED. Add a sentinel\n" +
                    "    branch for this type to TryPickSentinel so the guard can prove the resolver\n" +
                    "    copies it. Every configurable option must round-trip; there is no bypass list.");
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
            "configured value. The resolver copies every configurable LatticeOptions property " +
            "onto ResolvedLatticeOptions by reflection (CopyConfigurableBaseOptionsFrom); a " +
            "failure here means that copy was bypassed or a derived override clobbered the " +
            "configured value. Restore the round-trip in ResolveAsync.\n" +
            string.Join("\n", failures));
    }

    /// <summary>
    /// Explicit, behaviour-named pin for the storage-usage poll interval:
    /// the resolver previously dropped <see cref="LatticeOptions.StorageUsagePollInterval"/>
    /// on the floor, so the per-silo storage-usage gauge poller observed
    /// the inherited default instead of the operator's configured cadence.
    /// The reflective guard above catches this too, but this case names the
    /// regression directly so a failure points straight at the resolver's
    /// copy block rather than at a generic property-name mismatch.
    /// </summary>
    [Test]
    public async Task ResolveAsync_propagates_StorageUsagePollInterval()
    {
        var configured = TimeSpan.FromSeconds(42);
        var baseOptions = new LatticeOptions { StorageUsagePollInterval = configured };
        var resolver = BuildResolverFor(baseOptions);

        var resolved = await resolver.ResolveAsync("user-tree-storage-poll");

        Assert.That(resolved.StorageUsagePollInterval, Is.EqualTo(configured));
    }

    /// <summary>
    /// Explicit, behaviour-named pin for the deep storage-usage poll interval:
    /// the resolver must carry <see cref="LatticeOptions.StorageUsageDeepPollInterval"/>
    /// through so the per-silo poller's deep loop observes the operator's
    /// configured cadence rather than the inherited default.
    /// </summary>
    [Test]
    public async Task ResolveAsync_propagates_StorageUsageDeepPollInterval()
    {
        var configured = TimeSpan.FromSeconds(90);
        var baseOptions = new LatticeOptions { StorageUsageDeepPollInterval = configured };
        var resolver = BuildResolverFor(baseOptions);

        var resolved = await resolver.ResolveAsync("user-tree-storage-deep-poll");

        Assert.That(resolved.StorageUsageDeepPollInterval, Is.EqualTo(configured));
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
    /// Pick a sentinel value guaranteed to differ from the compile-time
    /// default the property carries, so that observing the default on the
    /// resolved instance is unambiguous proof the resolver never copied it.
    /// <para>
    /// Returns <see langword="false"/> when the property's type is one this
    /// guard cannot sentinel. That is deliberately NOT a silent skip: the
    /// caller turns it into a failure, because a silently skipped property is
    /// exactly the hole this guard exists to close. Nullable value types are
    /// unwrapped and sentinelled through their underlying type, which is what
    /// lets the guard audit the <c>int?</c> / <c>long?</c> / <c>TimeSpan?</c>
    /// options (issue #2182) it previously ignored.
    /// </para>
    /// </summary>
    private static bool TryPickSentinel(PropertyInfo prop, out object? sentinel)
    {
        sentinel = null;
        var declared = prop.PropertyType;
        var t = Nullable.GetUnderlyingType(declared) ?? declared;
        var compiledDefault = prop.GetValue(new LatticeOptions());

        if (t == typeof(int))
        {
            // Prime well above every documented floor in the system so
            // the compaction-floor clamps are no-ops.
            sentinel = 12289;
        }
        else if (t == typeof(long))
        {
            sentinel = 12289L;
        }
        else if (t == typeof(bool))
        {
            // Flip vs the existing default.
            sentinel = !(compiledDefault as bool? ?? false);
        }
        else if (t == typeof(TimeSpan))
        {
            sentinel = TimeSpan.FromMinutes(13) + TimeSpan.FromMilliseconds(37);
        }
        else if (t == typeof(double))
        {
            sentinel = 0.314159;
        }
        else if (t == typeof(string))
        {
            sentinel = "propagation-guard-sentinel";
        }
        else if (t.IsEnum)
        {
            // Any declared member other than the compiled default.
            sentinel = Enum.GetValues(t)
                .Cast<object>()
                .FirstOrDefault(v => !Equals(v, compiledDefault));
            if (sentinel is null)
            {
                // Single-member enum: no value can differ from the default,
                // so propagation is unobservable. Treat as unsupported rather
                // than asserting something vacuously true.
                return false;
            }
        }
        else if (t.IsInterface || typeof(Delegate).IsAssignableFrom(t))
        {
            // Reference-typed options: an interface policy
            // (ILatticeRetryPolicy) or a factory delegate
            // (Func<string, IWalStorageProvider>). The guard cannot
            // synthesise a "value" for these, but it can mint a distinct
            // instance and prove the resolver copies the *reference*
            // through. NSubstitute produces a proxy for both interfaces and
            // delegate types; reference equality is all this guard needs,
            // and the compiled default (null) is trivially distinct from it.
            sentinel = Substitute.For(new[] { t }, null);
        }
        else
        {
            // Any other reference type we cannot synthesise a comparable
            // value for. The caller fails the build rather than skipping:
            // an unaudited property is the exact hole issue #2182 closed.
            return false;
        }

        // A sentinel that happens to equal the compiled default proves
        // nothing - the property would "pass" even if the resolver dropped
        // it. Perturb until it differs.
        if (Equals(sentinel, compiledDefault))
        {
            sentinel = sentinel switch
            {
                int i => i + 1,
                long l => l + 1L,
                double d => d + 0.5d,
                TimeSpan ts => ts + TimeSpan.FromMinutes(1),
                string s => s + "-alt",
                _ => sentinel,
            };
            if (Equals(sentinel, compiledDefault))
            {
                return false;
            }
        }

        return true;
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
