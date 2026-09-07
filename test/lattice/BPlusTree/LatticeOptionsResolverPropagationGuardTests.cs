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
            // on the WAL hot path. WalPartitions is the exception -
            // it is pinned per-tree in TreeRegistryEntry.WalPartitions
            // and surfaced through the resolver so the foreground
            // commit-log writer and the activation-time materialiser
            // always agree on the partition fan-out shape for the
            // lifetime of the tree; the propagation guard exercises
            // its pass-through behaviour when the registry entry
            // carries a null pin.
            "WalMaxBatchEntries",
            "WalMaxBatchBytes",
            "WalMaxPendingBatches",
            "WalFlushTimeout",
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
            "SnapshotBaselineTtl",
            "PrefetchEntriesScan",
            // WAL saturation back-pressure surface: the silo-scoped
            // WalSaturationSampler reads these directly from
            // IOptionsMonitor.Get(string.Empty) on every tick. They are
            // process-wide / silo-wide cadence + threshold knobs, not
            // per-tree gates - a single sampler classifies every tree
            // on a shared interval, so a per-tree override would not
            // change its behaviour. Bypassing the resolver is therefore
            // intentional; the sampler never goes through the per-call
            // ResolvedLatticeOptions path.
            "WalSaturationSampleInterval",
            "WalSaturationThrottledRatio",
            "WalSaturationDispatchTimeoutThreshold",
            "WalSaturationProviderFailureRateThreshold",
            "WalSaturationRecoveryWindow",
            "WalSaturationFlushLatencyThreshold",
            "WalSaturationFlushLatencySampleWindows",
            "WalSaturationMaterialiserLagThreshold",
            "WalSaturationMaterialiserLagSampleWindows",
            // Durable pin-latency input (issue #2015): read alongside the rest
            // of the saturation family from the silo-wide unkeyed options in
            // WalSaturationSampler.SampleOnceAsync, and (for the threshold)
            // in LeafCursorReporter.ResolvePinLatencyThresholdMs.
            "WalSaturationMaterialiserPinLatencyThreshold",
            "WalSaturationMaterialiserPinLatencySampleWindows",
            // Durable leaf-materialiser pin store knobs: the pin grain and the
            // LeafCursorReporter read these directly via
            // IOptionsMonitor.Get(...) (the flush cadence is a silo-wide grain
            // timer interval; the shard count is a cluster-wide structural
            // fan-out, and the bucket count is a cluster-wide durable-state
            // layout that every activation of a shard must agree on). None is a
            // per-tree gate routed through the ResolvedLatticeOptions hot path.
            "WalMaterialiserPinShards",
            "WalMaterialiserPinBuckets",
            "WalMaterialiserPinFlushIntervalMs",
            // WAL replay throttles (issue #1030): the per-silo concurrent-leaf-
            // replay ceiling is a process-wide semaphore bound, and the per-turn
            // replay record budget is read directly off the leaf's resolved
            // options inside the activation hook. Neither flows through a per-
            // tree ResolvedLatticeOptions gate.
            "WalMaterialiserMaxConcurrentReplays",
            "WalReplayMaxRecordsPerTurn",
            // Distributed-lock lease knobs (issue #1608): LatticeLockGrain reads
            // these directly from IOptionsMonitor<LatticeOptions>.CurrentValue when
            // it clamps a requested lease duration. The lock grain is keyed by lock
            // name, not by tree id, and never flows through the per-tree
            // ResolvedLatticeOptions hot path, so bypassing the resolver is
            // intentional.
            "DefaultLockLeaseDuration",
            "MaxLockLeaseDuration",

            // Atomic-action (saga / TCC) coordinator knobs (issue #1609):
            // AtomicActionGrain reads these directly from
            // IOptionsMonitor<LatticeOptions>.CurrentValue. The coordinator is keyed
            // by operation id, not by tree id, and never flows through the per-tree
            // ResolvedLatticeOptions hot path, so bypassing the resolver is
            // intentional.
            "AtomicActionRetention",
            "MaxAtomicActionSteps",
            "MaxAtomicActionArgsBytes",

            // Cluster storage-usage roll-up tree fan-out bound (issue #1728):
            // LatticeAdminGrain reads this directly from
            // IOptionsMonitor<LatticeOptions>.Get(Options.DefaultName). The admin
            // grain is a cluster singleton that bounds how many *trees* a roll-up
            // samples at once, so it is not keyed by tree id and has no per-tree
            // meaning - a "which tree's value wins" question with no sensible
            // answer. The inner, genuinely per-tree half of the same fan-out
            // (MaxConcurrentStorageUsageSurfaces) does flow through the resolver,
            // exactly like MaxConcurrentSnapshotCaptures.
            "MaxConcurrentStorageUsageTrees",

            // StorageUsageRollupBudget is cluster-scoped for the same reason:
            // it bounds the wall-clock time one cluster-wide roll-up may spend
            // sampling trees, and that roll-up is driven by the same admin
            // singleton, so there is no tree whose override could sensibly win.
            "StorageUsageRollupBudget",

            // Leaf-cache pre-warm knobs (issue #332): ShardRootGrain resolves
            // these through the dedicated synchronous
            // LatticeOptionsResolver.GetLeafAccessTrackingSettings(treeId) seam,
            // which reads IOptionsMonitor.Get(treeId) directly. They are
            // deliberately kept off the ResolvedLatticeOptions path because the
            // first consumer is the read hot path, which cannot await the
            // registry round trip ResolveAsync performs; and because they are
            // silo-local operational tuning (how much startup work this silo
            // does) rather than tree-shape configuration that every replica of
            // the tree must agree on.
            "LeafCachePreWarmCount",
            "LeafAccessModelFlushIntervalMs",

            // Snapshot baseline fold fan-out (issue #1961): ShardRootGrain
            // resolves this through the dedicated synchronous
            // LatticeOptionsResolver.GetSnapshotBaselineFoldConcurrency(treeId)
            // seam, which reads IOptionsMonitor.Get(treeId) directly. It is
            // deliberately kept off the ResolvedLatticeOptions path because the
            // consumer is CaptureSnapshotBaselineAsync, a scan-page-bounded
            // entry point that must not await before its stall budget is armed
            // (issues #1992, #2002) - the registry round trip ResolveAsync
            // performs is exactly the await that would leave the ceiling
            // unarmed. It is also pure dispatch scheduling: the captured
            // baseline is byte-identical under any value, so no replica of the
            // tree has to agree on it.
            "MaxConcurrentSnapshotBaselineFolds",

            // --- Audited under issue #2182 -------------------------------
            // Before #2182 this guard silently skipped every property whose
            // type it could not sentinel (nullable value types, Func<,>,
            // interfaces), so the entries below were never checked and their
            // bypass status was never a decision anybody recorded. Making the
            // guard total surfaced all eleven at once. Each was traced to its
            // consumer and confirmed to read the option directly from
            // IOptionsMonitor rather than off a ResolvedLatticeOptions
            // instance, so none is a live propagation defect - but the bypass
            // is now explicit rather than an accident of the sentinel table.

            // Queue capacity: LatticeQueueGrain reads
            // optionsMonitor.Get(_queueName).QueueCapacity at enqueue time.
            // The queue grain is keyed by queue name, not by tree id, so it
            // never flows through the per-tree ResolvedLatticeOptions path.
            "QueueCapacity",

            // Write-size bounds: LatticeGrain.ValidateWriteSize reads them off
            // its own `Options => optionsMonitor.Get(TreeId)` accessor at the
            // public write boundary, which is a synchronous guard that must not
            // await the registry round trip ResolveAsync performs.
            "MaxKeyLength",
            "MaxValueSizeBytes",

            // Per-tree admission caps and advisory ceilings: read through the
            // same synchronous LatticeGrain.Options accessor in
            // EnforceAdmissionControl, and surfaced for reporting by
            // LatticeStorageUsageGrain from its own IOptionsMonitor read.
            "MaxLiveKeys",
            "MaxEstimatedBytes",
            "AdmissionAdvisoryLiveKeys",
            "AdmissionAdvisoryBytes",

            // Cluster-wide in-flight auto-split ceiling: HotShardMonitorGrain
            // reads it off its own IOptionsMonitor-backed Options accessor.
            // Cluster-scoped like MaxConcurrentStorageUsageTrees - it bounds
            // the aggregate across all trees, so "which tree's override wins"
            // has no sensible answer.
            "MaxClusterConcurrentAutoSplits",

            // Legacy per-tree WAL provider factory: the resolver consumes this
            // itself inside ResolveWalProvider, via its own
            // optionsMonitor.Get(treeId).WalStorageProvider read, as the
            // fallback for trees with no placement pin. It is a delegate, so
            // it is also not sentinel-testable.
            "WalStorageProvider",

            // WAL wall-clock retention: LatticeWalGc reads it from its own
            // optionsMonitor.Get(treeName) snapshot (the local named `resolved`
            // there is a plain LatticeOptions, not a ResolvedLatticeOptions),
            // and ViewMaintainerGrain reads it via latticeOptions.Get(treeId).
            "WalRetention",

            // Idempotency retry policy: LatticeGrain.Idempotency reads
            // Options.RetryPolicy off the same synchronous monitor-backed
            // accessor. It is an interface instance, so it is also not
            // sentinel-testable.
            "RetryPolicy",
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
            if (IntentionallyBypassedProperties.Contains(prop.Name))
            {
                continue;
            }

            if (!TryPickSentinel(prop, out var sentinel))
            {
                // The guard cannot synthesise a distinguishable value for this
                // property's type, so it cannot prove the resolver copies it.
                // That is a FAILURE, not a skip: silently ignoring a property
                // the guard cannot check is the very hole issue #2182 closed.
                // Resolve it by teaching TryPickSentinel the type, or by adding
                // the property to IntentionallyBypassedProperties with a comment
                // naming the consumer that reads it off IOptionsMonitor instead.
                failures.Add(
                    $"  LatticeOptions.{prop.Name} ({prop.PropertyType}) cannot be sentinel-tested by this guard,\n" +
                    "    so its propagation through LatticeOptionsResolver is UNVERIFIED. Either add a sentinel\n" +
                    "    branch for this type to TryPickSentinel, or add the property to\n" +
                    "    IntentionallyBypassedProperties with a comment naming its direct\n" +
                    "    IOptionsMonitor consumer.");
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

    /// <summary>
    /// Keeps <see cref="IntentionallyBypassedProperties"/> honest in the
    /// "still bypassed" direction: a property listed there must genuinely NOT
    /// be propagated by the resolver. If someone adds a property to the
    /// resolver's copy block but leaves it on the allow-list, the main guard
    /// would skip it forever - reintroducing exactly the silent hole issue
    /// #2182 closed, only one level up.
    /// </summary>
    [Test]
    public async Task Intentionally_bypassed_properties_are_not_silently_propagated()
    {
        var failures = new List<string>();

        var bypassed = typeof(LatticeOptions)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.GetSetMethod(nonPublic: false) is not null)
            .Where(p => IntentionallyBypassedProperties.Contains(p.Name));

        foreach (var prop in bypassed)
        {
            if (!TryPickSentinel(prop, out var sentinel))
            {
                // Not observable either way; the allow-list comment is the
                // only available evidence and the main guard already requires
                // one to exist.
                continue;
            }

            var baseOptions = new LatticeOptions();
            prop.SetValue(baseOptions, sentinel);
            var resolver = BuildResolverFor(baseOptions);

            var resolved = await resolver.ResolveAsync("user-tree-bypass-audit");

            if (Equals(prop.GetValue(resolved), sentinel))
            {
                failures.Add(
                    $"  LatticeOptions.{prop.Name} is listed in IntentionallyBypassedProperties, " +
                    "but ResolveAsync does propagate it.");
            }
        }

        Assert.That(failures, Is.Empty,
            "IntentionallyBypassedProperties is stale. The resolver now propagates the property/properties " +
            "below, so the allow-list entry suppresses a check that would otherwise hold. Remove the entry " +
            "so the main propagation guard covers it again.\n" +
            string.Join("\n", failures));
    }

    /// <summary>
    /// Keeps <see cref="IntentionallyBypassedProperties"/> honest in the
    /// "still exists" direction: every allow-list entry must name a real
    /// public settable <see cref="LatticeOptions"/> property. A dead entry
    /// left behind by a rename is a latent trap - a future property reusing
    /// that name would be silently exempted from the guard without anyone
    /// deciding so.
    /// </summary>
    [Test]
    public void Intentionally_bypassed_properties_all_name_a_real_option()
    {
        var actual = typeof(LatticeOptions)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.GetSetMethod(nonPublic: false) is not null)
            .Select(p => p.Name)
            .ToHashSet(StringComparer.Ordinal);

        var dead = IntentionallyBypassedProperties
            .Where(name => !actual.Contains(name))
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToList();

        Assert.That(dead, Is.Empty,
            "IntentionallyBypassedProperties names one or more properties that no longer exist on " +
            "LatticeOptions. Remove the stale entry: if a future option reuses the name it would be " +
            "exempted from the propagation guard silently.");
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
        else
        {
            // Func<,>, interfaces, and any other reference type we cannot
            // synthesise a comparable value for. The caller fails the build.
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
