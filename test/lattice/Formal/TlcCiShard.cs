namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Assigns each TLC mutation case a test category that names its CI shard, so
/// <c>.github/workflows/test-shards.json</c> can split the mutation gates
/// across legs with a <c>TestCategory</c> filter.
/// <para>
/// WHY A CATEGORY AND NOT A NAME PREFIX. A test case's FullName carries its
/// arguments in parentheses, for example
/// <c>Each_property_fires_under_its_mutation_and_not_on_the_base(Replication, X)</c>.
/// The NUnit adapter's filter parser rejects a <c>FullyQualifiedName~</c>
/// value with an unbalanced parenthesis, so a filter cannot select on a
/// module's name inside the argument list. A category needs no such matching.
/// </para>
/// <para>
/// BALANCE, NOT CORRECTNESS. These groups only spread CPU-bound JVM runs over
/// runners; a case with no group runs in the <c>formal-tlc</c> shard, which
/// claims the rest of <see cref="TlcModelCheckTests"/>. A new module therefore
/// runs whether or not anybody adds it here. AtomicCommit* variants are split
/// into six groups using their manifest state counts as weights, so a newly
/// declared variant is routed automatically and the work remains balanced.
/// </para>
/// <para>
/// CONTROL COST. <see cref="TlcModelCheckTests"/> shares controls per process:
/// one full-bound control per module, target and options, and none at all when
/// the module's base run subsumes it and the base case runs in the same shard
/// (<see cref="SharesBaseShard"/>, <see cref="TlcBaseRunSubsumption"/>). So a
/// base case is co-located with its mutations where that is cheaper than the
/// full-bound controls it replaces (<see cref="CoLocatedBaseCategory"/>), and
/// left where it is where it is not.
/// </para>
/// </summary>
internal static class TlcCiShard
{
    /// <summary>The Replication EventualConvergence* temporal mutants.</summary>
    public const string Convergence = "TlcShardConvergence";

    /// <summary>The other Replication mutants, plus the mutants of every Wal* module, and the Replication, WalDurability and WalMove bases.</summary>
    public const string ReplicationWal = "TlcShardReplicationWal";

    /// <summary>The mutants of every ShardOwnership* module, and the bases of all but ShardOwnership itself.</summary>
    public const string ShardOwnership = "TlcShardShardOwnership";

    /// <summary>
    /// The mutants of the replication companion modules (every Replication*
    /// module but the main one: ReplicationReBootstrap, ReplicationLowWatermark,
    /// ReplicationCausalDelivery), their bases and their variant configurations.
    /// </summary>
    public const string ReBootstrap = "TlcShardReBootstrap";

    /// <summary>
    /// The mutants of every AtomicCommit* module, and the AtomicCommit and AtomicCommitCrossCluster bases. Its variant configurations
    /// are split into <see cref="AtomicVariantShards"/>: a variant has no control arm, so
    /// splitting them off separates their measured JVM work from mutation controls.
    /// </summary>
    public const string Atomic = "TlcShardAtomic";

    /// <summary>The first state-balanced group of AtomicCommit* variant configurations.</summary>
    public const string AtomicVariants1 = "TlcShardAtomicVariants1";

    /// <summary>The second state-balanced group of AtomicCommit* variant configurations.</summary>
    public const string AtomicVariants2 = "TlcShardAtomicVariants2";

    /// <summary>The third state-balanced group of AtomicCommit* variant configurations.</summary>
    public const string AtomicVariants3 = "TlcShardAtomicVariants3";

    /// <summary>The fourth state-balanced group of AtomicCommit* variant configurations.</summary>
    public const string AtomicVariants4 = "TlcShardAtomicVariants4";

    /// <summary>The fifth state-balanced group of AtomicCommit* variant configurations.</summary>
    public const string AtomicVariants5 = "TlcShardAtomicVariants5";

    /// <summary>The sixth state-balanced group of AtomicCommit* variant configurations.</summary>
    public const string AtomicVariants6 = "TlcShardAtomicVariants6";

    /// <summary>The six categories used to partition AtomicCommit* variants.</summary>
    public static IReadOnlyList<string> AtomicVariantShards { get; } =
        [AtomicVariants1, AtomicVariants2, AtomicVariants3, AtomicVariants4, AtomicVariants5, AtomicVariants6];

    /// <summary>The mutants of every Backup* module but BackupRetention, and the bases of all but BackupCapture and BackupRetention.</summary>
    public const string Backup = "TlcShardBackup";

    /// <summary>
    /// Every base, variant and mutation of <c>BackupRetention</c>, in a shard of its
    /// own, so its base serves its mutations' full-bound controls. Its base grew to
    /// 2,680,648 states with the republish under a per-id hold (#5418), and left in
    /// the catch-all it made each of its mutations launch a full-bound control of
    /// its own, which turned the backup shard into the CI critical path.
    /// </summary>
    public const string BackupRetention = "TlcShardBackupRetention";

    /// <summary>The measured materialised-view tail base, variant and mutation runs.</summary>
    public const string MaterialisedViews = "TlcShardMaterialisedViews";

    /// <summary>The focused physical replay-fence lifetime runs.</summary>
    public const string MaterialisedViewFence = "TlcShardMaterialisedViewFence";

    /// <summary>The focused three-partition terminal-horizon recovery runs.</summary>
    public const string MaterialisedViewHorizon = "TlcShardMaterialisedViewHorizon";

    /// <summary>The focused numeric aggregation and history revision runs.</summary>
    public const string MaterialisedViewAggregation = "TlcShardMaterialisedViewAggregation";

    /// <summary>The focused captured physical source and generation rebind runs.</summary>
    public const string MaterialisedViewRebind = "TlcShardMaterialisedViewRebind";

    /// <summary>The independent-clock ready replacement closure runs.</summary>
    public const string MaterialisedViewReadyClosure = "TlcShardMaterialisedViewReadyClosure";

    /// <summary>The coherent revision and expected-participant admission runs.</summary>
    public const string MaterialisedViewSourceAdmission = "TlcShardMaterialisedViewSourceAdmission";

    /// <summary>The physical-tail resolved cache ownership runs.</summary>
    public const string MaterialisedViewResolvedIdentity = "TlcShardMaterialisedViewResolvedIdentity";

    /// <summary>The bounded fresh joint-repair, retry and acknowledgement-closure runs.</summary>
    public const string MaterialisedViewJointRepair = "TlcShardMaterialisedViewJointRepair";

    /// <summary>The schema-independent numeric (Count/Sum) projection companion runs.</summary>
    public const string MaterialisedViewNumeric = "TlcShardMaterialisedViewNumeric";

    /// <summary>The anti-entropy base and first measured mutation group.</summary>
    public const string AntiEntropy = "TlcShardAntiEntropy";

    /// <summary>The anti-entropy alias variant and second measured mutation group.</summary>
    public const string AntiEntropy2 = "TlcShardAntiEntropy2";

    /// <summary>The third measured anti-entropy mutation group.</summary>
    public const string AntiEntropy3 = "TlcShardAntiEntropy3";

    /// <summary>The resize, reshard and shadow-cutover composition runs.</summary>
    public const string TreeLifecycle = "TlcShardTreeLifecycle";

    /// <summary>The shared alias routing and cutover interaction runs.</summary>
    public const string Aliasing = "TlcShardAliasing";

    /// <summary>The leaf split-gate and grain-turn wait-cycle runs.</summary>
    public const string SplitGate = "TlcShardSplitGate";

    /// <summary>The balanced scheduling group with ReplayPermits bases and PartialPrefix.</summary>
    public const string Scheduling = "TlcShardScheduling";

    /// <summary>The ReplayPermits mutations and the second balanced scheduling workload group.</summary>
    public const string SchedulingWorkloadB = "TlcShardSchedulingWorkloadB";

    /// <summary>The ReplayPermits TwoPartitions variant configuration, kept in its own shard.</summary>
    public const string SchedulingReplayTwoPartitions = "TlcShardSchedulingReplayTwoPartitions";

    /// <summary>The ReplicationShipping base and all paired mutations, retaining process-local controls together.</summary>
    public const string SchedulingShipping = "TlcShardSchedulingShipping";

    /// <summary>The full ReplicationShipping variant configurations, currently FourRecords.</summary>
    public const string SchedulingShippingVariants = "TlcShardSchedulingShippingVariants";

    /// <summary>The BPlusCascade base model and every BPlusCascade mutant, co-located so the base serves their full-bound controls.</summary>
    public const string BPlusCascade = "TlcShardBPlusCascade";

    /// <summary>
    /// Every base, variant and mutation of the Schema* modules under
    /// <c>spec/schema/</c>, co-located so each base serves its mutations'
    /// full-bound controls. Routed by name prefix into a dedicated shard so a
    /// schema module never reaches the catch-all, and a new Schema* module joins
    /// it with no routing change.
    /// <para>
    /// Split by module, never by mutation, once the measured item would exceed
    /// the largest other TLC item: give the module an exact-name branch in
    /// <see cref="SchemaCategory"/> ahead of the prefix rule and a shard of its
    /// own, so its base stays co-located with its mutations. The per-module
    /// measured costs live in the shard's <c>test-shards.json</c> note.
    /// </para>
    /// </summary>
    public const string Schema = "TlcShardSchema";

    /// <summary>
    /// Every base, variant and mutation of <c>SchemaViewComposition</c> under
    /// <c>spec/schema-view/</c>, the schema/view composition model (#5233). It is
    /// named <c>Schema*</c> but lives in its own directory and measured shard, so
    /// it neither rides the <see cref="Schema"/> prefix route nor lengthens that
    /// shard's critical path.
    /// </summary>
    public const string SchemaView = "TlcShardSchemaView";

    /// <summary>
    /// The prefix of the per-module category every TLC base, variant and
    /// mutation case carries beside its shard category (contract C2 of #5148),
    /// so <c>TestCategory=TlcModule_&lt;Module&gt;</c> selects exactly one
    /// module's cases. The family is disjoint from the <c>TlcShard*</c> routing
    /// family and no <c>test-shards.json</c> shard claims it, so it moves no
    /// case between shards. <see cref="TlcCaseCensus"/> lists the cases per
    /// module.
    /// </summary>
    public const string ModuleCategoryPrefix = "TlcModule_";

    /// <summary>Every category <see cref="Of"/>, <see cref="OfModule"/> and <see cref="OfVariant"/> can return.</summary>
    public static IReadOnlyList<string> All { get; } =
        [Convergence, ReplicationWal, ShardOwnership, ReBootstrap, Atomic, .. AtomicVariantShards, Backup, BackupRetention,
         BPlusCascade,
         MaterialisedViews, MaterialisedViewFence, MaterialisedViewHorizon,
         MaterialisedViewAggregation, MaterialisedViewRebind,
         MaterialisedViewReadyClosure, MaterialisedViewSourceAdmission,
         MaterialisedViewResolvedIdentity, MaterialisedViewJointRepair, MaterialisedViewNumeric,
         AntiEntropy, AntiEntropy2, AntiEntropy3, TreeLifecycle, Aliasing, SplitGate,
         Scheduling, SchedulingWorkloadB, SchedulingReplayTwoPartitions, SchedulingShipping, SchedulingShippingVariants,
         Schema, SchemaView];

    private static readonly Lazy<IReadOnlyDictionary<(string Module, string Variant), string>> AtomicVariantCategories =
        new(CreateAtomicVariantCategories, LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>
    /// The shard category of <paramref name="mutation"/> of
    /// <paramref name="module"/>, or <see langword="null"/> to leave it in the
    /// <c>formal-tlc</c> shard.
    /// </summary>
    public static string? Of(SpecModule module, SpecMutation mutation)
    {
        ArgumentNullException.ThrowIfNull(module);
        ArgumentNullException.ThrowIfNull(mutation);

        var name = module.Name;
        if (SchedulingCategory(module, mutation: mutation.Name) is { } schedulingCategory)
        {
            return schedulingCategory;
        }
        if (SchemaCategory(name) is { } schemaCategory)
        {
            return schemaCategory;
        }
        if (string.Equals(name, "AntiEntropy", StringComparison.Ordinal))
        {
            return mutation.Name switch
            {
                "CooldownNeverHalfOpens" or "FallbackDropsTombstones" or "DeleteReplayedAsSet"
                    or "ReplayBelowCursor" or "ReplayReadsRetiredWal" or "EchoRecordedAsApplied"
                    or "OwnOriginApplied" or "SwapInvisibleToResolve" => AntiEntropy2,
                "WindowNeverRolls" or "PeerWriteNotShipped" or "DigestIgnoresDivergence"
                    or "AbortRollsBudgetWindow" or "ProbeAdvancesCursor" or "UnavailableClosesBreaker"
                    or "StaleTopologyWalk" => AntiEntropy3,
                _ => AntiEntropy,
            };
        }
        if (LifecycleCategory(name) is { } lifecycleCategory)
        {
            return lifecycleCategory;
        }
        if (string.Equals(name, "Replication", StringComparison.Ordinal))
        {
            return mutation.Name.StartsWith("EventualConvergence", StringComparison.Ordinal)
                ? Convergence
                : ReplicationWal;
        }

        if (IsReplicationCompanion(name))
        {
            return ReBootstrap;
        }

        if (name.StartsWith("Wal", StringComparison.Ordinal))
        {
            return ReplicationWal;
        }

        if (name.StartsWith("ShardOwnership", StringComparison.Ordinal))
        {
            return ShardOwnership;
        }

        if (string.Equals(name, "BackupRetention", StringComparison.Ordinal))
        {
            return BackupRetention;
        }

        if (name.StartsWith("Backup", StringComparison.Ordinal))
        {
            return Backup;
        }

        if (string.Equals(name, "BPlusCascade", StringComparison.Ordinal))
        {
            return BPlusCascade;
        }

        return name.StartsWith("AtomicCommit", StringComparison.Ordinal) ? Atomic : null;
    }

    /// <summary>The shard category of the base-model case, or <see langword="null"/> to leave it in the catch-all.</summary>
    public static string? OfModule(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);
        return SchedulingCategory(module) ?? SchemaCategory(module.Name) ?? LifecycleCategory(module.Name) ?? CoLocatedBaseCategory(module);
    }

    /// <summary>
    /// Whether the base case of <paramref name="module"/> runs in the same CI
    /// shard, and so the same test process, as <paramref name="mutation"/>:
    /// the condition under which <see cref="TlcModelCheckTests"/> may serve the
    /// mutation's full-bound control from the base run without launching the
    /// base a second time. Two cases left in the catch-all share it.
    /// </summary>
    public static bool SharesBaseShard(SpecModule module, SpecMutation mutation) =>
        string.Equals(OfModule(module), Of(module, mutation), StringComparison.Ordinal);

    /// <summary>
    /// The mutation shard a base case is moved into so that its subsumed
    /// full-bound controls are served by it (#5161), or <see langword="null"/>
    /// to leave it where it was. Listed by name, never by prefix, because each
    /// move is a measured decision: a new module's base stays put until its
    /// cost is known.
    /// <para>
    /// A base explores the same state space as each full-bound control it
    /// replaces and checks their properties together, so moving it replaces
    /// several launches with one. The risk is the shard's critical path: one
    /// long base case cannot be parallelised the way several controls can. So a
    /// base moves into the shard holding its module's mutations only when its
    /// measured case duration in CI run 40860831824 (an upper bound, since it
    /// includes waiting for a TLC slot) is at most half that shard's measured
    /// duration. That excludes ReplayPermits (605 s against 684 s), BackupCapture
    /// (249 s against 224 s) and ShardOwnership (265 s against 521 s), whose
    /// controls keep running. Replication's mutations are split: its base joins
    /// the replication-wal group, and its convergence mutations keep launching
    /// their controls, as do the split AntiEntropy groups whose base stays
    /// where it is. BPlusCascade moved later (#5166): in CI run 40909619830 its
    /// base took 10 s against 227 s for its thirteen mutations, then split over
    /// two shards, so its base and mutations now share one shard. WalPinRetirement
    /// joined at creation (#5349): its base measured 24 s on two local workers,
    /// far below half the replication-wal shard's 14-minute item. BackupIdClaim
    /// joined at creation (#5627): its base is 174 states, under a second.
    /// BackupRetention moved into a shard of its own with its mutations and
    /// variant (#5352): its base is the shard's longest case, and serving its
    /// full-bound controls removes a base-sized run per mutation.
    /// </para>
    /// </summary>
    private static string? CoLocatedBaseCategory(SpecModule module) => module.Name switch
    {
        "AtomicCommit" or "AtomicCommitCrossCluster" => Atomic,
        "BackupCutover" or "BackupIdClaim" or "BackupIncremental" or "BackupProvenance" or "BackupRestore" => Backup,
        "BackupRetention" => BackupRetention,
        "BPlusCascade" => BPlusCascade,
        "ReplicationCausalDelivery" or "ReplicationLowWatermark" or "ReplicationReBootstrap" => ReBootstrap,
        "ShardOwnershipCrdt" or "ShardOwnershipCutover" or "ShardOwnershipFold" or "ShardOwnershipRetention" => ShardOwnership,
        "Replication" or "WalDurability" or "WalMove" or "WalPinRetirement" => ReplicationWal,
        _ => null,
    };

    /// <summary>
    /// The shard category of variant configuration <paramref name="variant"/>
    /// of <paramref name="module"/>, or <see langword="null"/> to leave it in
    /// the <c>formal-tlc</c> shard.
    /// </summary>
    public static string? OfVariant(SpecModule module, string variant)
    {
        ArgumentNullException.ThrowIfNull(module);
        ArgumentNullException.ThrowIfNull(variant);

        if (SchedulingCategory(module, variant) is { } schedulingCategory)
        {
            return schedulingCategory;
        }

        if (SchemaCategory(module.Name) is { } schemaCategory)
        {
            return schemaCategory;
        }

        if (string.Equals(module.Name, "AntiEntropy", StringComparison.Ordinal)
            && string.Equals(variant, "Alias", StringComparison.Ordinal))
        {
            return AntiEntropy2;
        }

        if (string.Equals(module.Name, "BackupRetention", StringComparison.Ordinal))
        {
            return BackupRetention;
        }

        if (LifecycleCategory(module.Name) is { } lifecycleCategory)
        {
            return lifecycleCategory;
        }

        if (IsReplicationCompanion(module.Name))
        {
            return ReBootstrap;
        }

        if (!module.Name.StartsWith("AtomicCommit", StringComparison.Ordinal))
        {
            return null;
        }

        return AtomicVariantCategories.Value.TryGetValue((module.Name, variant), out var category)
            ? category
            : AtomicVariants1;
    }

    private static IReadOnlyDictionary<(string Module, string Variant), string> CreateAtomicVariantCategories()
    {
        var assignments = new Dictionary<(string Module, string Variant), string>();
        var loads = new long[AtomicVariantShards.Count];
        var variants = SpecModuleCatalogue.Repository()
            .Where(module => module.Name.StartsWith("AtomicCommit", StringComparison.Ordinal))
            .SelectMany(module => module.Manifest.Variants.Select(variant =>
                (Module: module.Name, Variant: variant.Key, States: variant.Value)))
            .OrderByDescending(variant => variant.States)
            .ThenBy(variant => variant.Module, StringComparer.Ordinal)
            .ThenBy(variant => variant.Variant, StringComparer.Ordinal);

        foreach (var variant in variants)
        {
            var shard = Array.IndexOf(loads, loads.Min());
            assignments.Add((variant.Module, variant.Variant), AtomicVariantShards[shard]);
            loads[shard] += variant.States;
        }

        return assignments;
    }

    private static bool IsReplicationCompanion(string name) =>
        name.StartsWith("Replication", StringComparison.Ordinal)
        && !string.Equals(name, "Replication", StringComparison.Ordinal);

    private static string? SchedulingCategory(SpecModule module, string? variant = null, string? mutation = null)
    {
        if (!string.Equals(Path.GetFileName(module.Directory), "scheduling", StringComparison.Ordinal))
            return null;

        if (string.Equals(module.Name, "ReplicationShipping", StringComparison.Ordinal))
            return variant is null ? SchedulingShipping : SchedulingShippingVariants;

        if (string.Equals(module.Name, "ReplayPermits", StringComparison.Ordinal))
        {
            if (string.Equals(variant, "TwoPartitions", StringComparison.Ordinal))
                return SchedulingReplayTwoPartitions;
            return mutation is null ? Scheduling : SchedulingWorkloadB;
        }

        return module.Name is "WalGcStarvationDrive" or "SagaReadProgress" or "TagIndexReconcile"
            ? SchedulingWorkloadB
            : Scheduling;
    }

    private static string? SchemaCategory(string name) =>
        string.Equals(name, "SchemaViewComposition", StringComparison.Ordinal) ? SchemaView
        : name.StartsWith("Schema", StringComparison.Ordinal) ? Schema
        : null;

    private static string? LifecycleCategory(string name) =>
        name == "MaterialisedViewFence" ? MaterialisedViewFence
        : name == "MaterialisedViewHorizon" ? MaterialisedViewHorizon
        : name == "MaterialisedViewAggregation" ? MaterialisedViewAggregation
        : name == "MaterialisedViewRebind" ? MaterialisedViewRebind
        : name == "MaterialisedViewReadyClosure" ? MaterialisedViewReadyClosure
        : name == "MaterialisedViewSourceAdmission" ? MaterialisedViewSourceAdmission
        : name == "MaterialisedViewResolvedIdentity" ? MaterialisedViewResolvedIdentity
        : string.Equals(name, "MaterialisedViewJointRepair", StringComparison.Ordinal) ? MaterialisedViewJointRepair
        : string.Equals(name, "MaterialisedViewNumeric", StringComparison.Ordinal) ? MaterialisedViewNumeric
        : name.StartsWith("MaterialisedView", StringComparison.Ordinal) ? MaterialisedViews
        : name.StartsWith("AntiEntropy", StringComparison.Ordinal) ? AntiEntropy
        : name.StartsWith("TreeLifecycle", StringComparison.Ordinal) ? TreeLifecycle
        : name.StartsWith("Alias", StringComparison.Ordinal) ? Aliasing
        : string.Equals(name, "SplitGateCycles", StringComparison.Ordinal) ? SplitGate
        : null;

    /// <summary>
    /// The per-module category of every TLC case of <paramref name="module"/>:
    /// <see cref="ModuleCategoryPrefix"/> followed by the module name.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The module name is not a plain identifier, so neither a VSTest filter nor
    /// the CI matrix planner could select the category.
    /// </exception>
    public static string ModuleCategory(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var name = module.Name;
        if (string.IsNullOrEmpty(name) || !name.All(c => char.IsAsciiLetterOrDigit(c) || c == '_'))
        {
            throw new InvalidOperationException(
                $"Module '{name}' cannot carry a {ModuleCategoryPrefix} category: a test filter selects only plain identifiers.");
        }

        return ModuleCategoryPrefix + name;
    }

    /// <summary>
    /// Adds <see cref="OfModule"/>'s category to a base case whose arguments are
    /// <c>(SpecModule)</c>, <see cref="Of"/>'s to a mutation case whose
    /// arguments are <c>(SpecModule, SpecMutation)</c>, and
    /// <see cref="OfVariant"/>'s to a variant case whose arguments are
    /// <c>(SpecModule, string)</c>. Every case whose first argument is a
    /// <see cref="SpecModule"/> also gets its <see cref="ModuleCategory"/>.
    /// </summary>
    public static TestCaseData Tag(TestCaseData data)
    {
        ArgumentNullException.ThrowIfNull(data);

        if (data.Arguments is [SpecModule owner, ..])
        {
            data.SetCategory(ModuleCategory(owner));
        }

        var category = data.Arguments switch
        {
            [SpecModule module] => OfModule(module),
            [SpecModule module, SpecMutation mutation, ..] => Of(module, mutation),
            [SpecModule module, string variant] => OfVariant(module, variant),
            _ => null,
        };

        if (category is not null)
        {
            data.SetCategory(category);
        }

        return data;
    }
}
