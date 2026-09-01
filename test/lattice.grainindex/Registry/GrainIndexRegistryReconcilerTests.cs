using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.GrainIndex.Registry;

namespace Orleans.Lattice.GrainIndex.Tests.Registry;

/// <summary>
/// Covers <see cref="GrainIndexRegistryReconciler"/>: all four reconciliation
/// branches and all three replication-guard outcomes.
/// <para>
/// A "restart" is modelled by running a second reconciler over a fresh
/// declaration set against the <i>same</i> store, which is exactly what a silo
/// coming back up with an edited declaration does. Nothing here uses a cluster,
/// a clock, or a delay, so every assertion is deterministic.
/// </para>
/// </summary>
[TestFixture]
public sealed class GrainIndexRegistryReconcilerTests
{
    private const string IndexName = "users";

    private static void DeclareBaseline(StubSiloBuilder builder) =>
        builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
            static cfg => cfg.WithName(IndexName).Include(x => x.Age).Include(x => x.Country));

    private static async Task<CapturingLogger<GrainIndexRegistryReconciler>> ReconcileAsync(
        Action<StubSiloBuilder> declare,
        FakeGrainIndexRegistryStore store,
        ILatticeMergeModeResolver? mergeModeResolver = null,
        CancellationToken cancellationToken = default)
    {
        var builder = new StubSiloBuilder();
        declare(builder);

        // A silo that declares no index never calls AddGrainIndex, so the
        // options infrastructure the reconciler resolves has to be present
        // independently for the "nothing declared" case to be exercisable.
        builder.Services.AddOptions();
        await using var provider = builder.BuildServiceProvider();

        var logger = new CapturingLogger<GrainIndexRegistryReconciler>();
        var reconciler = new GrainIndexRegistryReconciler(
            provider.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>(),
            provider.GetRequiredService<IOptionsMonitor<GrainIndexOptions>>(),
            store,
            logger,
            mergeModeResolver);

        await reconciler.ReconcileAsync(cancellationToken);
        return logger;
    }

    // ---- Branch 1: no stored record ------------------------------------

    [Test]
    public async Task A_first_run_persists_the_declaration_and_marks_it_needing_backfill()
    {
        var store = new FakeGrainIndexRegistryStore();

        var logger = await ReconcileAsync(DeclareBaseline, store);
        var record = store.Peek(IndexName);

        Assert.Multiple(() =>
        {
            Assert.That(record, Is.Not.Null, "Declaring an index for the first time must persist it.");
            Assert.That(record!.Descriptor.Name, Is.EqualTo(IndexName));
            Assert.That(record.Descriptor.TreeName, Is.EqualTo(GrainIndexTreeNames.ForIndex(IndexName)));
            Assert.That(record.NeedsBackfill, Is.True,
                "No entry has been written for a brand-new index, so it owes a backfill.");
            Assert.That(record.Fingerprint.Value, Is.Not.Empty);
            Assert.That(store.WriteCount, Is.EqualTo(1));
            Assert.That(logger.MessagesAt(LogLevel.Information), Has.One.Contains(IndexName));
        });
    }

    [Test]
    public async Task A_first_run_persists_the_key_codec_identity_alongside_the_descriptor()
    {
        var store = new FakeGrainIndexRegistryStore();

        await ReconcileAsync(DeclareBaseline, store);

        Assert.That(
            store.Peek(IndexName)!.KeyCodecId,
            Is.EqualTo(typeof(StringGrainKeyCodec<ITestStringKeyedGrain>).FullName),
            "The codec is drift-significant but is not part of the descriptor, so the record has "
            + "to carry it for a later start to notice a codec swap.");
    }

    [Test]
    public async Task A_first_run_stores_a_fingerprint_that_matches_the_declaration()
    {
        var store = new FakeGrainIndexRegistryStore();

        await ReconcileAsync(DeclareBaseline, store);
        var record = store.Peek(IndexName)!;

        Assert.That(
            record.Fingerprint,
            Is.EqualTo(GrainIndexFingerprint.Compute(record.Descriptor, record.KeyCodecId)),
            "The persisted fingerprint must be the fingerprint of the persisted declaration, or "
            + "the next start compares against a value that describes nothing.");
    }

    [Test]
    public async Task Every_declared_index_is_reconciled()
    {
        var store = new FakeGrainIndexRegistryStore();

        await ReconcileAsync(
            static builder => builder
                .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                    static cfg => cfg.WithName("users").Include(x => x.Age))
                .AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                    static cfg => cfg.WithName("orders").Include(x => x.Country)),
            store);

        Assert.Multiple(() =>
        {
            Assert.That(store.Peek("users"), Is.Not.Null);
            Assert.That(store.Peek("orders"), Is.Not.Null);
        });
    }

    [Test]
    public async Task A_silo_that_declares_no_index_reconciles_nothing()
    {
        var store = new FakeGrainIndexRegistryStore();

        await ReconcileAsync(static _ => { }, store);

        Assert.Multiple(() =>
        {
            Assert.That(store.ReadCount, Is.Zero);
            Assert.That(store.WriteCount, Is.Zero);
        });
    }

    // ---- Branch 2: match -----------------------------------------------

    [Test]
    public async Task Restarting_with_an_unchanged_declaration_writes_nothing()
    {
        var store = new FakeGrainIndexRegistryStore();
        await ReconcileAsync(DeclareBaseline, store);
        var writesAfterFirstRun = store.WriteCount;

        await ReconcileAsync(DeclareBaseline, store);

        Assert.That(store.WriteCount, Is.EqualTo(writesAfterFirstRun),
            "An unchanged restart must be a no-op, so a silo coming back up does not churn the "
            + "registry tree.");
    }

    [Test]
    public async Task Reconciliation_is_idempotent_across_repeated_runs()
    {
        var store = new FakeGrainIndexRegistryStore();
        await ReconcileAsync(DeclareBaseline, store);
        var first = store.Peek(IndexName)!;

        await ReconcileAsync(DeclareBaseline, store);
        await ReconcileAsync(DeclareBaseline, store);

        Assert.That(store.Peek(IndexName)!.Fingerprint, Is.EqualTo(first.Fingerprint),
            "Every silo in a cluster runs this at start, so running it repeatedly must converge "
            + "rather than oscillate.");
    }

    [Test]
    public async Task An_unchanged_restart_preserves_the_needs_backfill_flag()
    {
        var store = new FakeGrainIndexRegistryStore();
        await ReconcileAsync(DeclareBaseline, store);

        await ReconcileAsync(DeclareBaseline, store);

        Assert.That(store.Peek(IndexName)!.NeedsBackfill, Is.True,
            "A no-op branch must not silently clear work the backfill worker still owes.");
    }

    // ---- Branch 3: drift on a drift-safe field --------------------------

    [Test]
    public async Task A_drift_safe_change_updates_the_stored_record_and_logs_at_information()
    {
        var store = new FakeGrainIndexRegistryStore();
        await ReconcileAsync(DeclareBaseline, store);

        var logger = await ReconcileAsync(
            static builder => builder
                .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                    static cfg => cfg.WithName(IndexName).Include(x => x.Age).Include(x => x.Country))
                .ConfigureGrainIndex(IndexName, static options => options.AllowReplication = true),
            store);

        Assert.Multiple(() =>
        {
            Assert.That(store.Peek(IndexName)!.Descriptor.AllowReplication, Is.True,
                "A drift-safe change is accepted, so the stored record must reflect it.");
            Assert.That(logger.MessagesAt(LogLevel.Information), Has.One.Contains("drift-safe"));
            Assert.That(logger.MessagesAt(LogLevel.Warning), Is.Empty,
                "A drift-safe change is not a warning-level event.");
        });
    }

    [Test]
    public async Task A_drift_safe_change_carries_the_needs_backfill_state_across_unchanged()
    {
        var store = new FakeGrainIndexRegistryStore();
        await ReconcileAsync(DeclareBaseline, store);

        // Model a backfill that has already completed, so the safe-drift branch
        // has something to preserve rather than something to set.
        var completed = store.Peek(IndexName)!;
        store.Seed(
            IndexName,
            new GrainIndexRegistryRecord(
                completed.Descriptor,
                completed.KeyCodecId,
                completed.Fingerprint,
                needsBackfill: false));

        await ReconcileAsync(
            static builder => builder
                .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                    static cfg => cfg.WithName(IndexName).Include(x => x.Age).Include(x => x.Country))
                .ConfigureGrainIndex(IndexName, static options => options.AllowReplication = true),
            store);

        Assert.That(store.Peek(IndexName)!.NeedsBackfill, Is.False,
            "A completed index must not be pushed back into a backfill by a change that left its "
            + "data valid.");
    }

    // ---- Branch 4: drift on a drift-breaking field ----------------------

    [Test]
    public async Task A_removed_projected_property_rejects_start_up_by_default()
    {
        var store = new FakeGrainIndexRegistryStore();
        await ReconcileAsync(DeclareBaseline, store);

        Assert.That(
            async () => await ReconcileAsync(
                static builder => builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                    static cfg => cfg.WithName(IndexName).Include(x => x.Age)),
                store),
            Throws.TypeOf<GrainIndexConfigurationDriftException>()
                .With.Property(nameof(GrainIndexConfigurationDriftException.ChangedFields))
                .EqualTo(new[] { GrainIndexDefinitionField.Properties })
                .And.Message.Contains(IndexName));
    }

    [Test]
    public async Task A_changed_key_codec_rejects_start_up_by_default()
    {
        var store = new FakeGrainIndexRegistryStore();
        await ReconcileAsync(DeclareBaseline, store);

        // Same index name and projection, a different grain key shape - which
        // means a different codec and therefore unreadable stored keys.
        Assert.That(
            async () => await ReconcileAsync(
                static builder => builder.AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                    static cfg => cfg.WithName(IndexName).Include(x => x.Age).Include(x => x.Country)),
                store),
            Throws.TypeOf<GrainIndexConfigurationDriftException>()
                .With.Property(nameof(GrainIndexConfigurationDriftException.ChangedFields))
                .Contains(GrainIndexDefinitionField.KeyCodec));
    }

    [Test]
    public async Task A_rejected_drift_leaves_the_stored_record_untouched()
    {
        var store = new FakeGrainIndexRegistryStore();
        await ReconcileAsync(DeclareBaseline, store);
        var before = store.Peek(IndexName)!;
        var writesBefore = store.WriteCount;

        try
        {
            await ReconcileAsync(
                static builder => builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                    static cfg => cfg.WithName(IndexName).Include(x => x.Age)),
                store);
        }
        catch (GrainIndexConfigurationDriftException)
        {
            // Expected; the assertions below are what this test is about.
        }

        Assert.Multiple(() =>
        {
            Assert.That(store.WriteCount, Is.EqualTo(writesBefore),
                "Rejecting must not half-apply the new declaration.");
            Assert.That(store.Peek(IndexName), Is.SameAs(before));
        });
    }

    [Test]
    public async Task The_rebuild_policy_accepts_a_breaking_change_and_schedules_a_backfill()
    {
        var store = new FakeGrainIndexRegistryStore();
        await ReconcileAsync(DeclareBaseline, store);

        var completed = store.Peek(IndexName)!;
        store.Seed(
            IndexName,
            new GrainIndexRegistryRecord(
                completed.Descriptor,
                completed.KeyCodecId,
                completed.Fingerprint,
                needsBackfill: false));

        var logger = await ReconcileAsync(
            static builder => builder
                .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                    static cfg => cfg.WithName(IndexName).Include(x => x.Age))
                .ConfigureGrainIndex(
                    IndexName,
                    static options => options.DriftPolicy = GrainIndexDriftPolicy.Rebuild),
            store);

        var record = store.Peek(IndexName)!;

        Assert.Multiple(() =>
        {
            Assert.That(record.Descriptor.Properties, Has.Count.EqualTo(1),
                "The opt-in policy adopts the new declaration rather than throwing.");
            Assert.That(record.NeedsBackfill, Is.True,
                "The old entries no longer match, so the index owes a rebuild.");
            Assert.That(logger.MessagesAt(LogLevel.Warning), Has.One.Contains(IndexName));
        });
    }

    [Test]
    public async Task The_rebuild_policy_still_reports_the_breaking_fields_in_its_warning()
    {
        var store = new FakeGrainIndexRegistryStore();
        await ReconcileAsync(DeclareBaseline, store);

        var logger = await ReconcileAsync(
            static builder => builder
                .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                    static cfg => cfg.WithName(IndexName).Include(x => x.Age))
                .ConfigureGrainIndex(
                    IndexName,
                    static options => options.DriftPolicy = GrainIndexDriftPolicy.Rebuild),
            store);

        Assert.That(
            logger.MessagesAt(LogLevel.Warning),
            Has.One.Contains(nameof(GrainIndexDefinitionField.Properties)),
            "An operator must be able to see what was accepted, not just that something was.");
    }

    [Test]
    public async Task A_breaking_change_on_one_index_does_not_hide_behind_another_indexs_policy()
    {
        var store = new FakeGrainIndexRegistryStore();
        await ReconcileAsync(
            static builder => builder
                .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                    static cfg => cfg.WithName("users").Include(x => x.Age).Include(x => x.Country))
                .AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                    static cfg => cfg.WithName("orders").Include(x => x.Country)),
            store);

        Assert.That(
            async () => await ReconcileAsync(
                static builder => builder
                    .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                        static cfg => cfg.WithName("users").Include(x => x.Age))
                    .AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                        static cfg => cfg.WithName("orders").Include(x => x.Country))
                    .ConfigureGrainIndex(
                        "orders",
                        static options => options.DriftPolicy = GrainIndexDriftPolicy.Rebuild),
                store),
            Throws.TypeOf<GrainIndexConfigurationDriftException>()
                .With.Property(nameof(GrainIndexConfigurationDriftException.IndexName))
                .EqualTo("users"),
            "The policy is per index, so a permissive neighbour must not soften a strict index.");
    }

    // ---- Replication guard ---------------------------------------------

    [Test]
    public void A_replicated_tree_without_the_opt_in_rejects_start_up()
    {
        var store = new FakeGrainIndexRegistryStore();
        var resolver = new FakeMergeModeResolver()
            .Replicating(GrainIndexTreeNames.ForIndex(IndexName), LatticeMergeMode.OrSet);

        Assert.That(
            async () => await ReconcileAsync(DeclareBaseline, store, resolver),
            Throws.TypeOf<GrainIndexReplicationNotAllowedException>()
                .With.Property(nameof(GrainIndexReplicationNotAllowedException.TreeName))
                .EqualTo(GrainIndexTreeNames.ForIndex(IndexName))
                .And.Property(nameof(GrainIndexReplicationNotAllowedException.MergeMode))
                .EqualTo(LatticeMergeMode.OrSet));

        Assert.That(store.WriteCount, Is.Zero,
            "The guard runs before the drift branches, so a rejected silo must not first persist "
            + "a record it is not allowed to use.");
    }

    [Test]
    public async Task A_replicated_tree_with_the_opt_in_is_allowed_and_logged_at_information()
    {
        var store = new FakeGrainIndexRegistryStore();
        var resolver = new FakeMergeModeResolver()
            .Replicating(GrainIndexTreeNames.ForIndex(IndexName), LatticeMergeMode.OrSet);

        var logger = await ReconcileAsync(
            static builder => builder
                .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                    static cfg => cfg.WithName(IndexName).Include(x => x.Age).AllowReplication()),
            store,
            resolver);

        Assert.Multiple(() =>
        {
            Assert.That(store.Peek(IndexName), Is.Not.Null,
                "An opted-in index must reconcile normally.");
            Assert.That(logger.MessagesAt(LogLevel.Information), Has.One.Contains("replicated tree"));
        });
    }

    [Test]
    public async Task An_absent_resolver_makes_the_guard_a_silent_no_op()
    {
        var store = new FakeGrainIndexRegistryStore();

        var logger = await ReconcileAsync(DeclareBaseline, store, mergeModeResolver: null);

        Assert.Multiple(() =>
        {
            Assert.That(store.Peek(IndexName), Is.Not.Null,
                "A host with no replication package registered must reconcile normally.");
            Assert.That(
                logger.MessagesAt(LogLevel.Information),
                Has.None.Contains("replicated tree"),
                "With no resolver there is nothing to audit, so the guard says nothing at all.");
        });
    }

    [Test]
    public async Task A_resolver_that_reports_no_replication_makes_the_guard_a_silent_no_op()
    {
        var store = new FakeGrainIndexRegistryStore();
        var resolver = new FakeMergeModeResolver();

        var logger = await ReconcileAsync(DeclareBaseline, store, resolver);

        Assert.Multiple(() =>
        {
            Assert.That(store.Peek(IndexName), Is.Not.Null);
            Assert.That(resolver.Queried, Is.EqualTo(new[] { GrainIndexTreeNames.ForIndex(IndexName) }),
                "The guard consults the resolver exactly once per index tree.");
            Assert.That(logger.MessagesAt(LogLevel.Information), Has.None.Contains("replicated tree"));
        });
    }

    [Test]
    public async Task The_guard_only_audits_and_never_writes_a_merge_mode_back()
    {
        var store = new FakeGrainIndexRegistryStore();
        var tree = GrainIndexTreeNames.ForIndex(IndexName);
        var resolver = new FakeMergeModeResolver().Replicating(tree, LatticeMergeMode.GCounter);

        await ReconcileAsync(
            static builder => builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName(IndexName).Include(x => x.Age).AllowReplication()),
            store,
            resolver);

        Assert.That(resolver.Resolve(tree), Is.EqualTo(LatticeMergeMode.GCounter),
            "Replication must stay a deliberate, reversible operator choice: the package "
            + "discourages the footgun without disabling the capability.");
    }

    [Test]
    public void The_guard_is_evaluated_per_index_rather_than_once_for_the_silo()
    {
        var store = new FakeGrainIndexRegistryStore();
        var resolver = new FakeMergeModeResolver()
            .Replicating(GrainIndexTreeNames.ForIndex("orders"), LatticeMergeMode.OrSet);

        Assert.That(
            async () => await ReconcileAsync(
                static builder => builder
                    .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                        static cfg => cfg.WithName("users").Include(x => x.Age))
                    .AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                        static cfg => cfg.WithName("orders").Include(x => x.Country)),
                store,
                resolver),
            Throws.TypeOf<GrainIndexReplicationNotAllowedException>()
                .With.Property(nameof(GrainIndexReplicationNotAllowedException.IndexName))
                .EqualTo("orders"));
    }

    // ---- Guards and plumbing -------------------------------------------

    [Test]
    public void An_index_backed_by_the_registrys_own_tree_is_rejected()
    {
        var store = new FakeGrainIndexRegistryStore();

        Assert.That(
            async () => await ReconcileAsync(
                static builder => builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                    static cfg => cfg
                        .WithName(GrainIndexRegistryTrees.RegistrySegment)
                        .Include(x => x.Age)),
                store),
            Throws.TypeOf<InvalidOperationException>().With.Message.Contains("registry"),
            "An index writing into the registry tree would overwrite the bookkeeping that "
            + "governs it.");
    }

    [Test]
    public async Task The_cancellation_token_flows_through_to_the_store()
    {
        var store = new FakeGrainIndexRegistryStore();
        using var cts = new CancellationTokenSource();

        await ReconcileAsync(DeclareBaseline, store, cancellationToken: cts.Token);

        Assert.That(store.LastToken, Is.EqualTo(cts.Token),
            "A caller cancelling start-up must be able to cancel the registry round trips it "
            + "is waiting on.");
    }

    [Test]
    public void A_null_dependency_is_rejected_at_construction()
    {
        var builder = new StubSiloBuilder();
        DeclareBaseline(builder);
        using var provider = builder.BuildServiceProvider();

        var declarations = provider.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>();
        var options = provider.GetRequiredService<IOptionsMonitor<GrainIndexOptions>>();
        var store = new FakeGrainIndexRegistryStore();
        var logger = new CapturingLogger<GrainIndexRegistryReconciler>();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new GrainIndexRegistryReconciler(null!, options, store, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexRegistryReconciler(declarations, null!, store, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexRegistryReconciler(declarations, options, null!, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexRegistryReconciler(declarations, options, store, null!),
                Throws.ArgumentNullException);
        });
    }
}
