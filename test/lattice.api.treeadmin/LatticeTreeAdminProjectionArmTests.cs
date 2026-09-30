using System.Collections.Immutable;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Backup;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for the projection arms of <see cref="LatticeTreeAdmin"/> that the
/// verb-focused fixtures leave cold: the fall-through arm of every enum map the
/// facade owns, the retention modes no other fixture selects, the ordered listing
/// of more than one tag index, and the non-matching iteration of the cluster-wide
/// view-registry scan.
/// </summary>
/// <remarks>
/// <para>
/// A mapping switch is the cheapest place in a facade for a silent defect to live.
/// Every arm is one line, a missing arm compiles, and the discard arm swallows an
/// unmapped value into a plausible-looking default - so a core enum that gains a
/// member is reported to the wire as whichever value the discard names, with no
/// build break and no test failure anywhere. The maps are therefore asserted here
/// arm by arm through the public verbs that own them, rather than by reflection:
/// a reflected map test proves the map and exempts the wiring that selects it.
/// </para>
/// <para>
/// The discard arms are asserted with the arm's own documented meaning, so the
/// assertion still says something when a future member is added: a WAL move that
/// the engine did not classify reports <see cref="TreeWalMoveOutcome.NoOp"/>, and
/// a restore whose mode is not a shadow cutover reports
/// <see cref="TreeRestoreMode.InPlace"/> in both directions. Each is paired with
/// its accepting counterpart over the same setup, so neither can pass by mapping
/// everything to one value.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LatticeTreeAdminProjectionArmTests
{
    private const string Tree = "orders";
    private const string ViewName = "orders-by-region";
    private const string SourceTree = "orders";

    private sealed class AllowingGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Allow());
    }

    private static LatticeTreeAdmin Create(
        IGrainFactory factory,
        ILatticeBackupRestoreService? restoreService = null,
        IViewCatalog? viewCatalog = null,
        ILatticeViewFactory? viewFactory = null,
        ILatticeTagIndexFactory? tagIndexFactory = null)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(new AllowingGate()),
            Options.Create(new LatticeApiTreeAdminOptions()),
            new NullTenantContextResolver(),
            restoreService,
            viewCatalog,
            viewFactory,
            tagIndexFactory);

    private static ILatticeAdmin WireAdmin(IGrainFactory factory)
    {
        var admin = Substitute.For<ILatticeAdmin>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);
        return admin;
    }

    // ----- WAL move outcome: every arm, including the unclassified fall-through -----

    private static async Task<TreeWalMoveOutcome> ExecuteWithOutcomeAsync(WalMoveOutcome outcome)
    {
        var factory = Substitute.For<IGrainFactory>();
        WireAdmin(factory)
            .ExecuteWalMoveAsync(Tree, 0, "wal-secondary", Arg.Any<WalMoveOptions?>(), Arg.Any<CancellationToken>())
            .Returns(new WalMoveReceipt { TreeId = Tree, Partition = 0, Outcome = outcome });

        var receipt = await Create(factory).ExecuteWalMoveAsync(Tree, 0, "wal-secondary");
        return receipt.Outcome;
    }

    [Test]
    public async Task ExecuteWalMoveAsync_maps_every_named_core_outcome()
    {
        // The accepting counterparts to the fall-through arm below. Asserted over the
        // same setup so a map that collapsed onto a single value fails here rather
        // than passing both tests.
        Assert.Multiple(async () =>
        {
            Assert.That(await ExecuteWithOutcomeAsync(WalMoveOutcome.Moved), Is.EqualTo(TreeWalMoveOutcome.Moved));
            Assert.That(await ExecuteWithOutcomeAsync(WalMoveOutcome.AlreadyAtTarget), Is.EqualTo(TreeWalMoveOutcome.AlreadyAtTarget));
            Assert.That(await ExecuteWithOutcomeAsync(WalMoveOutcome.SourceReclaimed), Is.EqualTo(TreeWalMoveOutcome.SourceReclaimed));
        });
    }

    [Test]
    public async Task ExecuteWalMoveAsync_maps_the_unclassified_outcome_to_no_op()
    {
        Assert.That(await ExecuteWithOutcomeAsync(WalMoveOutcome.NoOp), Is.EqualTo(TreeWalMoveOutcome.NoOp));
    }

    [Test]
    public async Task ExecuteWalMoveAsync_maps_an_outcome_outside_the_named_set_to_no_op()
    {
        // A core enum that gains a member the facade has not mapped must degrade to
        // "nothing observable happened" rather than being reported as a completed
        // move. Cast past the declared members deliberately - that is exactly the
        // state a future core release produces against today's facade binary.
        var unmapped = (WalMoveOutcome)int.MaxValue;

        Assert.That(await ExecuteWithOutcomeAsync(unmapped), Is.EqualTo(TreeWalMoveOutcome.NoOp));
    }

    // ----- history retention mode: the two arms no other fixture selects -----

    private static async Task<HistoryRetentionMode?> SetRetentionAndCaptureAsync(TreeHistoryRetentionMode? mode)
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>(Tree).Returns(tree);
        tree.GetHistoryRetentionAsync(Arg.Any<CancellationToken>())
            .Returns(new HistoryRetentionSettings());

        // Capture from the recorded call rather than through Arg.Do inside a
        // Received(...) assertion: a matcher evaluated after the fact reports the
        // parameter's default when it does not bind, which reads as a real mapping
        // result and is exactly the false green this fixture exists to prevent.
        HistoryRetentionMode? captured = null;
        var captures = 0;
        tree.When(t => t.SetHistoryRetentionAsync(
                Arg.Any<HistoryRetentionMode?>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>()))
            .Do(call =>
            {
                captured = call.Arg<HistoryRetentionMode?>();
                captures++;
            });

        await Create(factory).SetHistoryRetentionAsync(Tree, mode, window: null);

        Assert.That(captures, Is.EqualTo(1), "the retention write must reach the tree exactly once");
        return captured;
    }

    [Test]
    public async Task SetHistoryRetentionAsync_maps_every_named_retention_mode()
    {
        Assert.Multiple(async () =>
        {
            Assert.That(await SetRetentionAndCaptureAsync(TreeHistoryRetentionMode.FullValue), Is.EqualTo(HistoryRetentionMode.FullValue));
            Assert.That(await SetRetentionAndCaptureAsync(TreeHistoryRetentionMode.Hybrid), Is.EqualTo(HistoryRetentionMode.Hybrid));
            Assert.That(await SetRetentionAndCaptureAsync(TreeHistoryRetentionMode.MetadataOnly), Is.EqualTo(HistoryRetentionMode.MetadataOnly));
        });
    }

    [Test]
    public async Task SetHistoryRetentionAsync_passes_a_null_mode_through_to_clear_the_override()
    {
        // The null arm is the documented "clear the override" signal: the core falls
        // back to its own default. It must not be silently rewritten to MetadataOnly
        // just because that is the core enum's zero value.
        Assert.That(await SetRetentionAndCaptureAsync(null), Is.Null);
    }

    // ----- restore mode: both directions, both arms -----

    private static LatticeRestoreResult EngineResult(LatticeRestoreMode mode)
        => new(
            "bk-1",
            Tree,
            mode,
            "op-1",
            ["bk-1"],
            entriesApplied: 1,
            shadowPhysicalTreeId: "phys-new",
            previousPhysicalTreeId: "phys-old");

    private static async Task<TreeRestoreMode> RestoreSetModeAsync(LatticeRestoreMode mode)
    {
        var service = Substitute.For<ILatticeBackupRestoreService>();
        service.RestoreSetAsync("set-1", Arg.Any<CancellationToken>())
            .Returns(new List<LatticeRestoreResult> { EngineResult(mode) });

        var results = await Create(Substitute.For<IGrainFactory>(), service).RestoreTreeSetAsync("set-1");
        return results[0].Mode;
    }

    [Test]
    public async Task RestoreTreeSetAsync_maps_a_shadow_cutover_result_onto_the_wire_mode()
    {
        Assert.That(await RestoreSetModeAsync(LatticeRestoreMode.ShadowCutover), Is.EqualTo(TreeRestoreMode.ShadowCutover));
    }

    [Test]
    public async Task RestoreTreeSetAsync_maps_a_non_shadow_result_onto_in_place()
    {
        // The set verb is the only path that surfaces a mode the facade did not
        // force: RestoreTreeAsync pins ShadowCutover itself, so the engine-to-wire
        // direction of this map is never exercised through it.
        Assert.That(await RestoreSetModeAsync(LatticeRestoreMode.InPlace), Is.EqualTo(TreeRestoreMode.InPlace));
    }

    private static async Task<LatticeRestoreMode> RevertModeAsync(TreeRestoreMode mode)
    {
        var service = Substitute.For<ILatticeBackupRestoreService>();
        LatticeRestoreMode captured = default;
        var captures = 0;
        service.When(s => s.RevertRestoreAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>()))
            .Do(call =>
            {
                captured = call.Arg<LatticeRestoreResult>().Mode;
                captures++;
            });
        var facade = Create(Substitute.For<IGrainFactory>(), service);

        await facade.RevertTreeRestoreAsync(new TreeRestoreResult
        {
            BackupId = "bk-1",
            TargetTreeId = Tree,
            Mode = mode,
            OperationId = "op-1",
            ManifestChain = ImmutableArray.Create("bk-1"),
            EntriesApplied = 1,
            ShadowPhysicalTreeId = "phys-new",
            PreviousPhysicalTreeId = "phys-old",
        });

        Assert.That(captures, Is.EqualTo(1), "the revert must reach the backup engine exactly once");
        return captured;
    }

    [Test]
    public async Task RevertTreeRestoreAsync_reconstructs_a_shadow_cutover_mode_for_the_engine()
    {
        Assert.That(await RevertModeAsync(TreeRestoreMode.ShadowCutover), Is.EqualTo(LatticeRestoreMode.ShadowCutover));
    }

    [Test]
    public async Task RevertTreeRestoreAsync_reconstructs_a_non_shadow_mode_as_in_place()
    {
        // The engine rejects a non-shadow-cutover result, so the value handed to it
        // must be the caller's own mode rather than a silently upgraded one - the
        // rejection is what makes the revert contract safe.
        Assert.That(await RevertModeAsync(TreeRestoreMode.InPlace), Is.EqualTo(LatticeRestoreMode.InPlace));
    }

    // ----- tag index listing: ordering across more than one index -----

    [Test]
    public async Task ListTagIndexesAsync_orders_multiple_indexes_ordinally()
    {
        // The single-index fixtures cannot witness the ordinal ordering at all: the
        // sort is a no-op below two elements, so the comparison this listing depends
        // on has never run. Seed the registry out of order, and include an ordinal
        // pair whose relative order differs from a culture-aware compare.
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        string Tag(string name) => LatticeConstants.TagIndexTreePrefix + name;
        var ids = new List<string> { Tag("colour"), Tag("Region"), Tag("age"), "orders" };
        registry.GetAllTreeIdsAsync(Arg.Any<string?>()).Returns(call =>
        {
            var prefix = call.Arg<string?>();
            return Task.FromResult<IReadOnlyList<string>>(
                string.IsNullOrEmpty(prefix)
                    ? ids
                    : ids.Where(id => id.StartsWith(prefix, StringComparison.Ordinal)).ToList());
        });
        registry.GetEntryAsync(Arg.Any<string>()).Returns(new TreeRegistryEntry { ShardCount = 4 });
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var multiTree = Substitute.For<ILatticeMultiTreeTagIndex>();
        multiTree.CoveredTreesAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<string>>([]));
        var tagFactory = Substitute.For<ILatticeTagIndexFactory>();
        tagFactory.CreateMultiTree(Arg.Any<string>(), Arg.Any<IReadOnlyCollection<string>?>()).Returns(multiTree);

        var catalog = await Create(factory, tagIndexFactory: tagFactory).ListTagIndexesAsync();

        Assert.Multiple(() =>
        {
            // Ordinal order puts every upper-case letter before every lower-case one,
            // so "Region" leads. A culture-aware compare would return age/colour/Region.
            Assert.That(
                catalog.Indexes.Select(i => i.IndexName).ToArray(),
                Is.EqualTo(new[] { "Region", "age", "colour" }),
                "the listing must be ordinally ordered, matching the registry's own key order");
            Assert.That(catalog.Indexes, Has.Length.EqualTo(3),
                "the non-tag tree must not be listed");
        });
    }

    // ----- view registry scan: the non-matching iteration -----

    private static RuntimeViewRegistration Registration(string viewName, string sourceTreeId)
        => new()
        {
            ViewName = viewName,
            SourceTreeId = sourceTreeId,
            ProjectionTypeName = "Test.Projection",
            ProjectionVersion = "v1",
            ProjectionProviderKey = "test-provider",
        };

    private static IViewRegistryGrain WireViewRegistry(IGrainFactory factory, params RuntimeViewRegistration[] views)
    {
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns(new List<RuntimeViewRegistration>(views));
        factory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);
        return registry;
    }

    [Test]
    public async Task DropViewAsync_scans_past_a_non_matching_registration_to_the_match()
    {
        // Every existing fixture seeds a registry whose first entry matches, so the
        // scan has only ever been observed returning on its first iteration. A
        // registry holding another tenant's or another team's views is the ordinary
        // case, and a scan that stopped at the first entry would silently report a
        // present view as absent - which DropView treats as success and skips.
        var factory = Substitute.For<IGrainFactory>();
        WireViewRegistry(
            factory,
            Registration("unrelated-view", "widgets"),
            Registration(ViewName, SourceTree));
        var viewFactory = Substitute.For<ILatticeViewFactory>();

        await Create(factory, viewFactory: viewFactory).DropViewAsync(ViewName);

        await viewFactory.Received(1).DeleteAsync(ViewName, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DropViewAsync_with_only_non_matching_registrations_is_a_no_op()
    {
        // The accepting counterpart above proves the scan does not stop early; this
        // proves it does not match everything either.
        var factory = Substitute.For<IGrainFactory>();
        WireViewRegistry(factory, Registration("unrelated-view", "widgets"));
        var viewFactory = Substitute.For<ILatticeViewFactory>();

        await Create(factory, viewFactory: viewFactory).DropViewAsync(ViewName);

        await viewFactory.DidNotReceiveWithAnyArgs().DeleteAsync(default!, default);
    }
}
