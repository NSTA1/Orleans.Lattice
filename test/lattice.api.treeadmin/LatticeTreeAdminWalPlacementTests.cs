using System.Collections.Immutable;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for the WAL placement inspection and move operations on
/// <see cref="LatticeTreeAdmin"/>. The read verbs (inspect, audit, plan) authorize
/// the whole-tree <c>Read</c> capability fail-closed and project the core
/// <see cref="ILatticeAdmin"/> WAL DTOs onto their transport-agnostic mirrors; the
/// mutating verbs (execute, reclaim) reject reserved tree ids and authorize the
/// whole-tree <c>TreeLifecycle</c> capability before dialing the admin grain. Driven
/// purely with substitutes and a hand-written access gate - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminWalPlacementTests
{
    private const string Tree = "orders";
    private const string TargetKey = "wal-secondary";
    private const string SourceKey = "wal-primary";

    private sealed class FixedGate : ILatticeAccessGate
    {
        private readonly bool _allow;
        public FixedGate(bool allow) => _allow = allow;

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(_allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied by test"));
    }

    private static LatticeTreeAdmin Create(IGrainFactory factory, bool allow = true)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(new FixedGate(allow)),
            Options.Create(new LatticeApiTreeAdminOptions()),
            new NullTenantContextResolver());

    private static ILatticeAdmin WireAdmin(IGrainFactory factory)
    {
        var admin = Substitute.For<ILatticeAdmin>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);
        return admin;
    }

    // ----- GetWalPlacement -----

    [Test]
    public async Task GetWalPlacementAsync_projects_the_core_placement()
    {
        var factory = Substitute.For<IGrainFactory>();
        var admin = WireAdmin(factory);
        admin.GetWalPlacementAsync(Tree, Arg.Any<CancellationToken>()).Returns(new WalPlacement
        {
            TreeId = Tree,
            Version = 7,
            DefaultProviderKey = SourceKey,
            Partitions = ImmutableArray.Create(
                new WalPartitionPlacement { Partition = 0, ProviderKey = SourceKey, ResolvableOnThisSilo = true },
                new WalPartitionPlacement { Partition = 1, ProviderKey = TargetKey, ResolvableOnThisSilo = false }),
        });
        var facade = Create(factory);

        var placement = await facade.GetWalPlacementAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(placement.TreeId, Is.EqualTo(Tree));
            Assert.That(placement.Version, Is.EqualTo(7));
            Assert.That(placement.DefaultProviderKey, Is.EqualTo(SourceKey));
            Assert.That(placement.Partitions, Has.Length.EqualTo(2));
            Assert.That(placement.Partitions[1].ProviderKey, Is.EqualTo(TargetKey));
            Assert.That(placement.Partitions[1].ResolvableOnThisSilo, Is.False);
        });
    }

    [Test]
    public void GetWalPlacementAsync_denied_by_read_gate_throws_and_does_not_dial()
    {
        var factory = Substitute.For<IGrainFactory>();
        var admin = WireAdmin(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.GetWalPlacementAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        admin.DidNotReceive().GetWalPlacementAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void GetWalPlacementAsync_null_or_empty_tree_id_throws()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.GetWalPlacementAsync(null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.GetWalPlacementAsync(""), Throws.ArgumentException);
        });
    }

    // ----- AuditWalPlacement -----

    [Test]
    public async Task AuditWalPlacementAsync_projects_the_core_audit()
    {
        var factory = Substitute.For<IGrainFactory>();
        var admin = WireAdmin(factory);
        admin.AuditWalPlacementAsync(Tree, Arg.Any<CancellationToken>()).Returns(new WalPlacementAudit
        {
            TreeId = Tree,
            Version = 3,
            PartitionCount = 2,
            Partitions = ImmutableArray.Create(
                new WalPartitionPlacement { Partition = 0, ProviderKey = SourceKey, ResolvableOnThisSilo = true }),
            AllResolvableOnThisSilo = false,
            KnownProviderKeys = ImmutableArray.Create(SourceKey, TargetKey),
        });
        var facade = Create(factory);

        var audit = await facade.AuditWalPlacementAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(audit.TreeId, Is.EqualTo(Tree));
            Assert.That(audit.Version, Is.EqualTo(3));
            Assert.That(audit.PartitionCount, Is.EqualTo(2));
            Assert.That(audit.AllResolvableOnThisSilo, Is.False);
            Assert.That(audit.KnownProviderKeys, Is.EquivalentTo(new[] { SourceKey, TargetKey }));
        });
    }

    [Test]
    public void AuditWalPlacementAsync_denied_by_read_gate_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireAdmin(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.AuditWalPlacementAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ----- PlanWalMove -----

    [Test]
    public async Task PlanWalMoveAsync_projects_the_core_plan()
    {
        var factory = Substitute.For<IGrainFactory>();
        var admin = WireAdmin(factory);
        admin.PlanWalMoveAsync(Tree, 1, TargetKey, Arg.Any<CancellationToken>()).Returns(new WalMovePlan
        {
            TreeId = Tree,
            Partition = 1,
            FromProviderKey = SourceKey,
            ToProviderKey = TargetKey,
            PlacementVersion = 9,
            SourceLowestOffset = 0,
            SourceHighestOffset = 41,
            EntriesToCopy = 42,
            TargetResolvableOnThisSilo = true,
            AlreadyAtTarget = false,
        });
        var facade = Create(factory);

        var plan = await facade.PlanWalMoveAsync(Tree, 1, TargetKey);

        Assert.Multiple(() =>
        {
            Assert.That(plan.Partition, Is.EqualTo(1));
            Assert.That(plan.FromProviderKey, Is.EqualTo(SourceKey));
            Assert.That(plan.ToProviderKey, Is.EqualTo(TargetKey));
            Assert.That(plan.PlacementVersion, Is.EqualTo(9));
            Assert.That(plan.EntriesToCopy, Is.EqualTo(42));
            Assert.That(plan.TargetResolvableOnThisSilo, Is.True);
            Assert.That(plan.AlreadyAtTarget, Is.False);
        });
    }

    [Test]
    public void PlanWalMoveAsync_denied_by_read_gate_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireAdmin(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.PlanWalMoveAsync(Tree, 0, TargetKey),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void PlanWalMoveAsync_null_or_empty_arguments_throw()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.PlanWalMoveAsync(null!, 0, TargetKey), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.PlanWalMoveAsync("", 0, TargetKey), Throws.ArgumentException);
            Assert.That(async () => await facade.PlanWalMoveAsync(Tree, 0, null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.PlanWalMoveAsync(Tree, 0, ""), Throws.ArgumentException);
        });
    }

    // ----- ExecuteWalMove -----

    [Test]
    public async Task ExecuteWalMoveAsync_maps_options_and_projects_the_receipt()
    {
        var factory = Substitute.For<IGrainFactory>();
        var admin = WireAdmin(factory);
        admin.ExecuteWalMoveAsync(Tree, 1, TargetKey, Arg.Any<WalMoveOptions?>(), Arg.Any<CancellationToken>())
            .Returns(new WalMoveReceipt
            {
                TreeId = Tree,
                Partition = 1,
                FromProviderKey = SourceKey,
                ToProviderKey = TargetKey,
                PreviousPlacementVersion = 4,
                NewPlacementVersion = 5,
                CopiedFromOffset = 0,
                CopiedThroughOffset = 41,
                SourceHighestOffset = 41,
                TargetHighestOffset = 41,
                SourceRetained = true,
                Outcome = WalMoveOutcome.Moved,
            });
        var facade = Create(factory);

        var options = new TreeWalMoveOptions
        {
            QuiesceLeaseSeconds = 45,
            CopyPageSize = 128,
            DisableVerifyAfterCopy = true,
        };
        var receipt = await facade.ExecuteWalMoveAsync(Tree, 1, TargetKey, options);

        await admin.Received(1).ExecuteWalMoveAsync(
            Tree, 1, TargetKey,
            Arg.Is<WalMoveOptions?>(o => o.HasValue
                && o.Value.QuiesceLease == TimeSpan.FromSeconds(45)
                && o.Value.CopyPageSize == 128
                && o.Value.VerifyAfterCopy == false),
            Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(receipt.NewPlacementVersion, Is.EqualTo(5));
            Assert.That(receipt.SourceRetained, Is.True);
            Assert.That(receipt.Outcome, Is.EqualTo(TreeWalMoveOutcome.Moved));
        });
    }

    [Test]
    public async Task ExecuteWalMoveAsync_null_options_passes_null_to_the_core()
    {
        var factory = Substitute.For<IGrainFactory>();
        var admin = WireAdmin(factory);
        admin.ExecuteWalMoveAsync(Tree, 0, TargetKey, Arg.Any<WalMoveOptions?>(), Arg.Any<CancellationToken>())
            .Returns(new WalMoveReceipt { TreeId = Tree, Partition = 0, Outcome = WalMoveOutcome.AlreadyAtTarget });
        var facade = Create(factory);

        await facade.ExecuteWalMoveAsync(Tree, 0, TargetKey);

        await admin.Received(1).ExecuteWalMoveAsync(
            Tree, 0, TargetKey, Arg.Is<WalMoveOptions?>(o => o == null), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ExecuteWalMoveAsync_denied_by_lifecycle_gate_throws_and_does_not_dial()
    {
        var factory = Substitute.For<IGrainFactory>();
        var admin = WireAdmin(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.ExecuteWalMoveAsync(Tree, 0, TargetKey),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        admin.DidNotReceive().ExecuteWalMoveAsync(
            Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<WalMoveOptions?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ExecuteWalMoveAsync_reserved_tree_id_is_rejected_before_any_dial()
    {
        var factory = Substitute.For<IGrainFactory>();
        var admin = WireAdmin(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.ExecuteWalMoveAsync(LatticeConstants.SystemTreePrefix + "trees", 0, TargetKey),
            Throws.ArgumentException);
        admin.DidNotReceive().ExecuteWalMoveAsync(
            Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<WalMoveOptions?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ExecuteWalMoveAsync_null_or_empty_arguments_throw()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.ExecuteWalMoveAsync(null!, 0, TargetKey), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.ExecuteWalMoveAsync("", 0, TargetKey), Throws.ArgumentException);
            Assert.That(async () => await facade.ExecuteWalMoveAsync(Tree, 0, null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.ExecuteWalMoveAsync(Tree, 0, ""), Throws.ArgumentException);
        });
    }

    // ----- ReclaimMovedWalSource -----

    [Test]
    public async Task ReclaimMovedWalSourceAsync_projects_the_receipt()
    {
        var factory = Substitute.For<IGrainFactory>();
        var admin = WireAdmin(factory);
        admin.ReclaimMovedWalSourceAsync(Tree, 1, SourceKey, Arg.Any<CancellationToken>())
            .Returns(new WalMoveReceipt
            {
                TreeId = Tree,
                Partition = 1,
                FromProviderKey = SourceKey,
                ToProviderKey = TargetKey,
                SourceRetained = false,
                Outcome = WalMoveOutcome.SourceReclaimed,
            });
        var facade = Create(factory);

        var receipt = await facade.ReclaimMovedWalSourceAsync(Tree, 1, SourceKey);

        Assert.Multiple(() =>
        {
            Assert.That(receipt.SourceRetained, Is.False);
            Assert.That(receipt.Outcome, Is.EqualTo(TreeWalMoveOutcome.SourceReclaimed));
        });
    }

    [Test]
    public void ReclaimMovedWalSourceAsync_denied_by_lifecycle_gate_throws_and_does_not_dial()
    {
        var factory = Substitute.For<IGrainFactory>();
        var admin = WireAdmin(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.ReclaimMovedWalSourceAsync(Tree, 0, SourceKey),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        admin.DidNotReceive().ReclaimMovedWalSourceAsync(
            Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ReclaimMovedWalSourceAsync_reserved_tree_id_is_rejected_before_any_dial()
    {
        var factory = Substitute.For<IGrainFactory>();
        var admin = WireAdmin(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.ReclaimMovedWalSourceAsync(LatticeConstants.SystemTreePrefix + "trees", 0, SourceKey),
            Throws.ArgumentException);
        admin.DidNotReceive().ReclaimMovedWalSourceAsync(
            Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ReclaimMovedWalSourceAsync_null_or_empty_arguments_throw()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.ReclaimMovedWalSourceAsync(null!, 0, SourceKey), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.ReclaimMovedWalSourceAsync("", 0, SourceKey), Throws.ArgumentException);
            Assert.That(async () => await facade.ReclaimMovedWalSourceAsync(Tree, 0, null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.ReclaimMovedWalSourceAsync(Tree, 0, ""), Throws.ArgumentException);
        });
    }
}
