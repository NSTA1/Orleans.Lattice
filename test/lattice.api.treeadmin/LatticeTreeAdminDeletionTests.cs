using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for the tree soft-delete / recover / hard-purge / deletion-status
/// operations on <see cref="LatticeTreeAdmin"/>. The mutating verbs wrap the public
/// <see cref="ILattice"/> grain (inheriting its guards and internal-origin marker)
/// after authorizing the whole-tree <c>TreeLifecycle</c> capability fail-closed; the
/// status read authorizes whole-tree <c>Read</c>. Every verb projects the tree's
/// deletion snapshot (read from the per-tree <see cref="ITreeDeletionGrain"/>) onto
/// the public <see cref="TreeDeletionStatus"/>. Driven purely with substitutes and a
/// hand-written access gate - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminDeletionTests
{
    private const string Tree = "orders";

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
            Options.Create(new LatticeApiTreeAdminOptions()));

    private static (ILattice Lattice, ITreeDeletionGrain Deletion) Wire(IGrainFactory factory)
    {
        var lattice = Substitute.For<ILattice>();
        var deletion = Substitute.For<ITreeDeletionGrain>();
        factory.GetGrain<ILattice>(Tree).Returns(lattice);
        factory.GetGrain<ITreeDeletionGrain>(Tree).Returns(deletion);
        return (lattice, deletion);
    }

    // ----- DeleteTree -----

    [Test]
    public async Task DeleteTreeAsync_wraps_the_lattice_verb_and_projects_the_status()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, deletion) = Wire(factory);
        var deletedAt = DateTimeOffset.UtcNow;
        deletion.GetDeletionStatusAsync().Returns(new TreeDeletionSnapshot
        {
            IsDeleted = true,
            DeletedAtUtc = deletedAt,
            RecoveryDeadlineUtc = deletedAt.AddHours(1),
            PurgeInProgress = false,
            PurgeComplete = false,
        });
        var facade = Create(factory);

        var status = await facade.DeleteTreeAsync(Tree);

        await lattice.Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(status.TreeId, Is.EqualTo(Tree));
            Assert.That(status.IsDeleted, Is.True);
            Assert.That(status.DeletedAtUtc, Is.EqualTo(deletedAt));
            Assert.That(status.RecoveryDeadlineUtc, Is.EqualTo(deletedAt.AddHours(1)));
            Assert.That(status.CanRecover, Is.True);
        });
    }

    [Test]
    public void DeleteTreeAsync_denied_by_lifecycle_gate_throws_and_does_not_dial_lattice()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, _) = Wire(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.DeleteTreeAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        lattice.DidNotReceive().DeleteTreeAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void DeleteTreeAsync_reserved_tree_id_is_rejected()
    {
        var factory = Substitute.For<IGrainFactory>();
        Wire(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.DeleteTreeAsync(LatticeConstants.SystemTreePrefix + "trees"),
            Throws.ArgumentException);
    }

    [Test]
    public void DeleteTreeAsync_null_or_empty_tree_id_throws()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.DeleteTreeAsync(null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.DeleteTreeAsync(""), Throws.ArgumentException);
        });
    }

    // ----- RecoverTree -----

    [Test]
    public async Task RecoverTreeAsync_wraps_the_lattice_verb_and_projects_the_status()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, deletion) = Wire(factory);
        deletion.GetDeletionStatusAsync().Returns(new TreeDeletionSnapshot { IsDeleted = false });
        var facade = Create(factory);

        var status = await facade.RecoverTreeAsync(Tree);

        await lattice.Received(1).RecoverTreeAsync(Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(status.IsDeleted, Is.False);
            Assert.That(status.CanRecover, Is.False);
        });
    }

    [Test]
    public void RecoverTreeAsync_denied_by_lifecycle_gate_throws_and_does_not_dial_lattice()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, _) = Wire(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.RecoverTreeAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        lattice.DidNotReceive().RecoverTreeAsync(Arg.Any<CancellationToken>());
    }

    // ----- PurgeTree -----

    [Test]
    public async Task PurgeTreeAsync_with_confirmation_wraps_the_lattice_verb_and_projects_the_status()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, deletion) = Wire(factory);
        deletion.GetDeletionStatusAsync().Returns(new TreeDeletionSnapshot
        {
            IsDeleted = true,
            PurgeComplete = true,
        });
        var facade = Create(factory);

        var status = await facade.PurgeTreeAsync(Tree, confirm: true);

        await lattice.Received(1).PurgeTreeAsync(Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(status.PurgeComplete, Is.True);
            Assert.That(status.CanRecover, Is.False);
        });
    }

    [Test]
    public void PurgeTreeAsync_without_confirmation_is_rejected_before_any_authorization_or_dial()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, _) = Wire(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.PurgeTreeAsync(Tree, confirm: false),
            Throws.ArgumentException);
        lattice.DidNotReceive().PurgeTreeAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void PurgeTreeAsync_denied_by_lifecycle_gate_throws_and_does_not_dial_lattice()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, _) = Wire(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.PurgeTreeAsync(Tree, confirm: true),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        lattice.DidNotReceive().PurgeTreeAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void PurgeTreeAsync_reserved_tree_id_is_rejected()
    {
        var factory = Substitute.For<IGrainFactory>();
        Wire(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.PurgeTreeAsync(LatticeConstants.SystemTreePrefix + "trees", confirm: true),
            Throws.ArgumentException);
    }

    // ----- GetTreeDeletionStatus -----

    [Test]
    public async Task GetTreeDeletionStatusAsync_reads_the_coordinator_and_projects_the_snapshot()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, deletion) = Wire(factory);
        var deletedAt = DateTimeOffset.UtcNow;
        deletion.GetDeletionStatusAsync().Returns(new TreeDeletionSnapshot
        {
            IsDeleted = true,
            DeletedAtUtc = deletedAt,
            RecoveryDeadlineUtc = deletedAt.AddHours(1),
            PurgeInProgress = true,
            PurgeComplete = false,
        });
        var facade = Create(factory);

        var status = await facade.GetTreeDeletionStatusAsync(Tree);

        // A pure read never touches the mutating lattice verbs.
        await lattice.DidNotReceive().DeleteTreeAsync(Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(status.TreeId, Is.EqualTo(Tree));
            Assert.That(status.IsDeleted, Is.True);
            Assert.That(status.PurgeInProgress, Is.True);
            // A purge in progress means the tree can no longer be recovered.
            Assert.That(status.CanRecover, Is.False);
        });
    }

    [Test]
    public void GetTreeDeletionStatusAsync_denied_by_read_gate_throws_and_does_not_dial_the_coordinator()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (_, deletion) = Wire(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.GetTreeDeletionStatusAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        deletion.DidNotReceive().GetDeletionStatusAsync();
    }

    [Test]
    public void GetTreeDeletionStatusAsync_null_or_empty_tree_id_throws()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.GetTreeDeletionStatusAsync(null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.GetTreeDeletionStatusAsync(""), Throws.ArgumentException);
        });
    }
}
