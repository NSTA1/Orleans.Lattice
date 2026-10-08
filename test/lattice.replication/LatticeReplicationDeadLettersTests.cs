using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class LatticeReplicationDeadLettersTests
{
    private const string TreeId = "tree";
    private const string TenantTreeId = "t/acme/orders";

    private static (LatticeReplicationDeadLetters seam, IReplicationDeadLetterGrain grain) Build()
    {
        var grain = Substitute.For<IReplicationDeadLetterGrain>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IReplicationDeadLetterGrain>(TreeId).Returns(grain);

        // ReplicationApplier is sealed; Substitute.ForPartsOf is impossible
        // here because it has no parameterless ctor. We construct it with
        // substituted dependencies - the inner is only invoked by ReplayAsync,
        // which our tests cover by stubbing TryGetAsync to return null (so
        // the inner is never called) for the unit-level tests.
        var innerFactory = Substitute.For<IGrainFactory>();
        var inner = new ReplicationApplier(
            innerFactory,
            Substitute.For<Microsoft.Extensions.Options.IOptionsMonitor<LatticeReplicationOptions>>());
        var seam = new LatticeReplicationDeadLetters(grainFactory, inner);
        return (seam, grain);
    }

    [Test]
    public void ListAsync_throws_on_null_or_empty_tree_id()
    {
        var (seam, _) = Build();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await seam.ListAsync(null!, CancellationToken.None), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await seam.ListAsync("", CancellationToken.None), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public async Task ListAsync_routes_to_the_per_tree_grain()
    {
        var (seam, grain) = Build();
        IReadOnlyList<DeadLetterEntry> stub = new[]
        {
            new DeadLetterEntry { EntryId = 1, FailureReason = "x" },
        };
        grain.ListAsync(Arg.Any<CancellationToken>()).Returns(stub);

        var entries = await seam.ListAsync(TreeId, CancellationToken.None);

        Assert.That(entries, Is.SameAs(stub));
    }

    [Test]
    public async Task CountAsync_routes_to_the_per_tree_grain()
    {
        var (seam, grain) = Build();
        grain.CountAsync(Arg.Any<CancellationToken>()).Returns(7);

        Assert.That(await seam.CountAsync(TreeId, CancellationToken.None), Is.EqualTo(7));
    }

    [Test]
    public async Task DiscardAsync_routes_to_the_per_tree_grain()
    {
        var (seam, grain) = Build();
        grain.DiscardAsync(42, Arg.Any<CancellationToken>()).Returns(true);

        Assert.That(await seam.DiscardAsync(TreeId, 42, CancellationToken.None), Is.True);
    }

    [Test]
    public async Task ReplayAsync_returns_null_when_entry_not_present()
    {
        var (seam, grain) = Build();
        grain.TryGetAsync(99, Arg.Any<CancellationToken>()).Returns((DeadLetterEntry?)null);

        Assert.That(await seam.ReplayAsync(TreeId, 99, CancellationToken.None), Is.Null);
        await grain.DidNotReceive().RemoveReplayedAsync(Arg.Any<long>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReplayAsync_keeps_legacy_tenant_entry_parked_when_sender_identity_is_missing()
    {
        var grain = Substitute.For<IReplicationDeadLetterGrain>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IReplicationDeadLetterGrain>(TenantTreeId).Returns(grain);
        var entry = new WalRecord
        {
            TreeId = TenantTreeId,
            Op = MutationKind.Set,
            Key = "k",
            Value = [1],
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "origin",
            Mode = LatticeMergeMode.LwwRegister,
        };
        grain.TryGetAsync(7, Arg.Any<CancellationToken>()).Returns(new DeadLetterEntry
        {
            EntryId = 7,
            Entry = entry,
            FailureReason = "old entry",
            SourceLineageClusterId = "origin",
            SourceLineage = Guid.NewGuid(),
            AuthenticatedSenderClusterId = null,
        });

        var gate = Substitute.For<IReplicationTenantIsolationGate>();
        gate.IsActive.Returns(true);
        gate.EvaluateAsync(TenantTreeId, Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(call => new ValueTask<ReplicationTenantIsolationDecision>(
                call.ArgAt<string?>(1) is null
                    ? ReplicationTenantIsolationDecision.RejectMissingSourceIdentity
                    : ReplicationTenantIsolationDecision.RejectSourceNotResident));
        var context = Substitute.For<ILatticeReplicationContext>();
        context.ResolveMergeMode(TenantTreeId).Returns(LatticeMergeMode.LwwRegister);
        var innerFactory = Substitute.For<IGrainFactory>();
        innerFactory.GetGrain<IReplicationDeadLetterGrain>(TenantTreeId).Returns(grain);
        var options = Substitute.For<Microsoft.Extensions.Options.IOptionsMonitor<LatticeReplicationOptions>>();
        var replicationOptions = new LatticeReplicationOptions { ClusterId = "receiver" };
        options.CurrentValue.Returns(replicationOptions);
        options.Get(Arg.Any<string>()).Returns(replicationOptions);
        var applier = new ReplicationApplier(
            innerFactory,
            options,
            replicationContext: context,
            tenantIsolationGate: gate);
        var seam = new LatticeReplicationDeadLetters(grainFactory, applier);

        var result = await seam.ReplayAsync(TenantTreeId, 7);

        Assert.Multiple(() =>
        {
            Assert.That(result?.TenantIsolationRefused, Is.True);
            Assert.That(result?.Applied, Is.False);
        });
        await gate.Received(1).EvaluateAsync(
            TenantTreeId,
            null,
            Arg.Any<CancellationToken>());
        await grain.DidNotReceive().RemoveReplayedAsync(7, Arg.Any<CancellationToken>());
    }

    [Test]
    public void DiscardAsync_throws_on_null_or_empty_tree_id()
    {
        var (seam, _) = Build();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await seam.DiscardAsync(null!, 1, CancellationToken.None), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await seam.DiscardAsync("", 1, CancellationToken.None), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void ReplayAsync_throws_on_null_or_empty_tree_id()
    {
        var (seam, _) = Build();

        Assert.That(async () => await seam.ReplayAsync(null!, 1, CancellationToken.None), Throws.InstanceOf<ArgumentException>());
        Assert.That(async () => await seam.ReplayAsync("", 1, CancellationToken.None), Throws.InstanceOf<ArgumentException>());
    }
}
