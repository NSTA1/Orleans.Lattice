using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Integration tests that exercise the dead-letter pipeline end-to-end
/// against a live single-silo <see cref="TestCluster"/>. The decorator
/// is constructed manually around a substituted inner
/// <see cref="IReplicationApplier"/> so the test can deterministically
/// inject apply failures, but every other component
/// (the per-tree <c>IReplicationDeadLetterGrain</c>, the system tree
/// backing it, the HWM grain, and the public
/// <see cref="ILatticeReplicationDeadLetters"/> seam) runs over the
/// real Orleans runtime resolved from the cluster.
/// </summary>
[TestFixture]
[Category("Integration")]
public class DeadLetterIntegrationTests
{
    private const string TreeId = "dlq-itest";
    private const string OriginCluster = "remote";
    private const string LocalCluster = "site-a";
    private const int MaxApplyRetries = 3;
    private const int DeadLetterCapacity = 10;

    private TestCluster _cluster = null!;
    private IOptionsMonitor<LatticeReplicationOptions> _optionsMonitor = null!;
    private ILatticeReplicationDeadLetters _inspector = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        // The seam and decorator only need the cluster's IGrainFactory
        // plus the same options the silo was configured with. We build
        // those directly rather than reach into silo-side DI so the
        // test stays loosely coupled to the cluster's internals.
        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            MaxApplyRetries = MaxApplyRetries,
            DeadLetterQueueCapacity = DeadLetterCapacity,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        monitor.CurrentValue.Returns(options);
        _optionsMonitor = monitor;

        _inspector = new LatticeReplicationDeadLetters(
            _cluster.GrainFactory,
            new ReplicationApplier(_cluster.GrainFactory, _optionsMonitor));
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = LocalCluster;
                opts.MaxApplyRetries = MaxApplyRetries;
                opts.DeadLetterQueueCapacity = DeadLetterCapacity;
            });
        }
    }

    private static WalRecord MakeEntry(string key) => new()
    {
        TreeId = TreeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1, 2, 3 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = OriginCluster,
    };

    private static IReplicationApplier AlwaysFailingInner(string message = "boom")
    {
        var inner = Substitute.For<IReplicationApplier>();
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException(message));
        return inner;
    }

    private DeadLetterTrackingReplicationApplier BuildDecorator(IReplicationApplier inner)
        => new(inner, _cluster.GrainFactory, _optionsMonitor);

    [Test]
    public async Task Apply_failure_exceeds_threshold_and_parks_entry_on_dlq_grain()
    {
        var entry = MakeEntry("itest-park");
        var decorator = BuildDecorator(AlwaysFailingInner());

        // First MaxApplyRetries - 1 attempts re-throw to surface the
        // failure to the transport. The Nth attempt parks the entry
        // and returns Applied=false.
        Assert.That(async () => await decorator.ApplyAsync(entry, CancellationToken.None), Throws.InvalidOperationException);
        Assert.That(async () => await decorator.ApplyAsync(entry, CancellationToken.None), Throws.InvalidOperationException);

        var terminal = await decorator.ApplyAsync(entry, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(terminal.Applied, Is.False);
            Assert.That(terminal.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });

        var parked = (await _inspector.ListAsync(TreeId, CancellationToken.None))
            .Where(e => e.Entry.Key == "itest-park")
            .ToList();

        Assert.That(parked, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(parked[0].Entry.OriginClusterId, Is.EqualTo(OriginCluster));
            Assert.That(parked[0].FailureReason, Is.EqualTo("boom"));
            Assert.That(parked[0].RetryCount, Is.EqualTo(MaxApplyRetries));
        });

        // HWM for (treeId, originClusterId) must have advanced past the
        // entry's timestamp so the canonical applier dedupes future
        // re-deliveries.
        var hwm = _cluster.GrainFactory.GetGrain<IReplicationHighWaterMarkGrain>(TreeId);
        var advanced = await hwm.GetAsync(OriginCluster);
        Assert.That(advanced, Is.EqualTo(entry.Timestamp));

        Assert.That(await _inspector.DiscardAsync(TreeId, parked[0].EntryId, CancellationToken.None), Is.True);
    }

    [Test]
    public async Task Successful_apply_after_one_failure_does_not_park()
    {
        var calls = 0;
        var inner = Substitute.For<IReplicationApplier>();
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ =>
            {
                calls++;
                if (calls == 1)
                {
                    throw new InvalidOperationException("transient");
                }
                return Task.FromResult(new ApplyResult { Applied = true });
            });

        var decorator = BuildDecorator(inner);
        var entry = MakeEntry("itest-transient");

        Assert.That(async () => await decorator.ApplyAsync(entry, CancellationToken.None), Throws.InvalidOperationException);
        var success = await decorator.ApplyAsync(entry, CancellationToken.None);

        Assert.That(success.Applied, Is.True);

        var entriesForKey = (await _inspector.ListAsync(TreeId, CancellationToken.None))
            .Where(e => e.Entry.Key == "itest-transient")
            .ToList();
        Assert.That(entriesForKey, Is.Empty);
    }

    [Test]
    public async Task Replay_routes_through_inner_and_removes_parked_entry()
    {
        var entry = MakeEntry("itest-replay");
        var failingInner = AlwaysFailingInner("apply-fault");
        var decorator = BuildDecorator(failingInner);
        for (var i = 0; i < MaxApplyRetries; i++)
        {
            try { await decorator.ApplyAsync(entry, CancellationToken.None); }
            catch (InvalidOperationException) { /* expected for the first MaxApplyRetries-1 calls */ }
        }

        var parked = (await _inspector.ListAsync(TreeId, CancellationToken.None))
            .Single(e => e.Entry.Key == "itest-replay");

        // Clear the recorded calls on the failing substitute so we can
        // prove the replay path bypasses the decorator's inner. The
        // seam holds its own concrete ReplicationApplier built against
        // the cluster's IGrainFactory; if the bypass were broken the
        // replay would re-invoke the failing substitute and the
        // DidNotReceive() assertion below would fail.
        failingInner.ClearReceivedCalls();

        // Replay routes through the canonical ReplicationApplier (not
        // the decorator). The parked entry's mutation never actually
        // landed (every apply attempt threw), and no snapshot floor is
        // pinned for this origin, so the canonical applier admits it and
        // reports Applied=true - the corrected #1060 semantics, where a
        // below-incremental-HWM point write is not silently deduped.
        // Either way the replay is terminal for cleanup: the parked row
        // is removed below.
        var result = await _inspector.ReplayAsync(TreeId, parked.EntryId, CancellationToken.None);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Value.Applied, Is.True);

        // Structural proof of bypass: the decorator's inner substitute
        // received zero calls during the replay. The seam built its
        // own ReplicationApplier against _cluster.GrainFactory, so the
        // failing substitute is not on the replay path.
        await failingInner.DidNotReceive().ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());

        var afterReplay = (await _inspector.ListAsync(TreeId, CancellationToken.None))
            .Where(e => e.EntryId == parked.EntryId)
            .ToList();
        Assert.That(afterReplay, Is.Empty);
    }

    [Test]
    public async Task Parked_entries_persist_across_repeated_inspection_calls()
    {
        var entry = MakeEntry("itest-persist");
        var decorator = BuildDecorator(AlwaysFailingInner("persist-fault"));
        for (var i = 0; i < MaxApplyRetries; i++)
        {
            try { await decorator.ApplyAsync(entry, CancellationToken.None); }
            catch (InvalidOperationException) { /* expected */ }
        }

        var beforeId = (await _inspector.ListAsync(TreeId, CancellationToken.None))
            .Single(e => e.Entry.Key == "itest-persist").EntryId;

        // The DLQ row lives in the system tree, so any subsequent
        // grain call (even on the same activation) reflects the
        // committed state. Issue a fresh List + Count round trip to
        // confirm the entry is durable through the public seam.
        var count = await _inspector.CountAsync(TreeId, CancellationToken.None);
        Assert.That(count, Is.GreaterThanOrEqualTo(1));

        var afterRead = (await _inspector.ListAsync(TreeId, CancellationToken.None))
            .Where(e => e.EntryId == beforeId)
            .ToList();
        Assert.That(afterRead, Has.Count.EqualTo(1));

        await _inspector.DiscardAsync(TreeId, beforeId, CancellationToken.None);
    }
}

