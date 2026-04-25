using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplicationMutationObserverTests
{
    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(string clusterId)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var opts = new LatticeReplicationOptions { ClusterId = clusterId };
        monitor.CurrentValue.Returns(opts);
        return monitor;
    }

    private sealed class CapturingSink : IReplogSink
    {
        public List<ReplogEntry> Entries { get; } = new();
        public Task WriteAsync(ReplogEntry entry, CancellationToken cancellationToken)
        {
            Entries.Add(entry);
            return Task.CompletedTask;
        }
    }

    [Test]
    public async Task Set_mutation_emits_entry_with_value_and_local_origin()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"));
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3 },
            Timestamp = ts,
            ExpiresAtTicks = 100L,
        }, CancellationToken.None);

        Assert.That(sink.Entries, Has.Count.EqualTo(1));
        var e = sink.Entries[0];
        Assert.Multiple(() =>
        {
            Assert.That(e.TreeId, Is.EqualTo("tree"));
            Assert.That(e.Op, Is.EqualTo(ReplogOp.Set));
            Assert.That(e.Key, Is.EqualTo("k"));
            Assert.That(e.EndExclusiveKey, Is.Null);
            Assert.That(e.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(e.Timestamp, Is.EqualTo(ts));
            Assert.That(e.IsTombstone, Is.False);
            Assert.That(e.ExpiresAtTicks, Is.EqualTo(100L));
            Assert.That(e.OriginClusterId, Is.EqualTo("site-a"));
        });
    }

    [Test]
    public async Task Delete_mutation_emits_entry_with_tombstone_flag()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Delete,
            Key = "gone",
            IsTombstone = true,
        }, CancellationToken.None);

        var e = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(e.Op, Is.EqualTo(ReplogOp.Delete));
            Assert.That(e.Key, Is.EqualTo("gone"));
            Assert.That(e.IsTombstone, Is.True);
            Assert.That(e.Value, Is.Null);
            Assert.That(e.OriginClusterId, Is.EqualTo("site-a"));
        });
    }

    [Test]
    public async Task DeleteRange_mutation_emits_entry_with_end_exclusive_key()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            IsTombstone = true,
        }, CancellationToken.None);

        var e = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(e.Op, Is.EqualTo(ReplogOp.DeleteRange));
            Assert.That(e.Key, Is.EqualTo("a"));
            Assert.That(e.EndExclusiveKey, Is.EqualTo("z"));
            Assert.That(e.IsTombstone, Is.True);
        });
    }

    [Test]
    public async Task Existing_origin_cluster_id_is_preserved_for_remote_replays()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 7 },
            OriginClusterId = "site-b",
        }, CancellationToken.None);

        Assert.That(sink.Entries.Single().OriginClusterId, Is.EqualTo("site-b"));
    }

    [Test]
    public async Task Local_cluster_id_is_forwarded_verbatim_when_origin_unset()
    {
        // Real registrations are guarded by LatticeReplicationOptionsValidator,
        // which rejects an empty ClusterId before any observer call. This unit
        // test bypasses validation by injecting options directly, so the
        // observer's documented behaviour is to forward the configured value
        // as-is rather than substitute null.
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-x"));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 7 },
        }, CancellationToken.None);

        Assert.That(sink.Entries.Single().OriginClusterId, Is.EqualTo("site-x"));
    }

    [Test]
    public void Unknown_mutation_kind_throws_invalid_operation()
    {
        var observer = new ReplicationMutationObserver(new CapturingSink(), Monitor("site-a"));

        Assert.That(
            async () => await observer.OnMutationAsync(
                new LatticeMutation { TreeId = "t", Kind = (MutationKind)999, Key = "k" },
                CancellationToken.None),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task Sink_cancellation_token_is_forwarded()
    {
        var sink = Substitute.For<IReplogSink>();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"));
        using var cts = new CancellationTokenSource();

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Set,
            Key = "k",
            Value = Array.Empty<byte>(),
        }, cts.Token);

        await sink.Received(1).WriteAsync(Arg.Any<ReplogEntry>(), cts.Token);
    }
}

