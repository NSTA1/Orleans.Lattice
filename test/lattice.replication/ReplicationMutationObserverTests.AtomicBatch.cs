using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the atomic-batch metadata pass-through on
/// <see cref="ReplicationMutationObserver"/>. R-094 only ships the
/// schema slots and the observer's pass-through; the producer-side
/// stamping inside <c>AtomicWriteGrain.RunSagaAsync</c> lives on
/// R-095. These tests therefore inject a synthetic
/// <see cref="LatticeMutation.AtomicBatchSize"/> /
/// <see cref="LatticeMutation.AtomicBatchIndex"/> pair and assert the
/// observer mirrors them verbatim onto the emitted
/// <see cref="ReplogEntry"/>.
/// </summary>
[TestFixture]
public class ReplicationMutationObserverAtomicBatchTests
{
    private const string Tree = "tree";
    private const string LocalCluster = "site-a";

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor()
    {
        var options = new LatticeReplicationOptions { ClusterId = LocalCluster };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
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

    private sealed class AllowAllResolver : IReplicationModeResolver
    {
        public ReplicationMode? Resolve(string treeId) => ReplicationMode.LwwRegister;
    }

    private static (ReplicationMutationObserver Observer, CapturingSink Sink) CreateObserver()
    {
        var sink = new CapturingSink();
        var factory = Substitute.For<IGrainFactory>();
        var grain = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(grain);
        grain.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());
        var cache = new LocalVectorClockCache(factory);
        var observer = new ReplicationMutationObserver(sink, Monitor(), new AllowAllResolver(), cache);
        return (observer, sink);
    }

    [Test]
    public async Task Default_mutation_emits_zero_atomic_batch_size_and_index()
    {
        // Single-key non-atomic write: producer leaves both slots at 0;
        // the observer must mirror them as 0 so a downstream receiver
        // with atomic-batch delivery enabled treats the entry as a
        // point write.
        var (observer, sink) = CreateObserver();

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
        }, CancellationToken.None);

        var entry = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.AtomicBatchSize, Is.EqualTo(0));
            Assert.That(entry.AtomicBatchIndex, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task Mutation_atomic_batch_size_and_index_flow_through_to_replog_entry()
    {
        // Producer-side stamping (R-095) is out of scope for R-094;
        // here we inject a synthetic (Size, Index) pair on the
        // mutation directly and assert the observer mirrors both
        // verbatim. R-095 will cover the AtomicWriteGrain saga
        // capture-once path that produces this shape end-to-end.
        var (observer, sink) = CreateObserver();

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            TransactionId = Guid.NewGuid(),
            AtomicBatchSize = 5,
            AtomicBatchIndex = 2,
        }, CancellationToken.None);

        var entry = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.AtomicBatchSize, Is.EqualTo(5));
            Assert.That(entry.AtomicBatchIndex, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task Atomic_batch_metadata_independent_of_origin_and_mode()
    {
        // The slot is independent of OriginClusterId: a remote-origin
        // emit (i.e. an inbound replay) still preserves the size/index
        // verbatim. The observer's existing origin / mode plumbing is
        // tested elsewhere; here we just assert the new slots are not
        // accidentally cleared by either branch.
        var (observer, sink) = CreateObserver();

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            OriginClusterId = "site-remote",
            TransactionId = Guid.NewGuid(),
            AtomicBatchSize = 3,
            AtomicBatchIndex = 0,
        }, CancellationToken.None);

        var entry = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.OriginClusterId, Is.EqualTo("site-remote"));
            Assert.That(entry.AtomicBatchSize, Is.EqualTo(3));
            Assert.That(entry.AtomicBatchIndex, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task Multi_emit_batch_round_trips_distinct_indices()
    {
        // Synthetic batch of N=3 emits, indices 0..2, all sharing the
        // same TransactionId. Each emit's pair is mirrored
        // independently on the corresponding ReplogEntry, in order of
        // observation.
        var (observer, sink) = CreateObserver();
        var txId = Guid.NewGuid();

        for (var i = 0; i < 3; i++)
        {
            await observer.OnMutationAsync(new LatticeMutation
            {
                TreeId = Tree,
                Kind = MutationKind.Set,
                Key = $"k-{i}",
                Value = new byte[] { (byte)i },
                TransactionId = txId,
                AtomicBatchSize = 3,
                AtomicBatchIndex = i,
            }, CancellationToken.None);
        }

        Assert.That(sink.Entries, Has.Count.EqualTo(3));
        for (var i = 0; i < 3; i++)
        {
            Assert.Multiple(() =>
            {
                Assert.That(sink.Entries[i].AtomicBatchSize, Is.EqualTo(3));
                Assert.That(sink.Entries[i].AtomicBatchIndex, Is.EqualTo(i));
            });
        }
    }

    [Test]
    public async Task Delete_mutation_preserves_atomic_batch_metadata()
    {
        // The observer dispatches Set / Delete / DeleteRange via the
        // same ReplogEntry construction; the atomic-batch slot copy
        // happens after the Op switch, so a Delete emit must mirror
        // the slots identically to a Set emit.
        var (observer, sink) = CreateObserver();

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Delete,
            Key = "k",
            IsTombstone = true,
            TransactionId = Guid.NewGuid(),
            AtomicBatchSize = 4,
            AtomicBatchIndex = 1,
        }, CancellationToken.None);

        var entry = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.Op, Is.EqualTo(ReplogOp.Delete));
            Assert.That(entry.AtomicBatchSize, Is.EqualTo(4));
            Assert.That(entry.AtomicBatchIndex, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task DeleteRange_mutation_preserves_atomic_batch_metadata()
    {
        // The observer's DeleteRange branch builds the same
        // ReplogEntry shape as Set / Delete and the atomic-batch slot
        // copy happens after the Op switch. A range emit that is part
        // of an atomic transaction (e.g. saga-issued bulk delete that
        // shares a transaction with point writes) must mirror the
        // (Size, Index) pair verbatim onto the resulting DeleteRange
        // entry.
        var (observer, sink) = CreateObserver();

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            IsTombstone = true,
            TransactionId = Guid.NewGuid(),
            AtomicBatchSize = 4,
            AtomicBatchIndex = 1,
        }, CancellationToken.None);

        var entry = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.Op, Is.EqualTo(ReplogOp.DeleteRange));
            Assert.That(entry.Key, Is.EqualTo("a"));
            Assert.That(entry.EndExclusiveKey, Is.EqualTo("z"));
            Assert.That(entry.AtomicBatchSize, Is.EqualTo(4));
            Assert.That(entry.AtomicBatchIndex, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Default_mutation_emits_empty_transaction_id()
    {
        // R-097 widens ReplogEntry with a [Id(16)] TransactionId slot
        // mirrored from LatticeMutation.TransactionId. A single-key
        // non-atomic write whose producer leaves the slot at the
        // default (Guid.Empty) must surface as Guid.Empty on the
        // resulting entry so a receiver with AtomicBatchDelivery
        // enabled treats it as a point write rather than routing it
        // through the TxApplyBuffer.
        var (observer, sink) = CreateObserver();

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
        }, CancellationToken.None);

        var entry = sink.Entries.Single();
        Assert.That(entry.TransactionId, Is.EqualTo(Guid.Empty));
    }

    [Test]
    public async Task Mutation_transaction_id_flows_through_to_replog_entry()
    {
        // Direct TransactionId pass-through: a producer that stamps
        // a non-empty TransactionId on the LatticeMutation (the saga
        // path under R-095) must see that exact Guid mirrored onto
        // every emitted ReplogEntry so the receiver-side
        // TxApplyBuffer can group siblings under the same key.
        var (observer, sink) = CreateObserver();
        var txId = Guid.NewGuid();

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            TransactionId = txId,
            AtomicBatchSize = 1,
            AtomicBatchIndex = 0,
        }, CancellationToken.None);

        var entry = sink.Entries.Single();
        Assert.That(entry.TransactionId, Is.EqualTo(txId));
    }

    [Test]
    public async Task Atomic_batch_shares_transaction_id_across_emits()
    {
        // The defining R-097 invariant on the observer side: every
        // emit of an atomic transaction carries the identical
        // TransactionId so the receiver-side buffer keys all siblings
        // under one (origin, txid) bucket. Indices differ, the txid
        // does not.
        var (observer, sink) = CreateObserver();
        var txId = Guid.NewGuid();

        for (var i = 0; i < 3; i++)
        {
            await observer.OnMutationAsync(new LatticeMutation
            {
                TreeId = Tree,
                Kind = MutationKind.Set,
                Key = $"k{i}",
                Value = new byte[] { (byte)i },
                TransactionId = txId,
                AtomicBatchSize = 3,
                AtomicBatchIndex = i,
            }, CancellationToken.None);
        }

        Assert.That(sink.Entries, Has.Count.EqualTo(3));
        Assert.Multiple(() =>
        {
            Assert.That(sink.Entries.Select(e => e.TransactionId), Is.All.EqualTo(txId));
            Assert.That(sink.Entries.Select(e => e.AtomicBatchIndex), Is.EquivalentTo(new[] { 0, 1, 2 }));
        });
    }
}
