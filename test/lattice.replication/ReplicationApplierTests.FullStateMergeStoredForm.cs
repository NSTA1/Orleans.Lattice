using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression tests for issue #2813: a full-state merge must fold the
/// <em>stored</em> local value, never the value the client-facing read
/// boundary hands back.
/// <para>
/// <see cref="ILattice.GetWithVersionAsync"/> is a client read and therefore
/// runs the read-path value decoder, which strips the per-value schema
/// envelope and - under an active schema - upcasts the payload. The applier's
/// full-state merge is a read-merge-write loop, so reading through that
/// boundary folded a decoded value and wrote the fold straight back: the
/// stored row silently lost its envelope, and the storage form of a key
/// depended on whether it happened to pre-exist the merge (the verbatim
/// first-write branch installs the peer bytes untouched, while the merge
/// branch re-serialised a decoded fold). That also performs a fold-time
/// upcast, which <see cref="ILatticeEnvelopeCodec"/> forbids because it makes
/// WAL replay non-deterministic.
/// </para>
/// <para>
/// The fix routes both merge sites through
/// <see cref="IReplicationApplyGrain.ReadStoredWithVersionAsync"/>, the
/// non-decoding apply-side read. These tests drive genuinely different bytes
/// down the two seams so an assertion cannot pass by coincidence: the stored
/// value and the decoded value hold different members, and the merge output
/// must contain the stored member and never the decoded one.
/// </para>
/// </summary>
public partial class ReplicationApplierTests
{
    private static readonly byte[] StoredOnlyMember = [0xAA];
    private static readonly byte[] DecodedOnlyMember = [0xBB];
    private static readonly byte[] IncomingMember = [0xCC];

    [Test]
    public async Task ApplyAsync_typed_full_state_merge_folds_the_stored_value_not_the_decoded_one()
    {
        var (applier, lattice, apply, _) = CreateTypedCrdtApplier(LatticeMergeMode.OrSet);

        // The stored row and the decoder's output differ by construction, so
        // the merge result names which seam the applier actually read.
        apply.ReadStoredWithVersionAsync("k")
            .Returns(new VersionedValue
            {
                Value = EncodeOrSet(s => s.Add(StoredOnlyMember, "site-a", 1)),
                Version = Hlc(5),
            });
        lattice.GetWithVersionAsync("k", Arg.Any<CancellationToken>())
            .Returns(new VersionedValue
            {
                Value = EncodeOrSet(s => s.Add(DecodedOnlyMember, "site-a", 1)),
                Version = Hlc(5),
            });

        byte[]? written = null;
        lattice.SetIfVersionAsync("k", Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(ci => { written = ci.ArgAt<byte[]>(1); return true; });

        var result = await applier.ApplyAsync(SnapshotEntry(
            "k",
            Hlc(20),
            LatticeMergeMode.OrSet,
            EncodeOrSet(s => s.Add(IncomingMember, "site-b", 1))));

        Assert.That(result.Applied, Is.True);
        Assert.That(written, Is.Not.Null);
        var merged = JsonLatticeSerializer<OrSet>.Default.Deserialize(written!);
        Assert.Multiple(() =>
        {
            Assert.That(
                merged.Contains(StoredOnlyMember),
                Is.True,
                "the fold must be taken over the stored value");
            Assert.That(
                merged.Contains(DecodedOnlyMember),
                Is.False,
                "the decoded (envelope-stripped, possibly upcast) value must never reach the fold");
            Assert.That(merged.Contains(IncomingMember), Is.True, "the peer state must still be merged in");
        });
    }

    [Test]
    public async Task ApplyAsync_typed_full_state_merge_uses_the_stored_read_version_for_the_cas()
    {
        var (applier, lattice, apply, _) = CreateTypedCrdtApplier(LatticeMergeMode.OrSet);

        apply.ReadStoredWithVersionAsync("k")
            .Returns(new VersionedValue
            {
                Value = EncodeOrSet(s => s.Add(StoredOnlyMember, "site-a", 1)),
                Version = Hlc(5),
            });
        lattice.GetWithVersionAsync("k", Arg.Any<CancellationToken>())
            .Returns(new VersionedValue
            {
                Value = EncodeOrSet(s => s.Add(DecodedOnlyMember, "site-a", 1)),
                Version = Hlc(9),
            });

        HybridLogicalClock? expected = null;
        lattice.SetIfVersionAsync("k", Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(ci => { expected = ci.ArgAt<HybridLogicalClock>(2); return true; });

        await applier.ApplyAsync(SnapshotEntry(
            "k",
            Hlc(20),
            LatticeMergeMode.OrSet,
            EncodeOrSet(s => s.Add(IncomingMember, "site-b", 1))));

        Assert.That(
            expected,
            Is.EqualTo(Hlc(5)),
            "the compare-and-swap must be anchored to the version the stored read observed");
    }

    [Test]
    public async Task ApplyAsync_typed_full_state_merge_never_reads_through_the_client_facing_surface()
    {
        var (applier, lattice, apply, _) = CreateTypedCrdtApplier(LatticeMergeMode.OrSet);

        apply.ReadStoredWithVersionAsync("k")
            .Returns(new VersionedValue
            {
                Value = EncodeOrSet(s => s.Add(StoredOnlyMember, "site-a", 1)),
                Version = Hlc(5),
            });

        await applier.ApplyAsync(SnapshotEntry(
            "k",
            Hlc(20),
            LatticeMergeMode.OrSet,
            EncodeOrSet(s => s.Add(IncomingMember, "site-b", 1))));

        await apply.Received(1).ReadStoredWithVersionAsync("k");
        await lattice.DidNotReceiveWithAnyArgs().GetWithVersionAsync(default!, default);
    }

    [Test]
    public async Task ApplyAsync_ormap_full_state_merge_folds_the_stored_value_not_the_decoded_one()
    {
        var (applier, lattice, apply, _) = CreateOrMapApplier();

        apply.ReadStoredWithVersionAsync("k")
            .Returns(new VersionedValue { Value = OrMapState(("stored", "site-a", 5)), Version = Hlc(4) });
        lattice.GetWithVersionAsync("k", Arg.Any<CancellationToken>())
            .Returns(new VersionedValue { Value = OrMapState(("decoded", "site-a", 5)), Version = Hlc(4) });

        byte[]? written = null;
        lattice.SetIfVersionAsync("k", Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(ci => { written = ci.ArgAt<byte[]>(1); return true; });

        var result = await applier.ApplyAsync(
            OrMapEntry("k", Hlc(10)) with { Value = OrMapState(("incoming", "site-b", 2)) });

        Assert.That(result.Applied, Is.True);
        Assert.That(written, Is.Not.Null);
        var merged = (OrMap<string, PnCounter>)OrMapShape.DeserializeState(written!);
        var keys = merged.Keys().ToHashSet(StringComparer.Ordinal);
        Assert.Multiple(() =>
        {
            Assert.That(keys, Does.Contain("stored"), "the fold must be taken over the stored value");
            Assert.That(
                keys,
                Does.Not.Contain("decoded"),
                "the decoded (envelope-stripped, possibly upcast) value must never reach the fold");
            Assert.That(keys, Does.Contain("incoming"), "the peer state must still be merged in");
        });
        await lattice.DidNotReceiveWithAnyArgs().GetWithVersionAsync(default!, default);
    }
}
