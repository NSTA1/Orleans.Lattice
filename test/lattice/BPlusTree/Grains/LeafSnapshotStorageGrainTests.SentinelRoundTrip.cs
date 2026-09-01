using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue 1888: <c>LeafSnapshotBlob.SnapshotOffset</c> used
/// a <c>-1</c> negative-sentinel initializer meaning "nothing captured", over a
/// domain in which WAL offset <c>0</c> is a legitimate captured prefix. A leaf that
/// snapshotted its projection consistent through offset <c>0</c> wrote <c>0</c>,
/// had it omitted by a grain-storage serializer that drops type defaults, and
/// reloaded claiming nothing had been captured - discarding the blob on the very
/// path that exists to survive a cold restart.
/// <para>
/// The member is nullable so that absent means <see langword="null"/> means unset,
/// and a captured offset of <c>0</c> is no longer indistinguishable from no capture
/// at all. Readers continue to fold a legacy <c>-1</c> into the same "unset"
/// reading, so blobs persisted by an older build are unaffected.
/// </para>
/// </summary>
public sealed partial class LeafSnapshotStorageGrainTests
{
    /// <summary>
    /// The exposed shape is a legacy blob - one persisted before the per-partition
    /// coverage array existed - because its scalar offset is the only coverage it
    /// carries. A blob that also carries <c>SnapshotOffsetsByPartition</c> survives
    /// by accident: the array is a reference type, so it is never omitted, and
    /// <c>HasCapturedPrefix</c> finds the coverage there instead. That accident is
    /// exactly why this defect can sit unnoticed, so the legacy shape is the one
    /// asserted.
    /// </summary>
    [Test]
    public async Task LoadAsync_still_finds_a_legacy_blob_captured_at_wal_offset_zero_after_a_default_omitting_reload()
    {
        var captured = NewBlob(0L, ("a", [1, 2]));
        Assume.That(captured.SnapshotOffsetsByPartition, Is.Null,
            "The legacy shape under test carries no per-partition array; if it gained one this "
            + "test would pass through that array's coverage and stop exercising the scalar.");

        var reloaded = DefaultOmittingStateRoundTrip.Simulate(captured);
        var (grain, _) = CreateGrain(new FakePersistentState<LeafSnapshotBlob> { State = reloaded });

        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(loaded, Is.Not.Null,
                "RED before issue 1888: the captured offset 0 is dropped by the omitting serializer "
                + "and the -1 initializer resurrects the 'nothing captured' sentinel, so the sole "
                + "durable copy of the checkpointed prefix is reported absent and discarded.");
            Assert.That(loaded!.SnapshotOffset, Is.Zero,
                "The offset the snapshot is consistent through must survive as 0, not as 'unset'.");
            Assert.That(loaded.GetRowCount(), Is.EqualTo(1));
        });
    }

    /// <summary>
    /// The complementary direction: a blob that genuinely captured nothing must
    /// keep reading as "nothing captured" after the round trip, so the repair does
    /// not turn an empty row into a claim of coverage at offset zero - which would
    /// authorise the coverage-gated WAL GC to trim a prefix no snapshot backs.
    /// </summary>
    [Test]
    public async Task LoadAsync_still_reports_nothing_captured_for_an_unset_blob_after_a_default_omitting_reload()
    {
        var reloaded = DefaultOmittingStateRoundTrip.Simulate(new LeafSnapshotBlob());
        var (grain, _) = CreateGrain(new FakePersistentState<LeafSnapshotBlob> { State = reloaded });

        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.That(loaded, Is.Null,
            "An uncaptured blob must remain absent; 'unset' and 'captured through offset 0' are "
            + "two distinct readings and only the latter may report coverage.");
    }

    /// <summary>
    /// A blob persisted by an older build carries the literal <c>-1</c> sentinel
    /// (non-default for <c>long</c>, so it was written rather than omitted). The new
    /// readers must fold that value into the same "nothing captured" reading as
    /// <see langword="null"/>, or a legacy blob would start claiming coverage at a
    /// negative offset.
    /// </summary>
    [Test]
    public async Task LoadAsync_treats_a_legacy_minus_one_scalar_as_nothing_captured()
    {
        var legacy = NewBlob(-1L, ("a", [1]));
        var (grain, _) = CreateGrain(new FakePersistentState<LeafSnapshotBlob> { State = legacy });

        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.That(loaded, Is.Null,
            "Blobs persisted before the member became nullable carry -1 explicitly; that must keep "
            + "meaning 'nothing captured' rather than becoming a claim of coverage.");
    }
}
