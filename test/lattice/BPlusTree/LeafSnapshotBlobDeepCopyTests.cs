using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;
using Orleans.Serialization.Cloning;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Guards the <c>[Immutable]</c> marking on <see cref="LeafSnapshotBlob"/>'s row
/// payload carriers (issue #2481).
/// <para>
/// <c>LeafSnapshotStorageGrain.LoadAsync</c> hands its persisted blob to a leaf
/// that is normally co-located, so Orleans deep-copies the response. The row
/// payload is essentially the whole mass of a snapshot, so that copy is a second
/// contiguous allocation the size of the payload at the exact moment the first
/// is already resident - which is what exhausts the heap on a large leaf and
/// forces it to activate cold without durable coverage, pinning its tree's WAL
/// trim floor at zero.
/// </para>
/// <para>
/// These fixtures assert the copier SHARES the payload arrays and still COPIES
/// the blob shell. Both halves matter: sharing the payload is the fix, and
/// copying the shell is what keeps it safe, because the blob type is genuinely
/// mutated after construction (the storage grain back-fills
/// <see cref="LeafSnapshotBlob.SnapshotBytes"/> on first byte-size read) and so
/// must never be marked immutable wholesale.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafSnapshotBlobDeepCopyTests
{
    private static DeepCopier<LeafSnapshotBlob> BuildCopier()
    {
        var services = new ServiceCollection();
        services.AddSerializer(builder =>
            builder.AddAssembly(typeof(LeafSnapshotBlob).Assembly));
        var provider = services.BuildServiceProvider();
        return provider.GetRequiredService<DeepCopier<LeafSnapshotBlob>>();
    }

    private static LeafSnapshotBlob BuildBlob()
    {
        var rows = new[]
        {
            new LeafSnapshotRow(
                "alpha",
                LwwValue<byte[]>.Create(
                    [1, 2, 3],
                    new HybridLogicalClock { WallClockTicks = 10L })),
        };

        return new LeafSnapshotBlob
        {
            SnapshotOffset = 7L,
            CapturedAtTicks = 1234L,
            SnapshotBytes = 99L,
            Rows = rows,
            EncodedRows = LeafSnapshotCodec.Encode(rows),
            SnapshotOffsetsByPartition = [7L, -1L],
        };
    }

    [Test]
    public void DeepCopy_shares_the_encoded_row_frame_rather_than_reallocating_it()
    {
        var copier = BuildCopier();
        var original = BuildBlob();

        var copy = copier.Copy(original);

        Assert.That(
            copy.EncodedRows,
            Is.SameAs(original.EncodedRows),
            "EncodedRows must be shared, not copied. A fresh array here is a second "
            + "contiguous allocation the size of the whole snapshot on the co-located "
            + "load path, which is the allocation that exhausts the heap in issue #2481.");
    }

    [Test]
    public void DeepCopy_shares_the_legacy_row_list_rather_than_reallocating_it()
    {
        var copier = BuildCopier();
        var original = BuildBlob();

        var copy = copier.Copy(original);

        Assert.That(
            copy.Rows,
            Is.SameAs(original.Rows),
            "Rows must be shared, not copied. A legacy blob captured before the binary "
            + "frame existed carries its whole payload here, so leaving this slot copied "
            + "moves the same exhaustion onto pre-frame blobs instead of fixing it.");
    }

    [Test]
    public void DeepCopy_still_produces_a_distinct_blob_instance()
    {
        var copier = BuildCopier();
        var original = BuildBlob();

        var copy = copier.Copy(original);

        Assert.That(
            copy,
            Is.Not.SameAs(original),
            "the blob SHELL must still be copied. LoadAsync returns the storage grain's "
            + "live persisted state, so aliasing the shell would let a caller mutate it - "
            + "and the grain itself back-fills SnapshotBytes after handing it out. This is "
            + "why the payload members are marked and the TYPE is not.");
    }

    [Test]
    public void DeepCopy_preserves_scalar_state_and_per_partition_coverage()
    {
        var copier = BuildCopier();
        var original = BuildBlob();

        var copy = copier.Copy(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.SnapshotOffset, Is.EqualTo(7L));
            Assert.That(copy.CapturedAtTicks, Is.EqualTo(1234L));
            Assert.That(copy.SnapshotBytes, Is.EqualTo(99L));
            Assert.That(copy.SnapshotOffsetsByPartition, Is.EqualTo(new[] { 7L, -1L }));
        });
    }

    [Test]
    public void Copied_blob_still_reads_its_rows_back_in_full()
    {
        var copier = BuildCopier();
        var original = BuildBlob();

        var copy = copier.Copy(original);

        Assert.That(copy.ValidateRowPayload(), Is.True);
        Assert.That(copy.GetRowCount(), Is.EqualTo(1));

        var keys = new List<string>();
        foreach (var row in copy.EnumerateRows())
        {
            keys.Add(row.Key);
        }
        Assert.That(keys, Is.EqualTo(new[] { "alpha" }));
    }
}
