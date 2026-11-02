namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="WalMoveResumeCore.CopyChunkLength"/>, which splits a
/// WAL move copy page into the appends the target receives (issue #5445), and for
/// the <see cref="IWalStorageProvider.MaxAppendBatchEntries"/> default it consumes.
/// </summary>
[TestFixture]
public sealed class WalMoveResumeCoreTests
{
    private static long[] Dense(long from, int count)
    {
        var offsets = new long[count];
        for (var i = 0; i < count; i++)
        {
            offsets[i] = from + i;
        }
        return offsets;
    }

    [Test]
    public void CopyChunkLength_caps_a_dense_page_at_the_target_limit()
    {
        var page = Dense(0, 256);

        Assert.Multiple(() =>
        {
            Assert.That(WalMoveResumeCore.CopyChunkLength(page, 0, 100), Is.EqualTo(100));
            Assert.That(WalMoveResumeCore.CopyChunkLength(page, 100, 100), Is.EqualTo(100));
            Assert.That(WalMoveResumeCore.CopyChunkLength(page, 200, 100), Is.EqualTo(56));
        });
    }

    [Test]
    public void CopyChunkLength_returns_the_whole_remainder_when_the_target_is_uncapped()
    {
        var page = Dense(10, 256);

        Assert.That(WalMoveResumeCore.CopyChunkLength(page, 6, int.MaxValue), Is.EqualTo(250));
    }

    [Test]
    public void CopyChunkLength_ends_a_run_at_an_offset_gap()
    {
        // A failed flush can leave a permanent gap in the source; a provider may
        // reject a non-dense append, so the run stops at the gap.
        long[] page = [4, 5, 6, 9, 10];

        Assert.Multiple(() =>
        {
            Assert.That(WalMoveResumeCore.CopyChunkLength(page, 0, 100), Is.EqualTo(3));
            Assert.That(WalMoveResumeCore.CopyChunkLength(page, 3, 100), Is.EqualTo(2));
        });
    }

    [Test]
    public void CopyChunkLength_treats_a_non_positive_cap_as_one()
    {
        var page = Dense(0, 4);

        Assert.Multiple(() =>
        {
            Assert.That(WalMoveResumeCore.CopyChunkLength(page, 0, 0), Is.EqualTo(1));
            Assert.That(WalMoveResumeCore.CopyChunkLength(page, 0, -5), Is.EqualTo(1));
        });
    }

    [Test]
    public void CopyChunkLength_rejects_a_start_outside_the_page()
    {
        var page = Dense(0, 4);

        Assert.Multiple(() =>
        {
            Assert.That(() => WalMoveResumeCore.CopyChunkLength(page, -1, 100),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => WalMoveResumeCore.CopyChunkLength(page, 4, 100),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => WalMoveResumeCore.CopyChunkLength(ReadOnlySpan<long>.Empty, 0, 100),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void MaxAppendBatchEntries_defaults_to_uncapped()
    {
        IWalStorageProvider provider = new InMemoryWalStorageProvider();

        Assert.That(provider.MaxAppendBatchEntries, Is.EqualTo(int.MaxValue));
    }
}
