using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5470: <c>QuiesceForMoveAsync</c> reports the source tail the move
/// coordinator then copies, so it must make that tail readable first. A provider
/// may count a returned append in <see cref="IWalStorageProvider.GetHighestOffsetAsync"/>
/// before publishing it to readers (the Azure Table provider until the batch's
/// manifest commit lands), and only the silo that wrote knows what it has yet to
/// publish.
/// </summary>
public partial class WalShardGrainTests
{
    [Test]
    public async Task QuiesceForMoveAsync_makes_the_reported_tail_readable_before_reporting_it()
    {
        var provider = new DeferredPublicationWalStorageProvider();
        var grain = await CreateGrainAsync(provider);
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        var unpublished = await provider.ReadEncodedAsync(
            TreeId, 0, -1, 100, CreateDefaultEncoder(), CancellationToken.None);
        Assert.That(unpublished.Offsets.Length, Is.EqualTo(0),
            "precondition: the appends are durable and counted, but not yet readable");

        var result = await grain.QuiesceForMoveAsync(
            expectedPlacementVersion: 0, lease: TimeSpan.FromSeconds(30), CancellationToken.None);

        var readable = await provider.ReadEncodedAsync(
            TreeId, 0, -1, 100, CreateDefaultEncoder(), CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(result.Quiesced, Is.True);
            Assert.That(result.HighestOffsetInclusive, Is.EqualTo(2L));
            Assert.That(readable.Offsets.ToArray(), Is.EqualTo(new long[] { 0, 1, 2 }),
                "every offset up to the reported tail is readable when the quiesce returns");
            Assert.That(provider.Calls.LastIndexOf("barrier"), Is.LessThan(provider.Calls.LastIndexOf("highest")),
                "the tail is read only after the barrier has published it");
        });
    }

    [Test]
    public async Task QuiesceForMoveAsync_surfaces_a_failed_publication_instead_of_reporting_the_tail()
    {
        var provider = new DeferredPublicationWalStorageProvider
        {
            PublicationFault = new InvalidOperationException("manifest commit failed"),
        };
        var grain = await CreateGrainAsync(provider);
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        Assert.That(
            async () => await grain.QuiesceForMoveAsync(0, TimeSpan.FromSeconds(30), CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("manifest commit failed"));
    }

    /// <summary>
    /// An in-memory WAL provider that, like the Azure Table provider under
    /// pipelined phase-2 commits, counts a returned append in its highest offset
    /// at once but serves it to reads only after
    /// <see cref="WaitForAppendVisibilityAsync"/> publishes it.
    /// </summary>
    private sealed class DeferredPublicationWalStorageProvider : IWalStorageProvider
    {
        private readonly IWalStorageProvider _inner = new InMemoryWalStorageProvider();
        private readonly object _gate = new();
        private long _publishedThrough = -1;

        /// <summary>When set, the barrier fails with this instead of publishing.</summary>
        public Exception? PublicationFault { get; init; }

        /// <summary>The barrier and highest-offset calls, in order.</summary>
        public List<string> Calls { get; } = new();

        public async Task WaitForAppendVisibilityAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            lock (_gate) Calls.Add("barrier");
            if (PublicationFault is { } fault)
            {
                throw fault;
            }
            var highest = await _inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);
            lock (_gate) _publishedThrough = Math.Max(_publishedThrough, highest);
        }

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            lock (_gate) Calls.Add("highest");
            return _inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);
        }

        public async Task<WalShardEncodedPage> ReadEncodedAsync(
            string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries,
            IWalRecordEncoder encoder, CancellationToken cancellationToken)
        {
            var page = await _inner.ReadEncodedAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, encoder, cancellationToken);
            long published;
            lock (_gate) published = _publishedThrough;
            var visible = 0;
            var offsets = page.Offsets.Span;
            while (visible < offsets.Length && offsets[visible] <= published)
            {
                visible++;
            }
            return new WalShardEncodedPage
            {
                EncodedEntries = page.EncodedEntries[..visible],
                Offsets = page.Offsets[..visible],
            };
        }

        public async IAsyncEnumerable<WalEntry> ReadAsync(
            string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var entry in _inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken))
            {
                long published;
                lock (_gate) published = _publishedThrough;
                if (entry.Offset > published)
                {
                    yield break;
                }
                yield return entry;
            }
        }

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => _inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);

        public Task AppendEncodedBatchAsync(
            string treeId, int shardIndex, ReadOnlyMemory<ArraySegment<byte>> encodedEntries,
            ReadOnlyMemory<long> offsets, IWalRecordEncoder encoder, CancellationToken cancellationToken)
            => _inner.AppendEncodedBatchAsync(treeId, shardIndex, encodedEntries, offsets, encoder, cancellationToken);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => _inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => _inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }
}
