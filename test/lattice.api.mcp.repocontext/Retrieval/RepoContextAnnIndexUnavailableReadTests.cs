using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Issue #3905 at the handle: a converged approximate index whose store could not
/// serve a consistent read during its open was recorded as
/// <c>discarded/unloadable_record</c> and rebuilt from zero. The open must defer
/// instead, keep the durable index, and converge on it once the store answers -
/// and a discard that is genuinely warranted must say what it destroyed.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class RepoContextAnnIndexUnavailableReadTests
{
    private const string RepoId = "acme";
    private const int Vectors = 24;

    private static readonly EmbeddingSpaceTag Space = new("test-model", 8, VectorNormalization.UnitL2);

    private static string Prefix => RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static RepoContextAnnOptions Options() => new()
    {
        MinimumTrainingCount = 8,
        PartitionCount = 4,
        Probes = 4,
        FlushAfterUpdates = 1,
        IngestBatchSize = 16,
        MaxItemsPerChunk = 8,
    };

    [Test]
    public async Task An_open_whose_store_omits_a_record_it_holds_defers_and_converges_on_the_durable_index()
    {
        var (store, source) = await BuiltAsync();
        var records = store.Inner.Count;
        store.HiddenFromBatchReads.Add(VectorChunkKeys(store.Inner)[^1]);

        using var reporter = new RepoContextAnnIndexLoadReporter();
        var logger = Substitute.For<ILogger>();
        using var handle = NewHandle(source, store, reporter, logger);

        Assert.DoesNotThrowAsync(
            async () => await handle.AdvanceAsync(Ct),
            "An inconsistent read is back-pressure: the open yields and the coordinator retries it.");

        var deferred = reporter.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(deferred.Deferred, Is.EqualTo(1));
            Assert.That(deferred.Discarded, Is.Zero,
                "Recording a discard here is issue #3905: a converged index destroyed by a read that failed.");
            Assert.That(deferred.Faulted, Is.Zero, "A deferral must not page as a fault.");
            Assert.That(store.Inner.Count, Is.EqualTo(records), "Every durable record must survive the deferral.");
            Assert.That(handle.IsServing, Is.False);
            Assert.That(Warnings(logger).Single(), Does.Contain("was kept, not discarded"));
        });

        store.HiddenFromBatchReads.Clear();
        await handle.EnsureBuiltAsync(Ct);

        var converged = reporter.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(handle.IsServing, Is.True);
            Assert.That(handle.RestoredFromDurableState, Is.True,
                "The durable index is adopted, not rebuilt from source.");
            Assert.That(handle.Progress.VectorsIndexed, Is.EqualTo(Vectors));
            Assert.That(converged.Resumed, Is.EqualTo(1),
                "The retry continued the completed key walk rather than starting again.");
            Assert.That(converged.Discarded, Is.Zero);
        });
    }

    [Test]
    public async Task A_discard_of_a_converged_index_warns_with_the_generation_and_vector_count_it_destroyed()
    {
        var (store, source) = await BuiltAsync();
        await store.Inner.DeleteAsync([VectorChunkKeys(store.Inner)[^1]], Ct);

        using var reporter = new RepoContextAnnIndexLoadReporter();
        var logger = Substitute.For<ILogger>();
        using var handle = NewHandle(source, store, reporter, logger);

        await handle.AdvanceAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(reporter.Snapshot().Discarded, Is.EqualTo(1),
                "A chunk absent on every read path is genuine damage, and is still rebuilt.");
            Assert.That(Warnings(logger).Single(), Does.Contain(
                $"Destroyed generation 1 holding {Vectors} vectors in {Options().PartitionCount} partitions"));
        });
    }

    private static List<string> Warnings(ILogger logger) =>
        [.. logger.ReceivedCalls()
            .Where(call => call.GetMethodInfo().Name == nameof(ILogger.Log)
                && Equals(call.GetArguments()[0], LogLevel.Warning))
            .Select(call => call.GetArguments()[2]!.ToString()!)];

    private async Task<(GapStore Store, InMemoryRepoContextVectorSource Source)> BuiltAsync()
    {
        var store = new GapStore();
        var source = new InMemoryRepoContextVectorSource(Space);
        for (var i = 0; i < Vectors; i++)
        {
            var angle = 2d * Math.PI * i / Vectors;
            var vector = new float[Space.Dimension];
            vector[0] = (float)Math.Cos(angle);
            vector[1] = (float)Math.Sin(angle);
            source.Set($"vec-{i:D6}", RepoContextKeys.File(RepoId, $"src/File{i}.cs"), vector);
        }

        using var reporter = new RepoContextAnnIndexLoadReporter();
        using (var seeding = NewHandle(source, store, reporter, Substitute.For<ILogger>()))
        {
            await seeding.EnsureBuiltAsync(Ct);
            await seeding.FlushAsync(Ct);
        }

        return (store, source);
    }

    private static List<string> VectorChunkKeys(InMemoryVectorIndexStore store)
    {
        var manifest = store.ReadAsync(VectorIndexStorageKeys.Manifest(Prefix)).GetAwaiter().GetResult();
        Assert.That(VectorIndexManifest.TryReadRecord(manifest, out var committed), Is.True,
            "The fixture needs a committed index to open.");
        var chunkPrefix = VectorIndexStorageKeys.GenerationPrefix(Prefix, committed.Generation) + "v/";
        var keys = store.Keys.Where(key => key.StartsWith(chunkPrefix, StringComparison.Ordinal)).ToList();
        Assert.That(keys, Is.Not.Empty);
        return keys;
    }

    private static RepoContextAnnIndexHandle NewHandle(
        InMemoryRepoContextVectorSource source,
        IVectorIndexStore store,
        RepoContextAnnIndexLoadReporter reporter,
        ILogger logger) => new(
            RepoId,
            Space,
            source,
            store,
            Options(),
            Prefix,
            logger,
            partitioning: null,
            load: reporter);

    /// <summary>
    /// A store whose batched reads can omit records it holds, while point reads
    /// and scans still return them: the store answering part of a request under
    /// pressure.
    /// </summary>
    private sealed class GapStore : IVectorIndexStore
    {
        public InMemoryVectorIndexStore Inner { get; } = new();

        public HashSet<string> HiddenFromBatchReads { get; } = new(StringComparer.Ordinal);

        public Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken = default)
            => Inner.ReadAsync(key, cancellationToken);

        public async Task<IReadOnlyDictionary<string, byte[]>> ReadManyAsync(
            IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        {
            var found = await Inner.ReadManyAsync(keys, cancellationToken).ConfigureAwait(false);
            return found
                .Where(entry => !HiddenFromBatchReads.Contains(entry.Key))
                .ToDictionary(entry => entry.Key, entry => entry.Value, StringComparer.Ordinal);
        }

        public Task WriteAsync(
            IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
            => Inner.WriteAsync(entries, cancellationToken);

        public Task DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
            => Inner.DeleteAsync(keys, cancellationToken);

        public IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
            string keyPrefix, CancellationToken cancellationToken = default)
            => Inner.ScanAsync(keyPrefix, cancellationToken);

        public Task DeletePrefixAsync(string keyPrefix, CancellationToken cancellationToken = default)
            => Inner.DeletePrefixAsync(keyPrefix, cancellationToken);
    }
}
