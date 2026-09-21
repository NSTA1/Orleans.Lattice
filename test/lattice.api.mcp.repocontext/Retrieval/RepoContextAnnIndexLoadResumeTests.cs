using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for the half of issue #2953 that lives in the handle: a load of the
/// durable approximate index that faults partway must keep the progress it made,
/// so the next attempt continues rather than reissuing the whole key-map walk.
/// <para>
/// <b>Why these assertions and not the obvious ones.</b> A resumed load and a
/// restarted one converge on the identical index - same mapping, same phase, same
/// vector count - so every assertion anyone would naturally reach for passes
/// under both. The two observables that actually separate them are the number of
/// records the store was asked for across the attempts, and the outcome arm the
/// reporter recorded. Both are asserted below, and a test that checked only the
/// finished index would be green against the defect.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed partial class RepoContextAnnIndexLoadResumeTests
{
    private const string RepoId = "acme";
    private const int Vectors = 24;
    private const int FaultAfter = 9;

    private static readonly EmbeddingSpaceTag Space = new("test-model", 8, VectorNormalization.UnitL2);

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
    public void Every_outcome_arm_exists_before_any_load_is_attempted()
    {
        // The listener is started BEFORE the reporter is constructed, because the
        // priming adds happen in the constructor. Matching by name rather than by
        // reference is forced by that ordering: there is no instrument to match
        // against until the type whose priming is under test has already run.
        var seen = new List<(string Outcome, long Value)>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (published, l) =>
            {
                if (string.Equals(
                        published.Meter.Name, RepoContextUsageRecorder.MeterName, StringComparison.Ordinal)
                    && string.Equals(
                        published.Name,
                        RepoContextAnnIndexLoadReporter.LoadInstrumentName,
                        StringComparison.Ordinal))
                {
                    l.EnableMeasurementEvents(published);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (string.Equals(
                        tag.Key, RepoContextAnnIndexLoadReporter.OutcomeTagKey, StringComparison.Ordinal))
                {
                    seen.Add(((string)tag.Value!, measurement));
                }
            }
        });
        listener.Start();

        using var reporter = new RepoContextAnnIndexLoadReporter();

        Assert.Multiple(() =>
        {
            // Asserted against the emitted series, not against the reporter's own
            // counters. Those counters are plain fields that read zero whether or
            // not anything was ever published, so asserting on them would pass with
            // the priming deleted - a test that cannot fail is the defect, not the
            // detector.
            Assert.That(
                seen.Select(s => s.Outcome),
                Is.EquivalentTo(new[]
                {
                    RepoContextAnnIndexLoadReporter.OutcomeFreshTag,
                    RepoContextAnnIndexLoadReporter.OutcomeResumedTag,
                    RepoContextAnnIndexLoadReporter.OutcomeFaultedTag,
                    RepoContextAnnIndexLoadReporter.OutcomeDeferredTag,
                    RepoContextAnnIndexLoadReporter.OutcomeRefusedTag,
                }),
                "Every arm has to be PRESENT on a host that has simply never faulted. An arm that appears "
                + "only once it is non-zero cannot distinguish a healthy plane from a build that never "
                + "shipped, which is the defect #2952 was filed over.");
            Assert.That(
                seen.Select(s => s.Value), Is.All.Zero,
                "Primed with a zero-valued add. A non-zero priming value would fabricate activity.");
        });
    }

    [Test]
    public async Task A_faulted_load_is_resumed_rather_than_reissued()
    {
        var store = new FaultOnceScanStore();
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var source = new InMemoryRepoContextVectorSource(Space);
        SeedRing(source, Vectors);

        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);

        // Build and flush once so the durable key map exists to be walked. This
        // open is the Fresh arm, and it is what makes the later Resumed reading a
        // genuine second attempt rather than an artefact of an empty store.
        using (var seeding = NewHandle(source, store, prefix, reporter))
        {
            await seeding.EnsureBuiltAsync(Ct);
            await seeding.FlushAsync(Ct);
        }

        var mappings = store.CountUnder(VectorIndexStorageKeys.KeyMapPrefix(prefix));
        Assert.That(mappings, Is.GreaterThan(FaultAfter),
            "The fault has to land partway through the walk. If the map were no longer than the fault "
            + "point the scan would complete before faulting and this fixture would prove nothing.");

        using var handle = NewHandle(source, store, prefix, reporter);
        store.ArmFault(VectorIndexStorageKeys.KeyMapPrefix(prefix), FaultAfter);
        store.ResetServed();

        Assert.That(
            async () => await handle.EnsureBuiltAsync(Ct),
            Throws.InstanceOf<IOException>(),
            "The armed store must actually fault. A scenario that silently completed would make every "
            + "assertion below a statement about an ordinary load.");

        var afterFault = reporter.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(afterFault.Faulted, Is.EqualTo(1));
            Assert.That(afterFault.Resumed, Is.Zero,
                "Nothing has resumed yet. Asserted here so the reading after the second attempt is known "
                + "to have been produced by that attempt.");
        });

        // Second attempt over the same handle: the partially built index was
        // retained, so this continues the walk.
        await handle.EnsureBuiltAsync(Ct);

        var afterResume = reporter.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(afterResume.Resumed, Is.EqualTo(1),
                "The second attempt continued banked progress, so it is a resumption and must be counted "
                + "as one. Recording it as Fresh would leave the resumed arm at zero forever, which is "
                + "indistinguishable from the defect.");
            Assert.That(afterResume.Faulted, Is.EqualTo(1));
            Assert.That(store.ServedUnderFaultPrefix, Is.EqualTo(mappings),
                "Across both attempts the key map must be read exactly once. A load that restarts from "
                + "the beginning instead reads the records before the fault point a second time, which "
                + "is the amplification #2953 measures.");
        });
    }

    [Test]
    public async Task A_load_that_never_faulted_is_counted_as_fresh()
    {
        var store = new FaultOnceScanStore();
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var source = new InMemoryRepoContextVectorSource(Space);
        SeedRing(source, Vectors);

        using var handle = NewHandle(
            source, store, RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space), reporter);
        await handle.EnsureBuiltAsync(Ct);

        var snapshot = reporter.Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Fresh, Is.EqualTo(1));
            Assert.That(snapshot.Resumed, Is.Zero,
                "A healthy load must not inflate the resumed arm. If it did, the arm would read non-zero "
                + "on every host and could no longer report that a resumption actually happened.");
            Assert.That(snapshot.Faulted, Is.Zero);
        });
    }

    private static RepoContextAnnIndexHandle NewHandle(
        InMemoryRepoContextVectorSource source,
        IVectorIndexStore store,
        string prefix,
        RepoContextAnnIndexLoadReporter reporter) => new(
            RepoId,
            Space,
            source,
            store,
            Options(),
            prefix,
            NullLogger.Instance,
            partitioning: null,
            load: reporter);

    private static void SeedRing(InMemoryRepoContextVectorSource source, int count)
    {
        for (var i = 0; i < count; i++)
        {
            var angle = 2d * Math.PI * i / count;
            var vector = new float[Space.Dimension];
            vector[0] = (float)Math.Cos(angle);
            vector[1] = (float)Math.Sin(angle);
            source.Set($"vec-{i:D6}", RepoContextKeys.File(RepoId, $"src/File{i}.cs"), vector);
        }
    }

    /// <summary>
    /// An in-memory store that can be armed to fault once, partway through the
    /// scan of a nominated prefix, and that counts how many records it served
    /// under that prefix.
    /// <para>
    /// It honours <c>exclusiveStartKey</c> itself rather than inheriting the
    /// interface default, because the count it reports is the whole measurement:
    /// a double served here would look exactly like the defect it is meant to
    /// detect.
    /// </para>
    /// </summary>
    private sealed class FaultOnceScanStore : IVectorIndexStore
    {
        private readonly InMemoryVectorIndexStore _inner = new();
        private string? _faultPrefix;
        private int _faultAfter;
        private bool _armed;

        public int ServedUnderFaultPrefix { get; private set; }

        public void ArmFault(string prefix, int serveBeforeFaulting)
        {
            _faultPrefix = prefix;
            _faultAfter = serveBeforeFaulting;
            _armed = true;
        }

        public void ResetServed() => ServedUnderFaultPrefix = 0;

        public int CountUnder(string prefix)
        {
            var count = 0;
            var enumerator = _inner.ScanAsync(prefix, CancellationToken.None).GetAsyncEnumerator();
            try
            {
                while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                {
                    count++;
                }
            }
            finally
            {
                enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }

            return count;
        }

        public Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken = default)
            => _inner.ReadAsync(key, cancellationToken);

        public Task<IReadOnlyDictionary<string, byte[]>> ReadManyAsync(
            IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
            => _inner.ReadManyAsync(keys, cancellationToken);

        public Task WriteAsync(
            IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
            => _inner.WriteAsync(entries, cancellationToken);

        public Task DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
            => _inner.DeleteAsync(keys, cancellationToken);

        public Task DeletePrefixAsync(string keyPrefix, CancellationToken cancellationToken = default)
            => _inner.DeletePrefixAsync(keyPrefix, cancellationToken);

        public IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
            string keyPrefix, CancellationToken cancellationToken = default)
            => ScanAsync(keyPrefix, null, cancellationToken);

        public async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
            string keyPrefix,
            string? exclusiveStartKey,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var watched = _faultPrefix is not null
                && string.Equals(keyPrefix, _faultPrefix, StringComparison.Ordinal);
            var servedThisCall = 0;

            await foreach (var entry in _inner.ScanAsync(keyPrefix, cancellationToken).ConfigureAwait(false))
            {
                if (exclusiveStartKey is not null
                    && string.CompareOrdinal(entry.Key, exclusiveStartKey) <= 0)
                {
                    continue;
                }

                if (watched)
                {
                    if (_armed && servedThisCall >= _faultAfter)
                    {
                        _armed = false;
                        throw new IOException("Injected scan fault partway through the key map.");
                    }

                    servedThisCall++;
                    ServedUnderFaultPrefix++;
                }

                yield return entry;
            }
        }
    }
}
