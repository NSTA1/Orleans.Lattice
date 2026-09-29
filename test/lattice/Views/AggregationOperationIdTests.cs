using System.IO.Hashing;
using System.Text;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Pins the aggregation saga operation id byte-for-byte against the string the
/// applier used to interpolate before composing the hash input directly into its
/// UTF-8 buffer.
/// <para>
/// This is load-bearing rather than cosmetic. The id is the saga's dedup key, so
/// a silent change to it would not corrupt a fold - the membership pointer still
/// carries the truth - but it would stop a replayed batch being recognised as a
/// replay, which is exactly the property the atomic flip was introduced for. The
/// reference implementation below is therefore a verbatim copy of the replaced
/// body, not a paraphrase of it.
/// </para>
/// </summary>
[TestFixture]
public sealed class AggregationOperationIdTests
{
    private sealed class OperationIdRecordingStore : IAggregationViewStore
    {
        private readonly Dictionary<string, byte[]> _map = new(StringComparer.Ordinal);

        public List<string> OperationIds { get; } = [];

        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
            => Task.FromResult(_map.TryGetValue(key, out var v) ? v : null);

        public Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default)
        {
            var result = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            foreach (var key in keys)
            {
                if (_map.TryGetValue(key, out var v))
                {
                    result[key] = v;
                }
            }

            return Task.FromResult(result);
        }

        public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
        {
            _map[key] = value;
            return Task.CompletedTask;
        }

        public Task DeleteAsync(string key, CancellationToken cancellationToken = default)
        {
            _map.Remove(key);
            return Task.CompletedTask;
        }

        public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default)
        {
            OperationIds.Add(operationId);
            foreach (var e in entries)
            {
                _map[e.Key] = e.Value;
            }

            return Task.CompletedTask;
        }
    }

    /// <summary>Verbatim copy of the pre-optimisation body, kept as the oracle.</summary>
    private static string ReferenceOperationId(string epoch, string sourceKey, HybridLogicalClock timestamp)
    {
        var payload = $"{epoch}\u0000{sourceKey}\u0000{timestamp.WallClockTicks}\u0000{timestamp.Counter}";
        var hash = XxHash64.HashToUInt64(Encoding.UTF8.GetBytes(payload));
        return "agg-" + hash.ToString("x16");
    }

    private static IEnumerable<TestCaseData> SourceKeyCases()
    {
        yield return new TestCaseData("src", "epoch-1").SetName("ascii");
        yield return new TestCaseData(string.Empty, "epoch-1").SetName("empty_source_key");
        yield return new TestCaseData("\u00e9\u4e2d\ud83d\ude00", "epoch-1").SetName("unicode_source_key");
        yield return new TestCaseData("src", "\u4e2d-epoch").SetName("unicode_epoch");
        // Past the 256-byte stack threshold, so the pooled arm formats the id.
        yield return new TestCaseData(new string('k', 400), "epoch-1").SetName("pooled_long_source_key");
    }

    [TestCaseSource(nameof(SourceKeyCases))]
    public async Task OperationId_is_byte_identical_to_the_interpolated_payload(string sourceKey, string epoch)
    {
        var store = new OperationIdRecordingStore();
        var applier = new AggregationApplier(store, AggregationKind.Sum, fanout: 1, maxGroupEntries: 0, operationEpoch: epoch);
        var timestamp = new HybridLogicalClock { WallClockTicks = 638_000_000_000_000_000, Counter = 7 };

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", sourceKey, 1.0, timestamp));

        Assert.That(store.OperationIds, Is.Not.Empty, "the numeric contribute path writes through the atomic batch");
        Assert.That(store.OperationIds[0], Is.EqualTo(ReferenceOperationId(epoch, sourceKey, timestamp)));
    }

    [Test]
    public async Task OperationId_is_byte_identical_for_a_zero_clock()
    {
        var store = new OperationIdRecordingStore();
        var applier = new AggregationApplier(store, AggregationKind.Count, fanout: 1, maxGroupEntries: 0, operationEpoch: "e");
        var timestamp = new HybridLogicalClock { WallClockTicks = 0, Counter = 0 };

        await applier.ApplyAsync(AggregationContribution.Membership("g", "s", timestamp));

        Assert.That(store.OperationIds[0], Is.EqualTo(ReferenceOperationId("e", "s", timestamp)));
    }

    [Test]
    public async Task OperationId_has_the_expected_shape()
    {
        var store = new OperationIdRecordingStore();
        var applier = new AggregationApplier(store, AggregationKind.Sum, fanout: 1, maxGroupEntries: 0, operationEpoch: "e");

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s", 1.0, HybridLogicalClock.Tick(new HybridLogicalClock())));

        var id = store.OperationIds[0];
        Assert.Multiple(() =>
        {
            Assert.That(id, Has.Length.EqualTo("agg-".Length + 16));
            Assert.That(id, Does.StartWith("agg-"));
            Assert.That(id[4..], Does.Match("^[0-9a-f]{16}$"), "the hash renders as lower-case zero-padded hex");
        });
    }

    [Test]
    public async Task OperationId_differs_for_a_different_source_key()
    {
        var store = new OperationIdRecordingStore();
        var applier = new AggregationApplier(store, AggregationKind.Sum, fanout: 1, maxGroupEntries: 0, operationEpoch: "e");
        var timestamp = new HybridLogicalClock { WallClockTicks = 5, Counter = 1 };

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s1", 1.0, timestamp));
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s2", 1.0, timestamp));

        Assert.That(store.OperationIds[0], Is.Not.EqualTo(store.OperationIds[1]));
    }
}
