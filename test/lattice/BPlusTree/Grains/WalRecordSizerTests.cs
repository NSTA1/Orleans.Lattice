using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the <see cref="IWalRecordSizer"/> contract and its
/// default <see cref="OrleansBinaryWalRecordSizer"/> implementation.
/// The sizer is the per-batch byte-budget gate for
/// <c>WalShardGrain</c>; any divergence between the measured count and
/// the canonical <c>Serializer&lt;WalRecord&gt;</c> would let a batch
/// over the Azure Table Storage 4 MB ceiling slip through and fail the
/// entire transaction.
/// </summary>
[TestFixture]
public sealed class WalRecordSizerTests
{
    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private OrleansBinaryWalRecordSizer _sizer = null!;

    [OneTimeSetUp]
    public void Setup()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
        _sizer = new OrleansBinaryWalRecordSizer(_serializer);
    }

    [OneTimeTearDown]
    public void Teardown()
    {
        _services?.Dispose();
    }

    [Test]
    public void Constructor_null_serializer_throws()
    {
        Assert.That(
            () => new OrleansBinaryWalRecordSizer(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Measure_returns_exact_serialized_byte_count_for_small_set()
    {
        var entry = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        };

        AssertMeasureMatchesSerializer(entry);
    }

    [Test]
    public void Measure_returns_exact_serialized_byte_count_for_delete()
    {
        var entry = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Delete,
            Key = "deleted-key",
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            IsTombstone = true,
            OriginClusterId = "site-b",
        };

        AssertMeasureMatchesSerializer(entry);
    }

    [Test]
    public void Measure_returns_exact_serialized_byte_count_for_delete_range()
    {
        var entry = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.DeleteRange,
            Key = "range-start",
            EndExclusiveKey = "range-end",
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            IsTombstone = true,
            OriginClusterId = "site-c",
        };

        AssertMeasureMatchesSerializer(entry);
    }

    [Test]
    public void Measure_returns_exact_serialized_byte_count_for_large_value()
    {
        // 1 MB value - the encoded size will be slightly larger than
        // 1 MB once Orleans framing is included. Sizer must agree to
        // the byte.
        var entry = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[1024 * 1024],
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        };

        AssertMeasureMatchesSerializer(entry);
    }

    [Test]
    public void Measure_returns_exact_serialized_byte_count_for_unicode_key()
    {
        // The historical heuristic accounted for keys at UTF-16 worst
        // case (`key.Length * 2`), but Orleans-binary encodes strings as
        // UTF-8; a key dominated by 3- and 4-byte code points encodes
        // longer than the historical estimate. Sizer must return the
        // exact UTF-8-framed length.
        var entry = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = "\u4e2d\u6587\u30c6\u30b9\u30c8\ud83d\ude00",
            Value = new byte[] { 1 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        };

        AssertMeasureMatchesSerializer(entry);
    }

    [Test]
    public void Measure_returns_exact_serialized_byte_count_across_vector_clock_cardinalities()
    {
        // The historical heuristic ignored VectorClock entirely, so a
        // record with 10 origins encoded ~10x larger than the estimate.
        // Sizer must agree to the byte across the cardinality range.
        for (var cardinality = 0; cardinality <= 10; cardinality++)
        {
            var vc = new VersionVector();
            for (var i = 0; i < cardinality; i++)
            {
                vc.Tick($"site-{i}");
            }
            var entry = new WalRecord
            {
                TreeId = "tree",
                Op = MutationKind.Set,
                Key = "k",
                Value = new byte[] { 1, 2 },
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
                VectorClock = cardinality == 0 ? null : vc,
            };

            AssertMeasureMatchesSerializer(entry);
        }
    }

    [Test]
    public void Measure_is_deterministic_across_repeated_calls()
    {
        var entry = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        };

        var first = _sizer.Measure(entry);
        var second = _sizer.Measure(entry);
        var third = _sizer.Measure(entry);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(second));
            Assert.That(second, Is.EqualTo(third));
            Assert.That(first, Is.GreaterThan(0));
        });
    }

    [Test]
    public void Measure_is_thread_safe()
    {
        var entry = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3, 4, 5 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        };
        var expected = _sizer.Measure(entry);

        // Concurrent invocations on the same sizer instance must agree.
        // The internal counting writer rents its own per-call scratch
        // from ArrayPool so no shared state can leak between threads.
        var results = new int[64];
        Parallel.For(0, results.Length, i => results[i] = _sizer.Measure(entry));

        Assert.That(results, Has.All.EqualTo(expected));
    }

    private void AssertMeasureMatchesSerializer(WalRecord entry)
    {
        var encoded = _serializer.SerializeToArray(entry);
        var measured = _sizer.Measure(entry);

        Assert.That(
            measured,
            Is.EqualTo(encoded.Length),
            $"Sizer reported {measured} bytes but Serializer<WalRecord> produced {encoded.Length} bytes.");
    }
}
