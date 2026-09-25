using System.Buffers;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Serialization;
using Orleans.Serialization.Session;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="WalRecordRoutingReader"/>, which decides from an
/// encoded record's routing prefix whether a <see cref="WalKeyFilter"/> excludes
/// it (issue #3565).
/// <para>
/// The reader walks the Orleans wire format by hand, so the oracle for every
/// verdict here is the full decode of the same bytes through the record's real
/// serializer, over every record shape the WAL writes: both encodings (the
/// encoder strips the tree id, the provider-boundary seam keeps it), every
/// mutation kind, and payloads that exercise every field after the key. A
/// verdict that ever disagreed with the full decode would drop a record its
/// owner applies, so the reader is allowed to answer "unknown" but never to be
/// wrong.
/// </para>
/// </summary>
[TestFixture]
public sealed class WalRecordRoutingReaderTests
{
    private const string TreeId = "tree-routing";

    private static readonly ShardMap Map = ShardMap.CreateDefault(64, 4);

    private static readonly WalKeyFilter[] Filters =
    [
        new WalKeyFilter("m", "n"),
        new WalKeyFilter(null, null, Map, 2),
        new WalKeyFilter("k010", "k040", Map, 1),
        new WalKeyFilter("\u00FC", null),
        default,
    ];

    private static readonly string[] Keys =
    [
        "a", "m", "m-owned", "n", "z", "\u00FCber", "\u65E5\u672C", "m\uD83D\uDE00", "\uD800lone",
        new string('m', 300), .. Enumerable.Range(0, 24).Select(i => $"k{i * 3:D3}"),
    ];

    private static readonly MutationKind[] Kinds =
    [
        MutationKind.Set, MutationKind.Delete, MutationKind.Tombstone, MutationKind.DeleteRange,
        MutationKind.TxCommit, MutationKind.TxAbort, (MutationKind)42,
    ];

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private OrleansBinaryWalRecordEncoder _encoder = null!;
    private WalRecordRoutingReader _reader = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
        _encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        _reader = new WalRecordRoutingReader(_services.GetRequiredService<SerializerSessionPool>());
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    /// <summary>
    /// Record shapes covering every field that follows the key on the wire, so
    /// the reader must stop at the key rather than depend on what comes after.
    /// </summary>
    private static IEnumerable<WalRecord> Shapes(MutationKind kind, string key)
    {
        var clock = new VersionVector();
        clock.Tick("replica-a");
        yield return new WalRecord { TreeId = TreeId, Op = kind, Key = key };
        yield return new WalRecord
        {
            TreeId = TreeId,
            Op = kind,
            Key = key,
            EndExclusiveKey = key + "~",
            Value = Enumerable.Range(0, 4096).Select(i => (byte)i).ToArray(),
            Timestamp = new HybridLogicalClock { WallClockTicks = 1234, Counter = 5 },
            IsTombstone = kind == MutationKind.Delete,
            ExpiresAtTicks = 99,
            OriginClusterId = "site-a",
            Mode = LatticeMergeMode.PnCounter,
            VectorClock = clock,
            DependencySummary = clock,
            Delta = [1, 2, 3],
            AtomicBatchSize = 3,
            AtomicBatchIndex = 1,
            TransactionId = Guid.NewGuid(),
            IsPrepared = true,
            ShardIndex = 7,
            AtomicShardCount = 2,
            IsMerge = true,
            Category = MutationCategory.Maintenance,
            MatchedKeys = [key, "other"],
            CrossTreeOperationId = "op-1",
            CrossTreeParticipants = ["t1", "t2"],
        };
    }

    /// <summary>Both encodings the WAL writes: the encoder's, which strips the tree id, and the raw serializer's.</summary>
    private IEnumerable<byte[]> Encodings(WalRecord record)
    {
        var encoded = new ArrayBufferWriter<byte>();
        _encoder.Encode(in record, encoded);
        yield return encoded.WrittenSpan.ToArray();

        var raw = new ArrayBufferWriter<byte>();
        _serializer.Serialize(record, raw);
        yield return raw.WrittenSpan.ToArray();
    }

    [Test]
    public void TryClassify_agrees_with_the_full_decode_for_every_shape_and_filter()
    {
        var verdicts = 0;
        Assert.Multiple(() =>
        {
            foreach (var kind in Kinds)
            {
                foreach (var key in Keys)
                {
                    foreach (var record in Shapes(kind, key))
                    {
                        foreach (var bytes in Encodings(record))
                        {
                            var full = _serializer.Deserialize(bytes);
                            foreach (var filter in Filters)
                            {
                                var read = _reader.TryClassify(bytes, in filter, out var excluded);

                                // A non-empty key is its own string instance, so it is
                                // never written as a back-reference and must be read.
                                Assert.That(read, Is.True, $"{kind} '{key}' could not be classified.");
                                Assert.That(
                                    excluded,
                                    Is.EqualTo(filter.Excludes(full.Op, full.Key)),
                                    $"{kind} '{key}' under {filter}: the prefix verdict disagrees with the full decode.");
                                verdicts++;
                            }
                        }
                    }
                }
            }
        });

        Assert.That(verdicts, Is.GreaterThan(1000), "The matrix must actually have run.");
    }

    [Test]
    public void TryClassify_reports_unknown_for_a_key_written_as_a_back_reference()
    {
        // The encoder strips the tree id to the empty string, so an empty key is
        // the same string instance and is written as a reference to it. The
        // reader cannot resolve references it never recorded, so it must answer
        // "unknown" and leave the verdict to the full decode.
        var record = new WalRecord { TreeId = TreeId, Op = MutationKind.Set, Key = string.Empty };
        var encoded = Encodings(record).First();
        var filter = new WalKeyFilter("m", "n");

        var read = _reader.TryClassify(encoded, in filter, out var excluded);

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.False);
            Assert.That(excluded, Is.False, "An unread record is never excluded.");
        });
    }

    [Test]
    public void TryClassify_reports_unknown_when_the_key_runs_past_the_supplied_prefix()
    {
        var record = new WalRecord { TreeId = TreeId, Op = MutationKind.Set, Key = new string('z', 2000), Value = [1] };
        var bytes = Encodings(record).First();
        var filter = new WalKeyFilter("m", "n");

        Assert.Multiple(() =>
        {
            Assert.That(_reader.TryClassify(bytes.AsSpan(0, 1024), in filter, out var excluded), Is.False);
            Assert.That(excluded, Is.False);
            Assert.That(_reader.TryClassify(bytes, in filter, out excluded), Is.True, "The whole record is readable.");
            Assert.That(excluded, Is.True);
        });
    }

    [Test]
    public void TryClassify_reports_unknown_for_input_that_is_not_a_record()
    {
        var filter = new WalKeyFilter("m", "n");
        var garbage = Enumerable.Range(0, 64).Select(i => (byte)(i * 37)).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(_reader.TryClassify(ReadOnlySpan<byte>.Empty, in filter, out var excluded), Is.False);
            Assert.That(excluded, Is.False);
            Assert.That(_reader.TryClassify(garbage, in filter, out excluded), Is.False);
            Assert.That(excluded, Is.False);
        });
    }

    [Test]
    public void TryReadRoutingOnly_returns_the_kind_and_key_of_the_full_decode_and_nothing_else()
    {
        Assert.Multiple(() =>
        {
            foreach (var kind in new[] { MutationKind.Set, MutationKind.Delete, MutationKind.Tombstone })
            {
                foreach (var key in Keys)
                {
                    foreach (var record in Shapes(kind, key))
                    {
                        foreach (var bytes in Encodings(record))
                        {
                            var full = _serializer.Deserialize(bytes);

                            Assert.That(_reader.TryReadRoutingOnly(bytes, out var routing), Is.True, $"{kind} '{key}'.");
                            Assert.That(routing.Op, Is.EqualTo(full.Op));
                            Assert.That(routing.Key, Is.EqualTo(full.Key));
                            Assert.That(routing.Value, Is.Null);
                            Assert.That(routing.TreeId, Is.Empty);
                        }
                    }
                }
            }
        });
    }

    [Test]
    public void TryReadRoutingOnly_reports_unknown_for_a_record_it_cannot_read_a_key_from()
    {
        var saga = Encodings(new WalRecord { TreeId = TreeId, Op = MutationKind.TxCommit, Key = "3" }).First();

        Assert.Multiple(() =>
        {
            Assert.That(_reader.TryReadRoutingOnly(saga, out _), Is.False,
                "A kind that is never excluded stops before its key, so it has no routing-only form.");
            Assert.That(_reader.TryReadRoutingOnly(ReadOnlySpan<byte>.Empty, out _), Is.False);
        });
    }

    [Test]
    public void TryClassify_allocates_nothing_per_record_on_either_verdict()
    {
        // The whole point of the prefix read: deciding a record costs no heap
        // allocation, whichever way it goes. Measured differentially through
        // the shared probe, so a one-off cost cancels and only per-record
        // growth is reported.
        var excludedBytes = Encodings(new WalRecord { TreeId = TreeId, Op = MutationKind.Set, Key = "z-foreign", Value = new byte[4096] }).First();
        var keptBytes = Encodings(new WalRecord { TreeId = TreeId, Op = MutationKind.Set, Key = "m-owned", Value = new byte[4096] }).First();
        var filter = new WalKeyFilter("m", "n", Map, 2);
        var reader = _reader;

        long Run(byte[] bytes)
        {
            return AllocationProbe.Growth(
                prepare: _ => bytes,
                measure: (payload, count) =>
                {
                    var excluded = 0L;
                    for (var i = 0; i < count; i++)
                    {
                        if (reader.TryClassify(payload, in filter, out var verdict) && verdict)
                        {
                            excluded++;
                        }
                    }

                    AllocationProbe.ScalarSink = excluded;
                },
                smallSize: 1_000,
                largeSize: 5_000);
        }

        Assert.Multiple(() =>
        {
            Assert.That(Run(excludedBytes), Is.Zero, "Classifying an excluded record must not allocate.");
            Assert.That(Run(keptBytes), Is.Zero, "Classifying a kept record must not allocate either; only its later decode does.");
        });
    }
}
