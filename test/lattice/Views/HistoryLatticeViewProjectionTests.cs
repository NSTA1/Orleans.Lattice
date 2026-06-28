using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Views;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for the built-in <see cref="HistoryLatticeViewProjection"/>: the
/// re-keying and lowering of each source mutation into an append-only revision
/// row. Decodes the emitted <see cref="HistoryRow"/> through the same
/// <see cref="HistoryRowCodec"/> the projection used so the round-trip is end to
/// end.
/// </summary>
[TestFixture]
public sealed class HistoryLatticeViewProjectionTests
{
    private ServiceProvider _services = null!;
    private HistoryRowCodec _codec = null!;
    private HistoryLatticeViewProjection _projection = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _codec = new HistoryRowCodec(_services.GetRequiredService<Serializer<HistoryRow>>());
        _projection = new HistoryLatticeViewProjection(_codec);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static HybridLogicalClock Clock(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static LatticeMutation Set(string key, byte[]? value, HybridLogicalClock ts) => new()
    {
        TreeId = "src",
        Kind = MutationKind.Set,
        Key = key,
        Value = value,
        Timestamp = ts,
    };

    [Test]
    public void ProjectionVersion_is_history_v1() =>
        Assert.That(_projection.ProjectionVersion, Is.EqualTo(HistoryLatticeViewProjection.Version));

    [Test]
    public void Project_lww_set_emits_rekeyed_upsert_with_value_fingerprint()
    {
        var value = new byte[] { 1, 2, 3, 4 };
        var ts = Clock(0x1A2B, 7);

        var writes = _projection.Project(Set("orders/42", value, ts)).ToList();

        Assert.That(writes, Has.Count.EqualTo(1));
        var write = writes[0];
        Assert.That(write.Kind, Is.EqualTo(ViewWriteKind.Upsert));
        Assert.That(write.SourceKey, Is.Null, "history keys carry the HLC and must skip the collision detector");
        Assert.That(write.Key, Does.StartWith("orders/42/"));
        Assert.That(write.ExpiresAtTicks, Is.Zero, "the maintainer stamps expiry at drain time, not the projection");

        var row = _codec.Decode(write.Value!);
        Assert.That(row.Kind, Is.EqualTo(HistoryRowKind.Set));
        Assert.That(row.SourceKey, Is.EqualTo("orders/42"));
        Assert.That(row.Value, Is.EqualTo(value));
        Assert.That(row.ValueLength, Is.EqualTo(4));
        Assert.That(row.ValueHash, Is.Not.Zero);
        Assert.That(row.Timestamp, Is.EqualTo(ts));
    }

    [Test]
    public void Project_lww_set_with_null_value_has_zero_fingerprint()
    {
        var writes = _projection.Project(Set("k", null, Clock(1))).ToList();

        var row = _codec.Decode(writes[0].Value!);
        Assert.That(row.Kind, Is.EqualTo(HistoryRowKind.Set));
        Assert.That(row.Value, Is.Null);
        Assert.That(row.ValueHash, Is.Zero);
        Assert.That(row.ValueLength, Is.Zero);
    }

    [Test]
    public void Project_crdt_set_stores_delta_only()
    {
        var delta = new byte[] { 9, 9, 9 };
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.Set,
            Key = "counter",
            Value = new byte[] { 1 },
            Delta = delta,
            Mode = LatticeMergeMode.PnCounter,
            Timestamp = Clock(5),
        };

        var writes = _projection.Project(mutation).ToList();

        Assert.That(writes, Has.Count.EqualTo(1));
        var row = _codec.Decode(writes[0].Value!);
        Assert.That(row.Kind, Is.EqualTo(HistoryRowKind.CrdtDelta));
        Assert.That(row.Delta, Is.EqualTo(delta));
        Assert.That(row.Value, Is.Null, "CRDT rows store the author delta only, not the merged value");
        Assert.That(row.Mode, Is.EqualTo(LatticeMergeMode.PnCounter));
    }

    [Test]
    public void Project_delete_emits_delete_revision_not_view_delete()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.Delete,
            Key = "k",
            Timestamp = Clock(3),
        };

        var writes = _projection.Project(mutation).ToList();

        Assert.That(writes, Has.Count.EqualTo(1));
        Assert.That(writes[0].Kind, Is.EqualTo(ViewWriteKind.Upsert), "a delete is an appended revision, not a view-key removal");
        var row = _codec.Decode(writes[0].Value!);
        Assert.That(row.Kind, Is.EqualTo(HistoryRowKind.Delete));
        Assert.That(row.Value, Is.Null);
    }

    [Test]
    public void Project_tombstone_emits_delete_revision()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.Tombstone,
            Key = "k",
            Timestamp = Clock(3),
        };

        var writes = _projection.Project(mutation).ToList();

        var row = _codec.Decode(writes[0].Value!);
        Assert.That(row.Kind, Is.EqualTo(HistoryRowKind.Delete));
    }

    [Test]
    public void Project_range_delete_with_matched_keys_emits_one_delete_per_key()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            MatchedKeys = new[] { "a", "b", "c" },
            Timestamp = Clock(8),
        };

        var writes = _projection.Project(mutation).ToList();

        Assert.That(writes, Has.Count.EqualTo(3));
        Assert.That(writes.All(w => w.Kind == ViewWriteKind.Upsert), Is.True);
        Assert.That(writes[0].Key, Does.StartWith("a/"));
        Assert.That(writes[1].Key, Does.StartWith("b/"));
        Assert.That(writes[2].Key, Does.StartWith("c/"));
        Assert.That(_codec.Decode(writes[1].Value!).Kind, Is.EqualTo(HistoryRowKind.Delete));
        Assert.That(_codec.Decode(writes[1].Value!).SourceKey, Is.EqualTo("b"));
    }

    [Test]
    public void Project_unconstrained_range_delete_emits_range_reconcile()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "m",
            Timestamp = Clock(8),
        };

        var writes = _projection.Project(mutation).ToList();

        Assert.That(writes, Has.Count.EqualTo(1));
        Assert.That(writes[0].Kind, Is.EqualTo(ViewWriteKind.RangeReconcile));
        Assert.That(writes[0].Key, Is.EqualTo("a"));
        Assert.That(writes[0].EndKey, Is.EqualTo("m"));
    }

    [Test]
    public void Project_unbounded_range_delete_without_end_emits_nothing()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = null,
            Timestamp = Clock(8),
        };

        Assert.That(_projection.Project(mutation), Is.Empty);
    }

    [Test]
    public void Project_revisions_of_same_key_sort_chronologically_by_view_key()
    {
        var early = _projection.Project(Set("k", new byte[] { 1 }, Clock(10, 0))).Single().Key;
        var laterCounter = _projection.Project(Set("k", new byte[] { 2 }, Clock(10, 5))).Single().Key;
        var laterWall = _projection.Project(Set("k", new byte[] { 3 }, Clock(20, 0))).Single().Key;

        Assert.That(string.CompareOrdinal(early, laterCounter), Is.LessThan(0));
        Assert.That(string.CompareOrdinal(laterCounter, laterWall), Is.LessThan(0));
    }
}
