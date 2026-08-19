using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Unit tests for <see cref="RepoContextMemoryCodec"/>, the single read/write plane
/// for the agent-memory tree. A memory value is stored as an <see cref="MvRegister"/>
/// envelope whose concurrent values are serialized <see cref="MemoryRecord"/> bytes;
/// <see cref="RepoContextMemoryCodec.Fold"/> unwraps that envelope and reduces the
/// conflict set through <see cref="MemoryRecord.Merge"/> so two clusters' concurrent
/// writes both survive and converge instead of one being lost last-writer-wins.
/// </summary>
[TestFixture]
public sealed class RepoContextMemoryCodecTests
{
    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    [Test]
    public void Fold_of_a_null_value_is_null()
    {
        Assert.That(RepoContextMemoryCodec.Fold(null, Serializer), Is.Null);
    }

    [Test]
    public void Fold_of_an_empty_register_is_null()
    {
        var empty = JsonLatticeSerializer<MvRegister>.Default.Serialize(new MvRegister());

        Assert.That(RepoContextMemoryCodec.Fold(empty, Serializer), Is.Null);
    }

    [Test]
    public void Fold_rejects_a_null_serializer()
    {
        Assert.That(() => RepoContextMemoryCodec.Fold(new byte[] { 1 }, null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Fold_of_a_single_value_returns_that_record()
    {
        var record = new MemoryRecord
        {
            RepoId = "acme",
            Topic = "notes",
            Id = "1",
            Title = RepoContextValues.Lww("only", Clock(1)),
        };
        var stored = MemoryRegisterTestEncoding.EncodeSingle(Serializer, "r", record);

        var folded = RepoContextMemoryCodec.Fold(stored, Serializer);

        Assert.That(folded, Is.Not.Null);
        Assert.That(RepoContextValues.ReadString(folded!.Title), Is.EqualTo("only"));
    }

    [Test]
    public void Fold_of_two_concurrent_writes_preserves_both_and_converges()
    {
        // Two clusters wrote the same key without observing each other. Cluster A set
        // the title; cluster B added a tag. Both dots are live in the envelope, so the
        // fold must merge them - no write is lost, and the fold is order-independent.
        var a = new MemoryRecord
        {
            RepoId = "acme",
            Topic = "notes",
            Id = "1",
            Title = RepoContextValues.Lww("from-a", Clock(1)),
        };
        var b = new MemoryRecord { RepoId = "acme", Topic = "notes", Id = "1" };
        b.Tags.Add(Encoding.UTF8.GetBytes("from-b"), "b", 0);

        var stored = MemoryRegisterTestEncoding.EncodeConcurrent(Serializer, ("clusterA", a), ("clusterB", b));

        var folded = RepoContextMemoryCodec.Fold(stored, Serializer);

        Assert.That(folded, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextValues.ReadString(folded!.Title), Is.EqualTo("from-a"),
                "Cluster A's write survives.");
            Assert.That(folded!.Tags.Elements().Select(e => Encoding.UTF8.GetString(e)), Is.EquivalentTo(new[] { "from-b" }),
                "Cluster B's write survives.");
        });
    }

    [Test]
    public void Fold_is_independent_of_the_concurrent_write_order()
    {
        var a = new MemoryRecord { RepoId = "acme", Topic = "notes", Id = "1" };
        a.Tags.Add(Encoding.UTF8.GetBytes("a"), "a", 0);
        var b = new MemoryRecord { RepoId = "acme", Topic = "notes", Id = "1" };
        b.Tags.Add(Encoding.UTF8.GetBytes("b"), "b", 0);

        var forward = RepoContextMemoryCodec.Fold(
            MemoryRegisterTestEncoding.EncodeConcurrent(Serializer, ("clusterA", a), ("clusterB", b)), Serializer);
        var backward = RepoContextMemoryCodec.Fold(
            MemoryRegisterTestEncoding.EncodeConcurrent(Serializer, ("clusterB", b), ("clusterA", a)), Serializer);

        var forwardTags = forward!.Tags.Elements().Select(e => Encoding.UTF8.GetString(e)).OrderBy(x => x);
        var backwardTags = backward!.Tags.Elements().Select(e => Encoding.UTF8.GetString(e)).OrderBy(x => x);
        Assert.That(forwardTags, Is.EqualTo(backwardTags));
    }

    [Test]
    public void ByteIdentity_round_trips_the_raw_bytes()
    {
        var payload = new byte[] { 1, 2, 3, 4 };

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextMemoryCodec.ByteIdentity.Serialize(payload), Is.SameAs(payload));
            Assert.That(RepoContextMemoryCodec.ByteIdentity.Deserialize(payload), Is.SameAs(payload));
        });
    }

    [Test]
    public void DecodeRegister_round_trips_a_written_envelope()
    {
        var record = new MemoryRecord { RepoId = "acme", Topic = "notes", Id = "1" };
        var stored = MemoryRegisterTestEncoding.EncodeSingle(Serializer, "r", record);

        var register = RepoContextMemoryCodec.DecodeRegister(stored);

        Assert.That(register.Values(), Has.Count.EqualTo(1));
    }
}
