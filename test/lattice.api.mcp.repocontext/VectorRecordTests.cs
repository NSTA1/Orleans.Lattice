using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for the vector record model - <see cref="VectorMetadataRecord"/> and
/// <see cref="VectorPayloadRecord"/>:
/// CRDT-backed merge convergence, preservation of the immutable embedding-space
/// tag across merges, content-addressed payload idempotence, and Orleans
/// serialization round-trips.
/// </summary>
[TestFixture]
public sealed class VectorRecordTests
{
    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static HybridLogicalClock Clock(long ticks, int counter = 0)
        => new() { WallClockTicks = ticks, Counter = counter };

    private static EmbeddingSpaceTag Space() => new("nomic", 768, VectorNormalization.UnitL2);

    [Test]
    public void Metadata_concurrent_scalar_and_attribute_edits_converge()
    {
        var baseline = new VectorMetadataRecord { RepoId = "acme", VectorId = "v1", Space = Space() };

        var aAttrs = new OrMap<string, BoundedRegister>();
        aAttrs.Set("ordinal", "A", RepoContextValues.Lww(0L, Clock(100)));
        var a = baseline with
        {
            ContentAddress = RepoContextValues.Lww("addr-old", Clock(100)),
            Attributes = aAttrs,
        };

        var bAttrs = new OrMap<string, BoundedRegister>();
        bAttrs.Set("role", "B", RepoContextValues.Lww("passage", Clock(150)));
        var b = baseline with
        {
            ContentAddress = RepoContextValues.Lww("addr-new", Clock(200)),
            Attributes = bAttrs,
        };

        var merged = VectorMetadataRecord.Merge(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(merged.RepoId, Is.EqualTo("acme"));
            Assert.That(merged.VectorId, Is.EqualTo("v1"));
            Assert.That(merged.Space, Is.EqualTo(Space()));
            Assert.That(RepoContextValues.ReadString(merged.ContentAddress), Is.EqualTo("addr-new"));
            Assert.That(merged.Attributes.ContainsKey("ordinal"), Is.True);
            Assert.That(merged.Attributes.ContainsKey("role"), Is.True);
        });
    }

    [Test]
    public void Metadata_merge_recovers_the_space_from_the_other_replica_when_one_is_default()
    {
        var known = new VectorMetadataRecord { RepoId = "acme", VectorId = "v1", Space = Space() };
        var unknown = new VectorMetadataRecord { RepoId = "acme", VectorId = "v1" };

        Assert.Multiple(() =>
        {
            Assert.That(VectorMetadataRecord.Merge(known, unknown).Space, Is.EqualTo(Space()));
            Assert.That(VectorMetadataRecord.Merge(unknown, known).Space, Is.EqualTo(Space()));
        });
    }

    [Test]
    public void Metadata_merge_is_commutative_and_idempotent_over_the_space_tag()
    {
        var a = new VectorMetadataRecord { RepoId = "acme", VectorId = "v1", Space = Space() };
        var b = new VectorMetadataRecord { RepoId = "acme", VectorId = "v1", Space = Space() };

        Assert.Multiple(() =>
        {
            Assert.That(VectorMetadataRecord.Merge(a, b).Space, Is.EqualTo(Space()));
            Assert.That(VectorMetadataRecord.Merge(b, a).Space, Is.EqualTo(Space()));
            Assert.That(VectorMetadataRecord.Merge(a, a).Space, Is.EqualTo(Space()));
        });
    }

    [Test]
    public void Payload_is_content_addressed_and_merge_is_idempotent()
    {
        var bytes = Encoding.UTF8.GetBytes("vector-bytes");
        var record = VectorPayloadRecord.Create("acme", "sha256:abc", Space(), bytes);

        var merged = VectorPayloadRecord.Merge(record, record);

        Assert.Multiple(() =>
        {
            Assert.That(merged.RepoId, Is.EqualTo("acme"));
            Assert.That(merged.ContentAddress, Is.EqualTo("sha256:abc"));
            Assert.That(merged.Space, Is.EqualTo(Space()));
            Assert.That(merged.Payload.Count, Is.EqualTo(1), "identical content addresses converge to one payload");
            Assert.That(merged.Payload.Contains(bytes), Is.True);
        });
    }

    [Test]
    public void Payload_create_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => VectorPayloadRecord.Create(null!, "a", Space(), Array.Empty<byte>()),
                Throws.ArgumentNullException);
            Assert.That(() => VectorPayloadRecord.Create("r", null!, Space(), Array.Empty<byte>()),
                Throws.ArgumentNullException);
            Assert.That(() => VectorPayloadRecord.Create("r", "a", Space(), null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Merge_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => VectorMetadataRecord.Merge(null!, new VectorMetadataRecord()),
                Throws.ArgumentNullException);
            Assert.That(() => VectorMetadataRecord.Merge(new VectorMetadataRecord(), null!),
                Throws.ArgumentNullException);
            Assert.That(() => VectorPayloadRecord.Merge(null!, new VectorPayloadRecord()),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Metadata_round_trips_through_the_orleans_serializer()
    {
        var attrs = new OrMap<string, BoundedRegister>();
        attrs.Set("role", "A", RepoContextValues.Lww("passage", Clock(100)));
        var record = new VectorMetadataRecord
        {
            RepoId = "acme",
            VectorId = "v1",
            Space = Space(),
            SourceKey = RepoContextValues.Lww("repo/acme/file/a.cs", Clock(100)),
            ContentAddress = RepoContextValues.Lww("sha256:abc", Clock(100)),
            CreatedAt = RepoContextValues.Lww(12345L, Clock(100)),
            Attributes = attrs,
        };

        var copy = _serializer.Deserialize<VectorMetadataRecord>(_serializer.SerializeToArray(record));

        Assert.Multiple(() =>
        {
            Assert.That(copy.RepoId, Is.EqualTo("acme"));
            Assert.That(copy.VectorId, Is.EqualTo("v1"));
            Assert.That(copy.Space, Is.EqualTo(Space()));
            Assert.That(RepoContextValues.ReadString(copy.SourceKey), Is.EqualTo("repo/acme/file/a.cs"));
            Assert.That(copy.Attributes.ContainsKey("role"), Is.True);
        });
    }

    [Test]
    public void Payload_round_trips_through_the_orleans_serializer()
    {
        var bytes = Encoding.UTF8.GetBytes("vector-bytes");
        var payload = VectorPayloadRecord.Create("acme", "sha256:abc", Space(), bytes);

        var payloadCopy = _serializer.Deserialize<VectorPayloadRecord>(_serializer.SerializeToArray(payload));

        Assert.Multiple(() =>
        {
            Assert.That(payloadCopy.RepoId, Is.EqualTo("acme"));
            Assert.That(payloadCopy.ContentAddress, Is.EqualTo("sha256:abc"));
            Assert.That(payloadCopy.Space, Is.EqualTo(Space()));
            Assert.That(payloadCopy.Payload.Contains(bytes), Is.True);
        });
    }
}
