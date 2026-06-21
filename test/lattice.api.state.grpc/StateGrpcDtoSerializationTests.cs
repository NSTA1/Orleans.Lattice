using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Round-trips the gRPC-layer wire DTOs (the <c>Model</c> request / response
/// records that the binding marshals with the Orleans serializer) to prove the
/// transport contract is coherent and stable across the wire. The
/// transport-agnostic facade DTOs are covered separately in the
/// <c>Orleans.Lattice.Api.State</c> test project; this fixture covers the
/// gRPC-only response envelopes the service materialises.
/// </summary>
[TestFixture]
public sealed class StateGrpcDtoSerializationTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    [Test]
    public void EntryGetRequest_round_trips()
    {
        var original = new EntryGetRequest { TreeId = "tree-a", Key = "k1" };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void EntryGetResponse_round_trips_with_record()
    {
        var original = new EntryGetResponse
        {
            Status = StateQueryStatus.Found,
            TreeId = "tree-a",
            Key = "k1",
            Entry = new EntryRecord
            {
                Key = "k1",
                ValuePreview = new byte[] { 9, 8, 7 },
                ValueLength = 32,
                Truncated = true,
                Hlc = HybridLogicalClock.Zero,
                IsTombstone = false,
                ExpiresAtTicks = 555,
                CrdtShape = "Lww",
            },
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(copy.TreeId, Is.EqualTo("tree-a"));
            Assert.That(copy.Key, Is.EqualTo("k1"));
            Assert.That(copy.Entry, Is.Not.Null);
            Assert.That(copy.Entry!.ValuePreview, Is.EqualTo(new byte[] { 9, 8, 7 }));
            Assert.That(copy.Entry.ValueLength, Is.EqualTo(32));
        });
    }

    [Test]
    public void EntryGetResponse_round_trips_when_key_not_found()
    {
        var original = new EntryGetResponse
        {
            Status = StateQueryStatus.KeyNotFound,
            TreeId = "tree-a",
            Key = "missing",
            Entry = null,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Status, Is.EqualTo(StateQueryStatus.KeyNotFound));
            Assert.That(copy.Entry, Is.Null);
        });
    }

    [Test]
    public void EntryScanResponse_round_trips_with_continuation_token()
    {
        var original = new EntryScanResponse
        {
            Status = StateQueryStatus.Found,
            TreeId = "tree-a",
            Entries = new[]
            {
                new EntryRecord { Key = "k0", ValuePreview = new byte[] { 1 }, ValueLength = 1, Hlc = HybridLogicalClock.Zero },
                new EntryRecord { Key = "k1", ValuePreview = new byte[] { 2 }, ValueLength = 1, Hlc = HybridLogicalClock.Zero },
            },
            ContinuationToken = "cursor-xyz",
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(copy.Entries.Select(e => e.Key), Is.EqualTo(new[] { "k0", "k1" }));
            Assert.That(copy.ContinuationToken, Is.EqualTo("cursor-xyz"));
        });
    }

    [Test]
    public void EntryScanResponse_round_trips_when_drained()
    {
        var original = new EntryScanResponse
        {
            Status = StateQueryStatus.Found,
            TreeId = "tree-a",
            Entries = Array.Empty<EntryRecord>(),
            ContinuationToken = null,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Entries, Is.Empty);
            Assert.That(copy.ContinuationToken, Is.Null);
        });
    }

    [Test]
    public void StructureResponse_round_trips_with_nodes()
    {
        var original = new StructureResponse
        {
            Status = StateQueryStatus.Found,
            TreeId = "tree-a",
            Roots = new[]
            {
                new NodeStateSummary
                {
                    Kind = NodeKind.ShardRoot,
                    NodeId = "root-0",
                    ShardIndex = 0,
                    Depth = 0,
                    ChildCount = 1,
                    Children = new[]
                    {
                        new NodeStateSummary { Kind = NodeKind.Leaf, NodeId = "leaf-0", ShardIndex = 0, Depth = 1 },
                    },
                },
            },
            Truncated = true,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(copy.Roots, Has.Count.EqualTo(1));
            Assert.That(copy.Roots[0].Children[0].NodeId, Is.EqualTo("leaf-0"));
            Assert.That(copy.Truncated, Is.True);
        });
    }
}
