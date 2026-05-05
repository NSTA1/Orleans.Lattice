using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Pins the Orleans-serializer wire-shape contract for the atomic-batch
/// metadata slots on <see cref="LatticeMutation"/>: both fields must
/// round-trip through serialize/deserialize verbatim, and a legacy
/// payload that leaves them unset must decode to zero. The serializer's
/// id-based wire format guarantees this by convention, but the
/// regression scaffold catches an accidental id renumber or a property
/// being dropped from the surface during a refactor.
/// </summary>
[TestFixture]
public sealed class LatticeMutationAtomicBatchRoundTripTests
{
    private ServiceProvider _services = null!;
    private Serializer<LatticeMutation> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<LatticeMutation>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private LatticeMutation RoundTrip(LatticeMutation mutation)
    {
        var bytes = _serializer.SerializeToArray(mutation);
        return _serializer.Deserialize(bytes);
    }

    [Test]
    public void Atomic_batch_slots_round_trip_with_explicit_values()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3 },
            TransactionId = Guid.NewGuid(),
            AtomicBatchSize = 5,
            AtomicBatchIndex = 2,
        };

        var decoded = RoundTrip(mutation);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.AtomicBatchSize, Is.EqualTo(5));
            Assert.That(decoded.AtomicBatchIndex, Is.EqualTo(2));
        });
    }

    [Test]
    public void Atomic_batch_slots_round_trip_zero_for_legacy_decode()
    {
        // Wire-compat: a producer (or persisted observer payload) that
        // never sets the slots emits the default zero value; the
        // serializer must decode the same shape so a legacy payload
        // upgraded in place reads as a single-key, non-atomic write.
        var mutation = new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
        };

        var decoded = RoundTrip(mutation);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.AtomicBatchSize, Is.EqualTo(0));
            Assert.That(decoded.AtomicBatchIndex, Is.EqualTo(0));
        });
    }
}
