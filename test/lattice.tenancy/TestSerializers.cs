using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Builds <see cref="OrleansLatticeSerializer{T}"/> instances backed by a real
/// Orleans <see cref="Serializer{T}"/> (constructed from a minimal serialization
/// container, no cluster) so unit tests can round-trip tenancy types through the
/// exact binary serializer the registry uses in production.
/// </summary>
internal static class TestSerializers
{
    private static readonly ServiceProvider Provider =
        new ServiceCollection().AddSerializer().BuildServiceProvider();

    /// <summary>The Orleans-backed lattice serializer for <typeparamref name="T"/>.</summary>
    internal static OrleansLatticeSerializer<T> For<T>() =>
        new(Provider.GetRequiredService<Serializer<T>>());

    /// <summary>The Orleans-backed lattice serializer for <see cref="TenantRecord"/>.</summary>
    internal static OrleansLatticeSerializer<TenantRecord> TenantRecords => For<TenantRecord>();
}
