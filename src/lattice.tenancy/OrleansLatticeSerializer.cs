using Orleans.Serialization;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// An <see cref="ILatticeSerializer{T}"/> that delegates to the Orleans binary
/// serializer. Unlike the default <see cref="JsonLatticeSerializer{T}"/> - which
/// uses <c>System.Text.Json</c> and therefore ignores non-public members and
/// cannot set private-init properties - this round-trips the full
/// <see cref="TenantRecord"/> object graph (its internal LWW registers and slot
/// dictionaries, the private-init <see cref="TenantRecord.Id"/>, and the nested
/// <see cref="TenantId"/>) through the <c>[GenerateSerializer]</c>/<c>[Id]</c>
/// contract the tenancy types are decorated with. The registry passes an
/// instance of this explicitly to every typed <see cref="ILattice"/> call so the
/// durable <c>sys-tenant-registry</c> state is stored in - and recovered from -
/// the Orleans wire format rather than a lossy JSON projection.
/// </summary>
/// <typeparam name="T">The serialized value type.</typeparam>
internal sealed class OrleansLatticeSerializer<T> : ILatticeSerializer<T>
{
    private readonly Serializer<T> _serializer;

    /// <summary>Initializes a new <see cref="OrleansLatticeSerializer{T}"/>.</summary>
    /// <param name="serializer">The Orleans serializer for <typeparamref name="T"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="serializer"/> is <c>null</c>.</exception>
    public OrleansLatticeSerializer(Serializer<T> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        _serializer = serializer;
    }

    /// <inheritdoc />
    public byte[] Serialize(T value) => _serializer.SerializeToArray(value);

    /// <inheritdoc />
    public T Deserialize(byte[] bytes)
    {
        ArgumentNullException.ThrowIfNull(bytes);
        return _serializer.Deserialize(bytes);
    }
}
