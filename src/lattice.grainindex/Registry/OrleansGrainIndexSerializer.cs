using Orleans.Serialization;

namespace Orleans.Lattice.GrainIndex.Registry;

/// <summary>
/// An <see cref="ILatticeSerializer{T}"/> that delegates to the Orleans binary
/// serializer, so a registry record round-trips through the
/// <c>[GenerateSerializer]</c> / <c>[Id]</c> contract its types are decorated
/// with.
/// <para>
/// The default <c>JsonLatticeSerializer</c> would use <c>System.Text.Json</c>,
/// which ignores non-public members and cannot set the get-only properties the
/// registry types expose, so the record would not survive a round trip. Passing
/// this explicitly to every typed <see cref="ILattice"/> call keeps the durable
/// registry state in the Orleans wire format the alias table already governs.
/// </para>
/// </summary>
/// <typeparam name="T">The serialized value type.</typeparam>
internal sealed class OrleansGrainIndexSerializer<T> : ILatticeSerializer<T>
{
    private readonly Serializer<T> _serializer;

    /// <summary>Initialises a new instance.</summary>
    /// <param name="serializer">The Orleans serializer for <typeparamref name="T"/>. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="serializer"/> is <c>null</c>.</exception>
    public OrleansGrainIndexSerializer(Serializer<T> serializer)
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
