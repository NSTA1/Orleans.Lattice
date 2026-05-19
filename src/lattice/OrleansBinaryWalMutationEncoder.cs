using System.Buffers;
using Orleans.Serialization;

namespace Orleans.Lattice;

/// <summary>
/// Default <see cref="IWalMutationEncoder"/> implementation that
/// writes <see cref="LatticeMutation"/> bytes through the canonical
/// <see cref="Serializer{T}"/> from <c>Orleans.Serialization</c>. Has
/// no per-call state beyond the bytes the caller supplies through the
/// destination <see cref="IBufferWriter{T}"/>; the underlying
/// serializer is itself thread-safe and stateless.
/// <para>
/// Registered as a singleton from
/// <see cref="LatticeServiceCollectionExtensions.AddLattice"/> so the
/// codec stays warm across every WAL append. Hosts that wish to
/// substitute a different wire format register their own
/// implementation before that call (the default registration uses
/// <c>TryAddSingleton</c>).
/// </para>
/// </summary>
public sealed class OrleansBinaryWalMutationEncoder(Serializer<LatticeMutation> serializer) : IWalMutationEncoder
{
    private readonly Serializer<LatticeMutation> _serializer = serializer
        ?? throw new ArgumentNullException(nameof(serializer));

    /// <inheritdoc />
    public void Encode(in LatticeMutation mutation, IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        _serializer.Serialize(mutation, writer);
    }

    /// <inheritdoc />
    public LatticeMutation Decode(ReadOnlySpan<byte> encoded)
    {
        // Serializer<T> exposes a span overload of Deserialize that
        // avoids copying the bytes; we delegate directly.
        return _serializer.Deserialize(encoded);
    }
}
