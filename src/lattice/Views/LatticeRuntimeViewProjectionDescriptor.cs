namespace Orleans.Lattice;

/// <summary>
/// Identifies a host-registered runtime-view projection provider and carries the
/// bounded opaque state that provider needs to reconstruct a view after restart.
/// </summary>
public sealed class LatticeRuntimeViewProjectionDescriptor
{
    /// <summary>The maximum provider payload size persisted for one runtime view.</summary>
    public const int MaxPayloadBytes = 64 * 1024;

    private readonly byte[] _payload;

    /// <summary>Creates a runtime projection descriptor.</summary>
    /// <param name="providerKey">The non-empty key registered through <c>AddLatticeViews</c>.</param>
    /// <param name="payload">Opaque provider state, limited to <see cref="MaxPayloadBytes"/> bytes.</param>
    public LatticeRuntimeViewProjectionDescriptor(string providerKey, ReadOnlySpan<byte> payload)
    {
        ArgumentException.ThrowIfNullOrEmpty(providerKey);
        if (payload.Length > MaxPayloadBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(payload),
                payload.Length,
                $"A runtime projection payload cannot exceed {MaxPayloadBytes} bytes.");
        }

        ProviderKey = providerKey;
        _payload = payload.ToArray();
    }

    /// <summary>The host-registered provider key.</summary>
    public string ProviderKey { get; }

    /// <summary>A defensive copy of the opaque provider payload.</summary>
    public byte[] Payload => _payload.ToArray();

    internal ReadOnlySpan<byte> PayloadSpan => _payload;
}
