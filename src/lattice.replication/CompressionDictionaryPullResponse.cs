namespace Orleans.Lattice.Replication;

/// <summary>
/// Peer-to-receiver response for the shared-compression-dictionary pull
/// round trip. Reports whether the peer can serve the pull at all
/// (<see cref="ExchangeSupported"/>), whether it actually holds the
/// requested id (<see cref="Found"/>), and - when it does - the dictionary
/// bytes (<see cref="Dictionary"/>) and their content fingerprint
/// (<see cref="Fingerprint"/>). The puller recomputes the fingerprint of
/// the returned bytes and installs them only when it matches both the
/// fingerprint the peer advertised and the <see cref="Fingerprint"/> on
/// this response, so a corrupted or mismatched payload can never silently
/// overwrite or corrupt a dictionary.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.CompressionDictionaryPullResponse)]
[Immutable]
public readonly record struct CompressionDictionaryPullResponse
{
    /// <summary>
    /// <see langword="true"/> when the peer (and the transport binding)
    /// performed the pull exchange and <see cref="Found"/> is
    /// authoritative; <see langword="false"/> when the peer or transport
    /// cannot serve the pull (an un-upgraded peer, or a momentarily
    /// unreachable hop), in which case the puller leaves the dictionary
    /// uninstalled and tries again on a later tick.
    /// </summary>
    [Id(0)] public bool ExchangeSupported { get; init; }

    /// <summary>
    /// <see langword="true"/> when the peer holds <see cref="DictionaryId"/>
    /// and <see cref="Dictionary"/> carries its bytes; <see langword="false"/>
    /// when the peer supports the exchange but does not (yet) hold the id.
    /// Meaningful only when <see cref="ExchangeSupported"/> is
    /// <see langword="true"/>.
    /// </summary>
    [Id(1)] public bool Found { get; init; }

    /// <summary>The dictionary id the bytes were resolved for.</summary>
    [Id(2)] public uint DictionaryId { get; init; }

    /// <summary>
    /// The content fingerprint of <see cref="Dictionary"/>
    /// (<see cref="CompressionDictionaryFingerprint.Compute(System.ReadOnlySpan{byte})"/>),
    /// echoed so the puller can cross-check it against the advertised
    /// fingerprint and the bytes it received. <c>0</c> when
    /// <see cref="Found"/> is <see langword="false"/>.
    /// </summary>
    [Id(3)] public ulong Fingerprint { get; init; }

    /// <summary>
    /// The resolved dictionary bytes, or empty when <see cref="Found"/> is
    /// <see langword="false"/>.
    /// </summary>
    [Id(4)] public ReadOnlyMemory<byte> Dictionary { get; init; }

    /// <summary>
    /// The response a transport (or peer) that has not implemented the pull
    /// returns: <see cref="ExchangeSupported"/> and <see cref="Found"/> are
    /// both <see langword="false"/>. The puller treats this as "try again
    /// later".
    /// </summary>
    public static CompressionDictionaryPullResponse NotSupported => new()
    {
        ExchangeSupported = false,
        Found = false,
        Dictionary = ReadOnlyMemory<byte>.Empty,
    };

    /// <summary>
    /// The response a peer that supports the pull but does not hold the
    /// requested id returns: <see cref="ExchangeSupported"/> is
    /// <see langword="true"/> and <see cref="Found"/> is
    /// <see langword="false"/>.
    /// </summary>
    public static CompressionDictionaryPullResponse NotHeld => new()
    {
        ExchangeSupported = true,
        Found = false,
        Dictionary = ReadOnlyMemory<byte>.Empty,
    };
}
