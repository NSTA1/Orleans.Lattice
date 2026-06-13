namespace Orleans.Lattice.Replication;

/// <summary>
/// Receiver-to-peer request for the shared-compression-dictionary pull
/// round trip: asks the advertising peer for the raw bytes of a dictionary
/// id the local provider does not yet hold, so an auto-training cluster can
/// converge onto a peer's trained dictionary (install it locally and then
/// both decode frames compressed against it and advertise it onward)
/// instead of hard-failing decode. The request carries only the id; the
/// peer answers with the bytes and their content fingerprint (see
/// <see cref="CompressionDictionaryPullResponse"/>), which the puller
/// verifies against the fingerprint the peer advertised before installing.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.CompressionDictionaryPullRequest)]
[Immutable]
public readonly record struct CompressionDictionaryPullRequest
{
    /// <summary>
    /// The stable shared-dictionary id whose bytes are requested. The
    /// reserved value <c>0</c> ("no dictionary") is never pulled.
    /// </summary>
    [Id(0)] public uint DictionaryId { get; init; }
}
