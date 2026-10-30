namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The outcome of <see cref="IReplicationApplyGrain.ApplySetIfVersionAsync"/>.
/// It tells a version miss apart from a write the write choke point diverted, which
/// the public <see cref="ILattice.SetIfVersionAsync"/> reports as the same
/// <see langword="false"/> (issue #5265).
/// </summary>
/// <remarks>
/// The zero value is <see cref="VersionMismatch"/>, the conservative answer: a
/// caller that sees an unknown or default value re-reads and retries, as on a real
/// miss, and never treats the value as installed or diverted.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.ReplicationConditionalSetOutcome)]
internal enum ReplicationConditionalSetOutcome
{
    /// <summary>
    /// The stored version did not match the expected one, so nothing was written.
    /// The caller re-reads and retries.
    /// </summary>
    VersionMismatch = 0,

    /// <summary>The value was installed at the key.</summary>
    Applied = 1,

    /// <summary>
    /// The write interceptor dead-lettered the value: it was diverted and never
    /// made durable at the key. Retrying would divert it again.
    /// </summary>
    Diverted = 2,
}
