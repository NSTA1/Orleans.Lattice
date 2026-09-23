namespace Orleans.Lattice.Testing;

/// <summary>
/// Consumer-supplied adapter over one WAL storage provider, used by
/// <see cref="WalOffsetAllocationContractTestsBase"/>, including its reconcile tests.
/// <para>
/// The shared testing library is deliberately product-agnostic and references no
/// Orleans.Lattice assembly, so the conformance suite cannot name
/// <c>IWalStorageProvider</c> directly. Each provider's own test project
/// implements this narrow probe instead - a handful of offset-shaped operations,
/// with no payload, encoder, or option surface - and inherits the contract tests
/// by construction rather than by copy-paste.
/// </para>
/// </summary>
public interface IWalOffsetAllocationProbe : IAsyncDisposable
{
    /// <summary>
    /// Appends one committed entry per supplied offset. Offsets arrive strictly
    /// ascending and gap-free, as the WAL grain emits them.
    /// </summary>
    Task AppendAsync(IReadOnlyList<long> offsets, CancellationToken cancellationToken);

    /// <summary>Trims every entry at or below <paramref name="throughOffsetInclusive"/>.</summary>
    Task TrimAsync(long throughOffsetInclusive, CancellationToken cancellationToken);

    /// <summary>Provider's <c>GetHighestOffsetAsync</c>.</summary>
    Task<long> GetHighestOffsetAsync(CancellationToken cancellationToken);

    /// <summary>Provider's <c>GetLowestOffsetAsync</c>.</summary>
    Task<long> GetLowestOffsetAsync(CancellationToken cancellationToken);

    /// <summary>The offsets still readable from the log, ascending.</summary>
    Task<IReadOnlyList<long>> ReadLiveOffsetsAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Appends one entry per supplied offset and returns as soon as the
    /// provider's own append returns, <b>without</b> crossing any further
    /// durability barrier that <see cref="AppendAsync"/>
    /// adds. This is the acknowledgement the WAL grain acts on: a provider may
    /// still be completing the append in the background (a pipelined commit, for
    /// example) when it returns. A provider with no such background work
    /// implements this exactly as <see cref="AppendAsync"/>.
    /// </summary>
    Task AppendAcknowledgedAsync(IReadOnlyList<long> offsets, CancellationToken cancellationToken);

    /// <summary>
    /// Provider's <c>ReconcileAsync</c>. The WAL grain calls it on activation and
    /// again in its post-failure resync, immediately before reading the highest
    /// offset, so it sits directly on the offset-allocation path.
    /// </summary>
    Task ReconcileAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Models a process restart: durable providers must drop all in-memory state
    /// and re-read from their backing store. A volatile provider implements this
    /// as a no-op, which is correct - it simply cannot lose the entries, and the
    /// offset-allocation assertions still apply to it unchanged.
    /// </summary>
    Task ReopenAsync(CancellationToken cancellationToken);
}
