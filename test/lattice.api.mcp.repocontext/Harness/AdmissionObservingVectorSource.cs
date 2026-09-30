using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// A source decorator that records the ambient
/// <see cref="LatticeReplayAdmissionClass"/> at the moment each operation is
/// served, so a fixture can assert how the build's own reads are classified to
/// the replay admission gate.
/// <para>
/// The class flows ambiently on <c>RequestContext</c> and is read by the leaf
/// grain at the far end of a call this fake stands in for, which is precisely why
/// it cannot be asserted at the call site: nothing in the handle's signature
/// mentions it, and a scope opened around the wrong region is invisible except to
/// the code it fails to cover. Observing it from INSIDE the read is the only
/// placement that answers the question actually being asked - "was the work this
/// phase performs classified Bulk?" - rather than the weaker "was a scope opened
/// somewhere?".
/// </para>
/// </summary>
/// <param name="inner">The source to delegate to.</param>
internal sealed class AdmissionObservingVectorSource(InMemoryRepoContextVectorSource inner)
    : IRepoContextVectorSource
{
    private readonly List<LatticeReplayAdmissionClass> _enumerations = [];
    private readonly List<LatticeReplayAdmissionClass> _counts = [];
    private readonly List<LatticeReplayAdmissionClass> _sourceKeyResolutions = [];
    private readonly List<LatticeReplayAdmissionClass> _containments = [];

    /// <summary>The class observed on each corpus enumeration, in order.</summary>
    public IReadOnlyList<LatticeReplayAdmissionClass> Enumerations => _enumerations;

    /// <summary>The class observed on each corpus count, in order.</summary>
    public IReadOnlyList<LatticeReplayAdmissionClass> Counts => _counts;

    /// <summary>
    /// The class observed on each source-key resolution, in order. This is the
    /// catch-up phase's own read, so it reports how the reconcile work is
    /// classified rather than the ingest.
    /// </summary>
    public IReadOnlyList<LatticeReplayAdmissionClass> SourceKeyResolutions => _sourceKeyResolutions;

    /// <summary>The class observed on each membership probe, in order.</summary>
    public IReadOnlyList<LatticeReplayAdmissionClass> Containments => _containments;

    /// <summary>
    /// Every observation this decorator has made, in no particular order. Used by
    /// a fixture that cares which CLASS was in force rather than which call
    /// carried it.
    /// </summary>
    public IReadOnlyList<LatticeReplayAdmissionClass> All =>
        [.. _enumerations, .. _counts, .. _sourceKeyResolutions, .. _containments];

    public int Dimensions => inner.Dimensions;

    public async IAsyncEnumerable<VectorSourceEntry> EnumerateAsync(
        string? afterIdExclusive,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _enumerations.Add(LatticeReplayAdmissionContext.Current);
        await foreach (var entry in inner.EnumerateAsync(afterIdExclusive, cancellationToken).ConfigureAwait(false))
        {
            yield return entry;
        }
    }

    public Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        _counts.Add(LatticeReplayAdmissionContext.Current);
        return inner.CountAsync(cancellationToken);
    }

    public Task<bool> ContainsAsync(string id, CancellationToken cancellationToken = default)
    {
        _containments.Add(LatticeReplayAdmissionContext.Current);
        return inner.ContainsAsync(id, cancellationToken);
    }

    public Task<IReadOnlyDictionary<string, string>> ResolveSourceKeysAsync(
        IReadOnlyList<string> vectorIds, CancellationToken cancellationToken = default)
    {
        _sourceKeyResolutions.Add(LatticeReplayAdmissionContext.Current);
        return inner.ResolveSourceKeysAsync(vectorIds, cancellationToken);
    }
}
