namespace Orleans.Lattice.BPlusTree.Grains;

using System.ComponentModel;

/// <summary>
/// Internal-only grain interface that mirrors the narrow subset of
/// <see cref="ILattice"/> operations the library itself needs to perform
/// against reserved system trees (names starting with
/// <see cref="LatticeConstants.SystemTreePrefix"/>, including the
/// registry tree <see cref="LatticeConstants.RegistryTreeId"/> and the
/// replication write-ahead-log prefix
/// <see cref="LatticeConstants.ReplogTreePrefix"/>).
/// <para>
/// <see cref="LatticeGrain"/> implements both <see cref="ILattice"/>
/// (public, guarded) and this interface (internal, unguarded) against the
/// same activation: the public surface rejects any call whose primary
/// key starts with <see cref="LatticeConstants.SystemTreePrefix"/> with
/// <see cref="InvalidOperationException"/>, while internal callers that
/// legitimately bootstrap system trees resolve
/// <c>grainFactory.GetGrain&lt;ISystemLattice&gt;(...)</c> and bypass the
/// guard. Because this interface is <see langword="internal"/>, user
/// code in other assemblies cannot call
/// <c>grainFactory.GetGrain&lt;ISystemLattice&gt;</c> &#8212; the type is
/// not visible &#8212; which is what makes the public guard genuinely
/// unbypassable for external callers.
/// </para>
/// </summary>
[Alias(TypeAliases.ISystemLattice)]
[EditorBrowsable(EditorBrowsableState.Never)]
internal interface ISystemLattice : IGrainWithStringKey
{
    /// <summary>Gets the value associated with <paramref name="key"/>, or <c>null</c> if not found.</summary>
    Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sets <paramref name="key"/> to <paramref name="value"/>.</summary>
    Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>Deletes <paramref name="key"/>. Returns <c>true</c> if a live entry was tombstoned.</summary>
    Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Returns <c>true</c> if <paramref name="key"/> exists and is not tombstoned.</summary>
    Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams keys in sorted order, optionally bounded by
    /// <paramref name="startInclusive"/> / <paramref name="endExclusive"/>.
    /// </summary>
    IAsyncEnumerable<string> KeysAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool? prefetch = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams key/value entries in sorted key order, optionally bounded by
    /// <paramref name="startInclusive"/> / <paramref name="endExclusive"/>.
    /// </summary>
    IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool? prefetch = null, CancellationToken cancellationToken = default);
}

