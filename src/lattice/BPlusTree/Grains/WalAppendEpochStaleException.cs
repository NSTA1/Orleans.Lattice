namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Raised by a leaf whose WAL append was rejected because its activation's append
/// epoch has been superseded, or whose epoch claim lost to another activation
/// (epic #5605). The activation deactivates; a fresh call to the same leaf id
/// reaches the live activation.
/// </summary>
/// <remarks>
/// <para>
/// Internal to the seam retry (<see cref="WalFenceSeamRetry"/>): every caller of
/// the leaf write surface retries it until the operation's existing deadline, so
/// it never crosses a public API boundary and is never the inner exception of
/// anything public. When the deadline expires the caller sees exactly what it sees
/// today when that deadline expires.
/// </para>
/// <para>
/// Derives directly from <see cref="Exception"/>, so the same-silo deep copier
/// Orleans registers for <see cref="Exception"/> covers its base slice.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.WalAppendEpochStaleException)]
internal sealed class WalAppendEpochStaleException : Exception
{
    /// <summary>Creates a new <see cref="WalAppendEpochStaleException"/>.</summary>
    /// <param name="leafId">The leaf whose activation was superseded.</param>
    /// <param name="epoch">The epoch the superseded activation held.</param>
    public WalAppendEpochStaleException(Guid leafId, long epoch)
        : base($"Leaf {leafId:N} activation at append epoch {epoch} has been superseded by a newer activation; "
            + "the write was not appended and is retried against the live activation.")
    {
        LeafId = leafId;
        Epoch = epoch;
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public WalAppendEpochStaleException() { }

    /// <summary>The leaf whose activation was superseded.</summary>
    [Id(0)] public Guid LeafId { get; set; }

    /// <summary>The epoch the superseded activation held.</summary>
    [Id(1)] public long Epoch { get; set; }
}
