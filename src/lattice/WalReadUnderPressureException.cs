namespace Orleans.Lattice;

/// <summary>
/// Thrown by an <see cref="IWalStorageProvider"/> read when the process
/// cannot afford to materialise even a single-entry page, so the read did
/// not happen and no bytes were returned.
/// <para>
/// This is a <b>transient resource verdict, not a durability fault</b>. The
/// log is intact and the entries are still there; the machine simply has no
/// heap to put them in at this instant. It exists so a caller can tell that
/// case apart from a corrupt or truncated log, because the two have opposite
/// remedies: a corrupt log must fail loudly, whereas an unaffordable read
/// must bank whatever forward progress the caller already holds, give up the
/// current attempt, and retry later with a smaller window.
/// </para>
/// <para>
/// It is deliberately a distinct type rather than the underlying
/// <see cref="OutOfMemoryException"/>. A replay read crosses a grain boundary
/// (<c>ILeafReplayCoordinatorGrain.ReadSliceAsync</c>), and only a type the
/// serializer knows about survives that hop with its identity intact; a
/// caller matching on the BCL exception would be matching on whatever the
/// runtime happened to marshal. It also carries the numbers an operator needs
/// - which offset, and how many bytes the read could not find - which a bare
/// allocation failure does not.
/// </para>
/// <para>
/// Derives directly from <see cref="Exception"/> so the generated same-silo
/// deep copier can resolve a base-type copier: Orleans registers one for
/// <see cref="Exception"/> but not for its BCL subclasses.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalReadUnderPressure)]
internal sealed class WalReadUnderPressureException : Exception
{
    /// <summary>The tree whose write-ahead log was being read.</summary>
    [Id(0)] public string TreeId { get; set; } = string.Empty;

    /// <summary>The WAL partition (shard index) being read.</summary>
    [Id(1)] public int ShardIndex { get; set; }

    /// <summary>The offset of the entry the read could not materialise.</summary>
    [Id(2)] public long Offset { get; set; }

    /// <summary>Payload bytes the unaffordable entry required.</summary>
    [Id(3)] public long RequiredBytes { get; set; }

    /// <summary>Creates a new <see cref="WalReadUnderPressureException"/>.</summary>
    public WalReadUnderPressureException(
        string treeId,
        int shardIndex,
        long offset,
        long requiredBytes,
        Exception? innerException)
        : base(
            $"The write-ahead log read for tree '{treeId}' partition {shardIndex} could not materialise the "
            + $"single entry at offset {offset} ({requiredBytes} payload bytes) within the memory available to "
            + "this process. The log is intact and the entry is still durable; this read is unaffordable right "
            + "now. Bank the progress already applied, abandon this attempt, and retry when pressure has eased.",
            innerException)
    {
        TreeId = treeId;
        ShardIndex = shardIndex;
        Offset = offset;
        RequiredBytes = requiredBytes;
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public WalReadUnderPressureException() { }
}
