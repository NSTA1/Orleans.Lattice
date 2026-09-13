namespace Orleans.Lattice;

/// <summary>
/// Thrown when an activating leaf cannot materialise its persisted snapshot
/// within the memory this process has, so the activation is declined rather than
/// allowed to fall through to the colder, larger recovery path (issue #2765).
/// <para>
/// The behaviour it replaces is the one that turned a memory shortage into a
/// restart loop. A snapshot load that failed was previously swallowed and
/// reported as "this leaf simply has no snapshot", which sends the activation
/// down the whole-window write-ahead-log replay - a path that allocates
/// <b>more</b> than the load which had just failed for want of memory. Under a
/// container memory limit that is a positive feedback loop: every leaf that
/// fails for want of heap immediately asks for more heap, so pressure rises
/// until the process dies, and it dies with a managed
/// <see cref="OutOfMemoryException"/>, exit code 0 and no OOM-kill flag.
/// </para>
/// <para>
/// Declining one leaf is strictly less harm than losing every leaf. Orleans
/// retries the activation on the next call, by which time the admission gate
/// will have let the storm drain, so the decline is transient and needs no
/// operator action. Nothing durable is lost: no coverage claim is advanced and
/// no state is written, so a later activation sees exactly the state this one
/// saw.
/// </para>
/// <para>
/// Kept distinct from the underlying <see cref="OutOfMemoryException"/> for the
/// same reason as <c>WalReadUnderPressureException</c>: it crosses a grain
/// boundary, and only a type the serializer knows about survives that hop with
/// its identity intact. Derives directly from <see cref="Exception"/> so the
/// generated same-silo deep copier can resolve a base-type copier, which Orleans
/// registers for <see cref="Exception"/> but not for its BCL subclasses.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafSnapshotUnaffordable)]
internal sealed class LeafSnapshotUnaffordableException : Exception
{
    /// <summary>The tree whose leaf could not be hydrated.</summary>
    [Id(0)] public string TreeId { get; set; } = string.Empty;

    /// <summary>Bytes the hydration had reserved when it failed.</summary>
    [Id(1)] public long ReservedBytes { get; set; }

    /// <summary>Aggregate bytes the admission gate will allow concurrently.</summary>
    [Id(2)] public long BudgetBytes { get; set; }

    /// <summary>
    /// The largest single <b>contiguous</b> allocation the failed hydration
    /// required (issue #2844). Carried separately from
    /// <see cref="ReservedBytes"/> because the two are different predicates and
    /// only one of them is a budget: a reservation is satisfied out of total
    /// free bytes, whereas this one has to be met by an unbroken run of memory,
    /// which no amount of aggregate headroom guarantees.
    /// </summary>
    [Id(3)] public long ContiguousBytes { get; set; }

    /// <summary>Creates a new <see cref="LeafSnapshotUnaffordableException"/>.</summary>
    public LeafSnapshotUnaffordableException(
        string treeId,
        long reservedBytes,
        long budgetBytes,
        long contiguousBytes,
        Exception? innerException)
        : base(
            BuildMessage(treeId, reservedBytes, budgetBytes, contiguousBytes),
            innerException)
    {
        TreeId = treeId;
        ReservedBytes = reservedBytes;
        BudgetBytes = budgetBytes;
        ContiguousBytes = contiguousBytes;
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public LeafSnapshotUnaffordableException() { }

    // Two messages, because the two failures call for opposite reactions and a
    // single message that averaged them would mislead on both (issue #2844).
    //
    // The message this replaces quoted reserved-against-budget unconditionally.
    // On the arm where the claim FITTED - 394,270,800 bytes of a 1,207,959,552
    // byte budget, with 776 MiB never used - that phrasing reads as "the budget
    // was too small" and sends a reader straight to enlarging the memory grant.
    // That is the single worst available action: the hydration budget, the
    // resident working-set budget and the replay gate are all derived from the
    // grant and every one of them admits MORE concurrent work as it grows, while
    // the quantity that actually ran out - one unbroken run of memory - does not
    // improve at all. So the exception has to name which predicate failed, not
    // merely report the numbers and leave the inference to the reader.
    private static string BuildMessage(
        string treeId,
        long reservedBytes,
        long budgetBytes,
        long contiguousBytes)
    {
        var prefix =
            $"The leaf snapshot for tree '{treeId}' could not be materialised within the memory available to "
            + "this process. ";

        var diagnosis = reservedBytes <= budgetBytes
            ? $"The hydration claim FITTED the gate's budget (reserved {reservedBytes} bytes of {budgetBytes}, "
                + $"leaving {budgetBytes - reservedBytes} bytes unused) and the load failed anyway while "
                + $"materialising a single contiguous {contiguousBytes} byte buffer. This is a CONTIGUITY "
                + "failure, not a shortage of total memory: raising this process's memory limit will not fix "
                + "it and will make it more frequent, because the hydration budget, the resident working-set "
                + "budget and the replay gate are all derived from that limit and each admits more concurrent "
                + "work as it rises. Divide this leaf, or reduce MaxLeafBytes for this tree, so that no single "
                + "snapshot has to be read into one buffer this large. "
            : $"The hydration claim exceeded the gate's budget (reserved {reservedBytes} bytes against a "
                + $"{budgetBytes} byte hydration budget) and was admitted as sole occupant, requiring a single "
                + $"contiguous {contiguousBytes} byte buffer. ";

        return prefix
            + diagnosis
            + "The snapshot is intact and still durable; this activation is unaffordable right now. It is "
            + "declined deliberately, because replaying the whole write-ahead-log window instead would allocate "
            + "more than the load that just failed and drive the process into a restart loop. Orleans will retry "
            + "the activation once the cold-start storm has drained; no operator action is required.";
    }
}
