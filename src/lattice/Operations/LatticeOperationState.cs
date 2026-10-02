namespace Orleans.Lattice.Operations;

/// <summary>
/// The lifecycle state of a coordinated long-running operation. <see cref="Queued"/>
/// and <see cref="Running"/> are the only non-terminal states.
/// </summary>
internal enum LatticeOperationState
{
    /// <summary>Accepted and recorded, not yet started by its runner.</summary>
    Queued = 0,

    /// <summary>In progress.</summary>
    Running = 1,

    /// <summary>Completed successfully.</summary>
    Succeeded = 2,

    /// <summary>Failed, including when the silo running it was lost.</summary>
    Failed = 3,

    /// <summary>Cancelled before it completed.</summary>
    Cancelled = 4,
}
