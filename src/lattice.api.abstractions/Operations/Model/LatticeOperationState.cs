namespace Orleans.Lattice.Api.Operations;

/// <summary>
/// The lifecycle state of a long-running operation. <see cref="Queued"/> and
/// <see cref="Running"/> are the only non-terminal states.
/// </summary>
[GenerateSerializer]
[Alias(ApiOperationTypeAliases.LatticeOperationState)]
public enum LatticeOperationState
{
    /// <summary>Accepted and recorded, not yet started.</summary>
    Queued = 0,

    /// <summary>In progress.</summary>
    Running = 1,

    /// <summary>Completed successfully. The result is on the status.</summary>
    Succeeded = 2,

    /// <summary>
    /// Failed. The status carries the reason. An operation whose silo was lost while
    /// it ran is reported as failed, never left running; resuming it is not supported.
    /// </summary>
    Failed = 3,

    /// <summary>Cancelled before it completed.</summary>
    Cancelled = 4,
}
