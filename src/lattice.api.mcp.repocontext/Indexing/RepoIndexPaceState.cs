namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// What the silo's shared indexing pacer is doing to the embedding drain loop
/// right now. It is reported on <see cref="RepoIndexPacing.State"/> so a job the
/// pacer has deliberately slowed reads as slowed rather than as stalled.
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.RepoIndexPaceState)]
public enum RepoIndexPaceState
{
    /// <summary>Pacing is switched off; batches run back to back as they always did.</summary>
    Disabled = 0,

    /// <summary>No embedding batch has run on this silo recently.</summary>
    Idle = 1,

    /// <summary>Batches are running at the full rate with no inter-batch delay.</summary>
    Pacing = 2,

    /// <summary>
    /// A congestion signal (a failed or slow batch, a throttled vector tree, or GC
    /// memory load) raised the inter-batch delay; it decays as clean batches land.
    /// </summary>
    Backoff = 3,

    /// <summary>A vector tree reported saturation and the drain loop is waiting, bounded, for it to recover.</summary>
    Waiting = 4,

    /// <summary>The drain loop finished a work slice and is resting before the next one.</summary>
    Resting = 5,

    /// <summary>A foreground search or context request is in flight and the drain loop is yielding to it, bounded.</summary>
    Yielding = 6,
}
