namespace Orleans.Lattice.Explorer.Core.DeadLetter;

/// <summary>
/// The explorer's view of the ingest path that produced a strict-mode
/// dead-letter entry, projected from the state-API
/// <see cref="Orleans.Lattice.Api.State.DeadLetterSourceKind"/> so the UI never
/// binds directly to the wire enum.
/// </summary>
public enum DeadLetterSource
{
    /// <summary>The item arrived via cross-cluster replication apply.</summary>
    Replication = 0,

    /// <summary>The item arrived via a backup restore / bulk load.</summary>
    Restore = 1,

    /// <summary>The item was a rejected local write a deployment opted to retain for inspection.</summary>
    LocalRejected = 2,

    /// <summary>The source could not be mapped to a known kind (forward-compatibility fallback).</summary>
    Unknown = 3,
}
