namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// What a WAL shard activation must do about the durable move fence it read with
/// its placement pin (issue #4525). Decided by
/// <see cref="WalMoveFenceCore.EvaluateActivationFence"/>.
/// </summary>
internal enum WalMoveFenceActivation
{
    /// <summary>No move holds the partition on the provider this activation resolved: serve appends.</summary>
    Unfenced,

    /// <summary>A live move fence holds the partition: come up fenced and refuse appends until it lapses.</summary>
    Fenced,

    /// <summary>
    /// The fence's lease has lapsed: release it durably, re-resolve placement from
    /// the pin the release returns, and only then serve appends.
    /// </summary>
    ReleaseExpired,
}
