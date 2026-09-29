namespace Orleans.Lattice.Explorer.Shell.Areas.Replication;

/// <summary>Why a replication read produced no data.</summary>
internal enum ReplicationFaultKind
{
    /// <summary>The caller is not allowed to read it.</summary>
    Denied = 0,

    /// <summary>The cluster does not serve the facade (or none is registered).</summary>
    NotServed = 1,

    /// <summary>The cluster could not answer: not connected, unavailable, or an unexpected fault.</summary>
    Failed = 2,
}
