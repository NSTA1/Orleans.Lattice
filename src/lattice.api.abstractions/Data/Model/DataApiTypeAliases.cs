namespace Orleans.Lattice.Api.Data;

/// <summary>
/// Centralised Orleans serialization alias constants for every
/// <c>Orleans.Lattice.Api.Data</c> type that participates in the wire format.
/// Each alias is a short, fixed string that gives a type a stable wire identity
/// independent of its CLR name. Data-API facade aliases use the <c>olad.</c>
/// prefix (Orleans Lattice Api Data) to avoid collision with the read-only
/// state-API facade (<c>ola.</c>), the core (<c>ol.</c>), and the replication
/// (<c>olr.</c>) alias namespaces.
/// </summary>
/// <remarks>
/// Never rename or reuse an alias value: it is part of the on-the-wire and
/// persisted format. New types append new constants.
/// </remarks>
public static class DataApiTypeAliases
{
    /// <summary>Alias for <see cref="DataEntry"/>.</summary>
    public const string DataEntry = "olad.de";

    /// <summary>Alias for <see cref="DataAtomicBatch"/>.</summary>
    public const string DataAtomicBatch = "olad.ab";

    /// <summary>Alias for <see cref="DataTreeBatch"/>.</summary>
    public const string DataTreeBatch = "olad.tb";

    /// <summary>Alias for <see cref="DataRangeRequest"/>.</summary>
    public const string DataRangeRequest = "olad.rq";

    /// <summary>Alias for <see cref="DataRangePage"/>.</summary>
    public const string DataRangePage = "olad.rp";

    /// <summary>Alias for <see cref="DataReadResult"/>.</summary>
    public const string DataReadResult = "olad.rr";
}
