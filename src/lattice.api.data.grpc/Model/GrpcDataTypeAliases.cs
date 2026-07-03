namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Centralised Orleans serialization alias constants for the wire messages the
/// <c>Orleans.Lattice.Api.Data.Grpc</c> binding adds on top of the
/// transport-agnostic facade DTOs. Grpc-binding aliases use the <c>oladg.</c>
/// prefix (Orleans Lattice Api Data Grpc) to avoid collision with the data-API
/// facade (<c>olad.</c>), the state-API facade (<c>ola.</c>), core (<c>ol.</c>),
/// and replication (<c>olr.</c>) alias namespaces.
/// </summary>
/// <remarks>
/// Never rename or reuse an alias value: it is part of the on-the-wire format.
/// New types append new constants.
/// </remarks>
public static class GrpcDataTypeAliases
{
    /// <summary>Alias for <see cref="DataSetRequest"/>.</summary>
    public const string DataSetRequest = "oladg.setq";

    /// <summary>Alias for <see cref="DataSetResponse"/>.</summary>
    public const string DataSetResponse = "oladg.setr";

    /// <summary>Alias for <see cref="DataDeleteRequest"/>.</summary>
    public const string DataDeleteRequest = "oladg.delq";

    /// <summary>Alias for <see cref="DataDeleteResponse"/>.</summary>
    public const string DataDeleteResponse = "oladg.delr";

    /// <summary>Alias for <see cref="DataAtomicRequest"/>.</summary>
    public const string DataAtomicRequest = "oladg.atmq";

    /// <summary>Alias for <see cref="DataAtomicResponse"/>.</summary>
    public const string DataAtomicResponse = "oladg.atmr";

    /// <summary>Alias for <see cref="DataCrossTreeRequest"/>.</summary>
    public const string DataCrossTreeRequest = "oladg.xtq";

    /// <summary>Alias for <see cref="DataCrossTreeResponse"/>.</summary>
    public const string DataCrossTreeResponse = "oladg.xtr";

    /// <summary>Alias for <see cref="DataGetRequest"/>.</summary>
    public const string DataGetRequest = "oladg.getq";
}
