namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Centralised Orleans serialization alias constants for the wire messages
/// the <c>Orleans.Lattice.Api.State.Grpc</c> binding adds on top of the
/// transport-agnostic facade DTOs. Grpc-binding aliases use the
/// <c>olag.</c> prefix (Orleans Lattice Api Grpc) to avoid collision with the
/// facade (<c>ola.</c>), core (<c>ol.</c>), and replication (<c>olr.</c>)
/// alias namespaces.
/// </summary>
/// <remarks>
/// Never rename or reuse an alias value: it is part of the on-the-wire
/// format. New types append new constants.
/// </remarks>
public static class GrpcStateTypeAliases
{
    /// <summary>Alias for <see cref="EntryGetRequest"/>.</summary>
    public const string EntryGetRequest = "olag.egq";

    /// <summary>Alias for <see cref="StructureResponse"/>.</summary>
    public const string StructureResponse = "olag.sresp";

    /// <summary>Alias for <see cref="EntryScanResponse"/>.</summary>
    public const string EntryScanResponse = "olag.esresp";

    /// <summary>Alias for <see cref="EntryScanCancelResponse"/>.</summary>
    public const string EntryScanCancelResponse = "olag.escresp";

    /// <summary>Alias for <see cref="EntryGetResponse"/>.</summary>
    public const string EntryGetResponse = "olag.egresp";

    /// <summary>Alias for <see cref="EntryHistoryResponse"/>.</summary>
    public const string EntryHistoryResponse = "olag.ehresp";

    /// <summary>Alias for <see cref="AuthSchemeAdvertisementRequest"/>.</summary>
    public const string AuthSchemeAdvertisementRequest = "olag.asreq";

    /// <summary>Alias for <see cref="AuthSchemeDescriptor"/>.</summary>
    public const string AuthSchemeDescriptor = "olag.asdesc";

    /// <summary>Alias for <see cref="AuthSchemeAdvertisement"/>.</summary>
    public const string AuthSchemeAdvertisement = "olag.asadv";
}
