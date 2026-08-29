namespace Orleans.Lattice.Api.Telemetry.Grpc;

/// <summary>
/// Centralized Orleans serialization alias constants for the wire messages the
/// <c>Orleans.Lattice.Api.Telemetry.Grpc</c> binding adds on top of the
/// transport-agnostic telemetry facade DTOs. Grpc-binding aliases use the
/// <c>oitlg.</c> prefix (Orleans Lattice Api Telemetry Grpc) to avoid collision
/// with the telemetry control-API contract (<c>oitl.</c>), the tenant-administration
/// control-API facade (<c>oitn.</c>) and its gRPC binding (<c>oitng.</c>), the
/// tree-administration control-API facade (<c>oit.</c>) and its gRPC binding
/// (<c>oitg.</c>), and the core (<c>ol.</c>) alias namespaces.
/// </summary>
/// <remarks>
/// <para>
/// The binding adds only the messages the transport itself needs: a marker
/// request for the catalogue read and the auth-scheme advertisement trio. The
/// telemetry request and both responses are the contract's own DTOs, carried on
/// the wire unchanged under their existing <c>oitl.</c> aliases, so the binding
/// cannot drift from the facade surface and cannot introduce a request shape that
/// carries query text or asserts a tenant.
/// </para>
/// <para>
/// Never rename or reuse an alias value: it is part of the on-the-wire format.
/// New types append new constants.
/// </para>
/// </remarks>
public static class GrpcTelemetryTypeAliases
{
    /// <summary>
    /// The reserved alias prefix owned by the telemetry gRPC binding. Every alias
    /// constant here starts with this value.
    /// </summary>
    public const string AliasPrefix = "oitlg.";

    /// <summary>Alias for <see cref="TelemetryCatalogRequest"/>.</summary>
    public const string TelemetryCatalogRequest = "oitlg.catreq";

    /// <summary>Alias for <see cref="AuthSchemeAdvertisementRequest"/>.</summary>
    public const string AuthSchemeAdvertisementRequest = "oitlg.asreq";

    /// <summary>Alias for <see cref="AuthSchemeDescriptor"/>.</summary>
    public const string AuthSchemeDescriptor = "oitlg.asdesc";

    /// <summary>Alias for <see cref="AuthSchemeAdvertisement"/>.</summary>
    public const string AuthSchemeAdvertisement = "oitlg.asadv";
}
