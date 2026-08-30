namespace Orleans.Lattice.Api.Telemetry.Grpc;

/// <summary>
/// Wire request for the read-only <c>GetCatalog</c> RPC. Carries no fields: the
/// catalogue a caller may select from is derived entirely from the authenticated
/// caller, so the caller identity travels in the credential header and never in
/// the payload.
/// </summary>
/// <remarks>
/// The absence of fields is deliberate and load-bearing. A field naming a tenant
/// would be a scoping input the transport could assert, and a field carrying text
/// would be a query the caller composed - both of which the facade contract
/// forbids. There is nothing here to widen.
/// </remarks>
[GenerateSerializer]
[Alias(GrpcTelemetryTypeAliases.TelemetryCatalogRequest)]
[Immutable]
public sealed record TelemetryCatalogRequest;
