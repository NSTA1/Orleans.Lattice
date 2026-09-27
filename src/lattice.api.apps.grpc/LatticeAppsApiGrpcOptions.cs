namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Host configuration for the app-control gRPC endpoint.</summary>
public sealed class LatticeAppsApiGrpcOptions
{
    /// <summary>Enforces the transport authorizer by default; disable only behind an outer authentication boundary.</summary>
    public bool RequireAuthorization { get; set; } = true;

    /// <summary>The credential metadata key, bridged even when transport authorization is disabled.</summary>
    public string CredentialHeaderName { get; set; } = "authorization";

    /// <summary>The optional prefix stripped from the token and stamped as its authentication scheme.</summary>
    public string CredentialScheme { get; set; } = "Bearer";

    /// <summary>The asserted tenant header; null or empty disables it. The facade must validate this assertion.</summary>
    public string ActiveTenantHeaderName { get; set; } = LatticeActiveTenantAssertion.DefaultHeaderName;

    /// <summary>Public sign-in schemes in preference order. Never include credentials or user-specific data.</summary>
    public IList<AuthSchemeDescriptor> AdvertisedAuthSchemes { get; } = new List<AuthSchemeDescriptor>();
}
