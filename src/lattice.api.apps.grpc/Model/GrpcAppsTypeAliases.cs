namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Stable aliases for app control transport messages.</summary>
public static class GrpcAppsTypeAliases
{
    /// <summary>The reserved app gRPC alias prefix.</summary>
    public const string AliasPrefix = "oiag.";
    /// <summary>Alias for the empty control request.</summary>
    public const string AppsEmptyRequest = "oiag.e";
    /// <summary>Alias for the app-slug request.</summary>
    public const string AppsSlugRequest = "oiag.s";
    /// <summary>Alias for the version-selecting description request.</summary>
    public const string AppsDescribeRequest = "oiag.d";
    /// <summary>Alias for a nullable description response.</summary>
    public const string AppsDescribeResponse = "oiag.r";
    /// <summary>Alias for a nullable consent response.</summary>
    public const string AppsConsentResponse = "oiag.c";
    /// <summary>Alias for the unauthenticated discovery request.</summary>
    public const string AuthSchemeAdvertisementRequest = "oiag.q";
    /// <summary>Alias for a public sign-in scheme.</summary>
    public const string AuthSchemeDescriptor = "oiag.a";
    /// <summary>Alias for the sign-in advertisement.</summary>
    public const string AuthSchemeAdvertisement = "oiag.b";
}
