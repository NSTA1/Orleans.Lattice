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
    /// <summary>Alias for the app-source list response.</summary>
    public const string AppsSourcesResponse = "oiag.ss";
    /// <summary>Alias for the source-app selection request.</summary>
    public const string AppsSourceAppRequest = "oiag.sa";
    /// <summary>Alias for a nullable icon response.</summary>
    public const string AppsIconResponse = "oiag.ic";
    /// <summary>Alias for the workspace app list response.</summary>
    public const string AppsWorkspaceListResponse = "oiag.wl";
    /// <summary>Alias for a nullable workspace description response.</summary>
    public const string AppsWorkspaceDescribeResponse = "oiag.wd";
    /// <summary>Alias for the UI asset request.</summary>
    public const string AppsUiAssetRequest = "oiag.uq";
    /// <summary>Alias for a nullable UI asset response.</summary>
    public const string AppsUiAssetResponse = "oiag.ur";
    /// <summary>Alias for the app bridge single-key request.</summary>
    public const string AppsBridgeKeyRequest = "oiag.bk";
    /// <summary>Alias for a nullable app bridge read response.</summary>
    public const string AppsBridgeGetResponse = "oiag.bg";
    /// <summary>Alias for the app bridge scan request.</summary>
    public const string AppsBridgeScanRequest = "oiag.bs";
    /// <summary>Alias for the app bridge write request.</summary>
    public const string AppsBridgeSetRequest = "oiag.bw";
    /// <summary>Alias for the app bridge delete response.</summary>
    public const string AppsBridgeDeleteResponse = "oiag.bd";}
