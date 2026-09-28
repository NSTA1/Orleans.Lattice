using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>A full replacement consent for an exact installed version, using no composed tree ids.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppConsentUpdate), Immutable]
public sealed record AppConsentUpdate
{
    /// <summary>The installed app slug.</summary>
    [Id(0)] public required string Slug { get; init; }
    /// <summary>The expected installed version; mismatch must reject the update.</summary>
    [Id(1)] public required string Version { get; init; }
    /// <summary>The replacement operation mask and complete approved exception list.</summary>
    [Id(2)] public required AppCapabilityCeilingDescriptor Ceiling { get; init; }
    /// <summary>
    /// The replacement consented bridge grants, or null to leave them unchanged. An
    /// upgrade that requests a grant outside this set cannot activate until it is
    /// re-consented.
    /// </summary>
    [Id(3)] public ImmutableArray<AppUiBridgeGrantDescriptor>? BridgeGrants { get; init; }
}
