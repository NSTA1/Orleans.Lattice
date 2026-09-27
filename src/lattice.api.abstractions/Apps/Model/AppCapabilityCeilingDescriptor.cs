using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Operator-approved operations and exception scopes. The app's own namespace is
/// implicit; exception tree references never contain composed physical ids.
/// Consent is pinned by the containing request or report to an exact app version.
/// </summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppCapabilityCeilingDescriptor), Immutable]
public sealed record AppCapabilityCeilingDescriptor
{
    /// <summary>The maximum operation mask; no operations are approved by default.</summary>
    [Id(0)] public LatticeOperation AllowedOperations { get; init; } = LatticeOperation.None;
    /// <summary>The explicit approved external scopes; empty means structural app scope only.</summary>
    [Id(1)] public ImmutableArray<AppExceptionScope> ApprovedExceptionScopes { get; init; } = [];
}
