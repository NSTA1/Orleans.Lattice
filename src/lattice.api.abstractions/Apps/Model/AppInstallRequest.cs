using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>An exact source version, group bindings, and explicit consent to install without enabling.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppInstallRequest), Immutable]
public sealed record AppInstallRequest
{
    /// <summary>The validated app slug, transported as a string.</summary>
    [Id(0)] public required string Slug { get; init; }
    /// <summary>The exact semantic version to install, transported as a string.</summary>
    [Id(1)] public required string Version { get; init; }
    /// <summary>The operator's role-to-membership-group bindings.</summary>
    [Id(2)] public ImmutableArray<AppRoleBindingDescriptor> RoleBindings { get; init; } = [];
    /// <summary>The explicit operator-approved ceiling; scope references never carry composed physical ids.</summary>
    [Id(3)] public required AppCapabilityCeilingDescriptor Ceiling { get; init; }
    /// <summary>
    /// The key of the source to install from, or null to resolve the slug across every
    /// source; an ambiguous slug then fails rather than choosing a source.
    /// </summary>
    [Id(4)] public string? SourceKey { get; init; }
}
