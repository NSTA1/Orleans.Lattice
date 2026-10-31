using System.Collections.Immutable;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// One host-registered runtime-view projection provider, as
/// <see cref="ILatticeTreeAdmin.ListViewProvidersAsync"/> lists it.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeViewProviderInfo)]
[Immutable]
public sealed record TreeViewProviderInfo
{
    /// <summary>The stable provider key a create names.</summary>
    [Id(0)] public required string ProviderKey { get; init; }

    /// <summary>
    /// What the provider builds, or <see langword="null"/> when the host registered it
    /// without a description.
    /// </summary>
    [Id(1)] public string? Description { get; init; }

    /// <summary>
    /// The properties of the provider's JSON payload object, in display order. Empty
    /// when the payload is free-form JSON (or, for an undescribed provider, of an
    /// undeclared shape), which the provider validates when the view is created.
    /// </summary>
    [Id(2)] public ImmutableArray<TreeViewProviderField> PayloadFields { get; init; }
}
