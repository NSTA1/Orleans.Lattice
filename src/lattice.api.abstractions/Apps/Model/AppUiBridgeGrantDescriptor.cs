namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// One requested or consented app UI bridge grant: an operation, optionally restricted to
/// a single app-local tree.
/// </summary>
/// <remarks>
/// A bridge request or consent is the set of these pairs. A null <see cref="Tree"/> covers
/// every tree the app declares; a data operation limited to several trees appears as one
/// grant per tree. That keeps per-operation tree scopes, so a read on two trees and a
/// write on one of them stay distinct.
/// </remarks>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppUiBridgeGrantDescriptor), Immutable]
public sealed record AppUiBridgeGrantDescriptor
{
    /// <summary>The bridge operation, a member of the app engine's bridge operation vocabulary, such as data.read.</summary>
    [Id(0)] public required string Operation { get; init; }
    /// <summary>The app-local tree the grant is restricted to, or null for every declared tree.</summary>
    [Id(1)] public string? Tree { get; init; }
}
