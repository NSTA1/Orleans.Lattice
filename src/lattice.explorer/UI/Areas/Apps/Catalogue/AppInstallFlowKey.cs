namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>The identity of one staged install flow in its circuit.</summary>
/// <param name="Tenant">The tenant the flow installs into, or <see langword="null"/> when tenancy is off.</param>
/// <param name="SourceKey">The source the app is installed from.</param>
/// <param name="Slug">The app slug.</param>
/// <param name="Version">The requested version, or <see langword="null"/> for the source's newest.</param>
internal sealed record AppInstallFlowKey(string? Tenant, string SourceKey, string Slug, string? Version);
