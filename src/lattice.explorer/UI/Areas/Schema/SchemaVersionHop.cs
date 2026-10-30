namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>One registered migration hop, with what it changes.</summary>
/// <param name="FromVersion">The version it upgrades from.</param>
/// <param name="ToVersion">The version it upgrades to.</param>
/// <param name="Steps">What it does to a value, as sentences; empty for host code, which the cluster does not describe.</param>
/// <param name="TransformId">The host transform it runs, when it runs installed code rather than a declarative transform.</param>
/// <param name="Installed">Whether the host advertises <paramref name="TransformId"/>; always <see langword="true"/> for a declarative hop.</param>
internal sealed record SchemaVersionHop(uint FromVersion, uint ToVersion, IReadOnlyList<string> Steps, string? TransformId, bool Installed);
