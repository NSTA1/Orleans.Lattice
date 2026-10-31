namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>One version of a family in the version map.</summary>
/// <param name="Version">The version number.</param>
/// <param name="Name">The registered descriptor's name; <see langword="null"/> when no descriptor names it.</param>
/// <param name="Reach">How a value stamped with it reads at the current target.</param>
/// <param name="Route">The versions a value passes through on its way to the target, starting with this one; empty unless <see cref="Reach"/> is <see cref="SchemaVersionReach.Upgraded"/>.</param>
internal sealed record SchemaVersionRow(uint Version, string? Name, SchemaVersionReach Reach, IReadOnlyList<uint> Route);
