namespace Orleans.Lattice.Tests.Formal;

/// <summary>One assignment in a cfg's <c>CONSTANTS</c> block.</summary>
/// <param name="Name">The name assigned or overridden.</param>
/// <param name="IsOverride"><see langword="true"/> for <c>Name &lt;- Other</c>; <see langword="false"/> for <c>Name = value</c>.</param>
/// <param name="Value">The value, or for an override the name of the replacing definition.</param>
public sealed record CfgAssignment(string Name, bool IsOverride, string Value);
