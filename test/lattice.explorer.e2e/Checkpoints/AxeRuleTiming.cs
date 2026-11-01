namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>How long one axe rule took when run alone (#5565).</summary>
/// <param name="RuleId">The axe rule id.</param>
/// <param name="Duration">How long its run took.</param>
/// <param name="Error">Why axe refused or failed the run, or <see langword="null"/>.</param>
internal sealed record AxeRuleTiming(string RuleId, TimeSpan Duration, string? Error);
