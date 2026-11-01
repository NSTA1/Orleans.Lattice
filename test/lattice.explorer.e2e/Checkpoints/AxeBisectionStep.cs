namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>One level of a slow axe rule's DOM bisection (#5565): the subtree the rule spent the most time in.</summary>
/// <param name="Selector">A readable CSS path from <c>body</c> to the subtree.</param>
/// <param name="Duration">How long the rule took on this subtree alone, or <see langword="null"/> when it was the only child holding targets and was not timed separately.</param>
/// <param name="Elements">How many elements the subtree holds, itself included.</param>
/// <param name="Targets">How many of them could be pointer targets (links, buttons, form controls, widget roles, tabindex).</param>
internal sealed record AxeBisectionStep(string Selector, TimeSpan? Duration, int Elements, int Targets);
