namespace Orleans.Lattice.Explorer.Tests.Shell.Design;

/// <summary>One rule of a stylesheet.</summary>
/// <param name="AtRule">The enclosing at-rule, normalised, or empty at the top level.</param>
/// <param name="Selector">The selector list, normalised.</param>
/// <param name="Body">The text between the rule's braces.</param>
internal sealed record CssRule(string AtRule, string Selector, string Body);
