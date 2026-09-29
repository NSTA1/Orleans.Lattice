namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// One existing value an <see cref="ILtSuggestionSource"/> offers a combobox:
/// the value itself, set in Cascadia Mono, and an optional short description.
/// </summary>
/// <param name="Value">The value the field takes when the suggestion is chosen, such as a tree id.</param>
/// <param name="Detail">
/// A short description shown beside the value, such as a principal's display name
/// or "App tree". It is rendered as text, never as markup.
/// </param>
public sealed record LtSuggestion(string Value, string? Detail = null);
