namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>One sampled value a draft rule set refuses, with the first rule it fails.</summary>
/// <param name="Key">The value's key.</param>
/// <param name="RuleIndex">The zero-based position of the first rule it fails.</param>
/// <param name="Reason">That rule's failure reason, as enforcement would report it.</param>
/// <param name="Preview">The value's leading text, for showing.</param>
internal sealed record SchemaPreviewFailure(string Key, int RuleIndex, string Reason, string Preview);
