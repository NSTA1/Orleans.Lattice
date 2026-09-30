namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>One row of the Metrics tab's measures figure.</summary>
/// <param name="Name">The measure.</param>
/// <param name="Value">Its formatted value.</param>
internal sealed record DataMeasure(string Name, string Value);
