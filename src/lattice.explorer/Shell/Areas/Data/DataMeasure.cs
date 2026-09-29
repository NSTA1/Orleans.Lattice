namespace Orleans.Lattice.Explorer.Shell.Areas.Data;

/// <summary>One row of the Metrics tab's measures figure.</summary>
/// <param name="Name">The measure.</param>
/// <param name="Value">Its formatted value.</param>
internal sealed record DataMeasure(string Name, string Value);
