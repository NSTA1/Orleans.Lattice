namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>One option of an <see cref="LtSelect"/>.</summary>
/// <param name="Value">The value submitted when the option is chosen.</param>
/// <param name="Text">The option's visible text.</param>
public sealed record LtSelectOption(string Value, string Text)
{
    /// <summary>Whether the option is shown but cannot be chosen.</summary>
    public bool Disabled { get; init; }
}
