namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>What an <see cref="LtButton"/> does inside a form.</summary>
public enum LtButtonType
{
    /// <summary>An ordinary button: it never submits a form.</summary>
    Button,

    /// <summary>A submit button: it submits its form, including by implicit submission with Enter.</summary>
    Submit,
}
