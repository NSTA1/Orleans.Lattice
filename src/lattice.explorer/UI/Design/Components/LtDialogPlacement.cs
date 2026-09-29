namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>Where an <see cref="LtDialog"/> sits.</summary>
public enum LtDialogPlacement
{
    /// <summary>Centred above the page: a question and its actions.</summary>
    Center = 0,

    /// <summary>
    /// A full-height sheet on the inline-start edge, such as the directory panel
    /// below the small breakpoint. On a narrow screen it takes the full width.
    /// </summary>
    Start = 1,

    /// <summary>
    /// A full-height sheet on the inline-end edge, such as a table row's detail or
    /// the header's overflow menu. On a narrow screen it takes the full width.
    /// </summary>
    End = 2,
}
