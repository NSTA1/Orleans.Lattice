namespace Orleans.Lattice.Explorer.UI.Layout.Appearance;

/// <summary>The contrast overlay, layered over whichever material is in force.</summary>
internal enum ShellContrast
{
    /// <summary>Follow the operating system's <c>prefers-contrast</c> hint.</summary>
    System = 0,

    /// <summary>The standard palette, even when the platform asks for more.</summary>
    Standard = 1,

    /// <summary>The high-contrast overlay: text at 7:1 and boundaries at 4.5:1.</summary>
    More = 2,
}
