namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>
/// The appearances the story's ledger carries (#5364), as <c>theme/contrast/density</c> in
/// lower case, each part the Explorer's appearance menu choice: theme <c>system</c>,
/// <c>paper</c> or <c>board</c>; contrast <c>system</c>, <c>standard</c> or <c>more</c>;
/// density <c>comfortable</c> or <c>compact</c>. An appearance lives in the browser's storage,
/// so a journey records the one it leaves in the ledger and a later journey applies it.
/// </summary>
public static class ContosoAppearance
{
    /// <summary>olivia's appearance after A1: the Board theme, the system contrast, compact density.</summary>
    public const string OliviaAfterA1 = "board/system/compact";

    /// <summary>sam's appearance after U5, whose last step resets the view: everything back to its default.</summary>
    public const string SamAfterU5 = "system/system/comfortable";
}
