namespace Orleans.Lattice.Explorer.DesignSystem.Tokens;

/// <summary>
/// What a help disclosure is explaining, which decides how its trigger and
/// panel are presented.
/// </summary>
public enum LatticeHelpTone
{
    /// <summary>
    /// An explanation of a term or a control: a quiet question-mark trigger
    /// beside the thing it explains.
    /// </summary>
    Informational = 0,

    /// <summary>
    /// An explanation of why something is refused, and what to do about it.
    /// Rendered in the danger tone so a refusal reads as a refusal rather than
    /// as a footnote, and always paired with a remedy.
    /// </summary>
    Denial = 1,
}
