namespace Orleans.Lattice.Apps;

/// <summary>Which dimension of an install capability ceiling a manifest role exceeds.</summary>
public enum AppCeilingExcessKind
{
    /// <summary>The role requests operation bits outside the ceiling's allowed operations mask.</summary>
    Operations = 0,

    /// <summary>
    /// The role requests a scope outside the app's structural <c>a/{app}/</c> namespace that no
    /// operator-approved exception scope covers.
    /// </summary>
    Scope = 1,
}
