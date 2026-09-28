namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>What kind of news a toast carries.</summary>
internal enum LtToastTone
{
    /// <summary>A neutral notice.</summary>
    Info,

    /// <summary>Something the reader asked for has finished.</summary>
    Success,

    /// <summary>Something finished, with a caveat worth reading.</summary>
    Warning,

    /// <summary>Something the reader asked for failed.</summary>
    Danger,
}
