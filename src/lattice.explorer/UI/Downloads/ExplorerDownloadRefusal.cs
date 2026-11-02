namespace Orleans.Lattice.Explorer.UI.Downloads;

/// <summary>
/// Whether a download withheld anything the caller asked for because their access
/// refused it, as its trailer reports. It is a classification, never a count or a
/// list of keys: either would disclose data the caller may not read.
/// </summary>
internal enum ExplorerDownloadRefusal
{
    /// <summary>Nothing was withheld: the caller's access admitted everything asked for.</summary>
    None = 0,

    /// <summary>The caller's access withheld some of what was asked for.</summary>
    Some = 1,

    /// <summary>The caller's access withheld everything asked for.</summary>
    All = 2,

    /// <summary>The download could not establish what was withheld, so a reader must not assume nothing was.</summary>
    Unknown = 3,
}
