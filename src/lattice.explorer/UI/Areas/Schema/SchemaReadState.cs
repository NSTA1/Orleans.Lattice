namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>How one per-tree schema read ended.</summary>
internal enum SchemaReadState
{
    /// <summary>The read answered; a <see langword="null"/> value means "none set".</summary>
    Read = 0,

    /// <summary>The caller may not read it.</summary>
    Denied = 1,

    /// <summary>The cluster does not serve it, such as versioning when the versioning add-on is not registered.</summary>
    Unavailable = 2,

    /// <summary>The read failed for another reason.</summary>
    Failed = 3,
}
