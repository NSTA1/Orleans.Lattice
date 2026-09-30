namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>What a failed auth-facade call means for the page that made it.</summary>
internal enum AccessFailureKind
{
    /// <summary>The caller may not administer access.</summary>
    Denied = 0,

    /// <summary>A principal id did not validate against the identity directory; shown beside its field.</summary>
    DirectoryValidation = 1,

    /// <summary>A write named an app-owned rule id; shown beside the rule id field.</summary>
    AppOwned = 2,

    /// <summary>The server rejected an argument; shown as the form's error.</summary>
    Invalid = 3,

    /// <summary>The object no longer exists.</summary>
    NotFound = 4,

    /// <summary>The facade is not served or not reachable.</summary>
    Unavailable = 5,
}
