namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>What kind of input the address line is completing.</summary>
internal enum AddressQueryMode
{
    /// <summary>Free text: search every visible area.</summary>
    Search = 0,

    /// <summary>The user typed <c>a/</c>: complete app slugs.</summary>
    App = 1,

    /// <summary>The user typed <c>t/</c>: complete tenants.</summary>
    Tenant = 2,

    /// <summary>The user typed <c>&gt;</c>: the command palette.</summary>
    Command = 3,

    /// <summary>The user typed a literal address beginning with <c>/</c>.</summary>
    Address = 4,
}
