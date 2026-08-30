namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The operations a cross-tenant grant authorizes once it is active. Mirrors
/// the control API's grant access flags in Explorer terms.
/// </summary>
[Flags]
public enum ExplorerTenantGrantAccess
{
    /// <summary>The grant authorizes nothing.</summary>
    None = 0,

    /// <summary>The grant authorizes reads over its scope.</summary>
    Read = 1,

    /// <summary>The grant authorizes writes over its scope.</summary>
    Write = 2,

    /// <summary>The grant authorizes both reads and writes over its scope.</summary>
    ReadWrite = Read | Write,
}
