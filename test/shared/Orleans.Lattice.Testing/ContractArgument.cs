namespace Orleans.Lattice.Testing;

/// <summary>
/// Sentinel values shared between <see cref="PublicApiSizeContractTestsBase"/>
/// and its subclasses when building the argument array for a reflected call.
/// </summary>
public static class ContractArgument
{
    /// <summary>
    /// Returned from
    /// <see cref="PublicApiSizeContractTestsBase.ResolveArgumentAsync"/> to tell
    /// the base to substitute the parameter's own default value (its declared
    /// optional default if any, otherwise the runtime default for its type:
    /// <see langword="null"/> for reference types, the zero value for value
    /// types such as <see cref="System.Threading.CancellationToken"/>). A
    /// subclass only needs to return a real value for the parameters that must
    /// be meaningful for the call to reach its size-sensitive allocation (for
    /// example a live cursor id or an existing key).
    /// </summary>
    public static readonly object UseDefault = new();
}
