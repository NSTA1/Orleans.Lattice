namespace Orleans.Lattice.Api.Operations;

/// <summary>
/// Centralized Orleans serialization alias constants for the shared
/// long-running-operation contract in <c>Orleans.Lattice.Api.Abstractions</c>.
/// Every constant uses the reserved <c>oio.</c> prefix, is at most 6 characters,
/// and is unique.
/// </summary>
public static class ApiOperationTypeAliases
{
    /// <summary>
    /// The reserved alias prefix owned by the shared operation contract. Every
    /// alias constant added here must start with this value.
    /// </summary>
    public const string AliasPrefix = "oio.";

    /// <summary>Alias for <see cref="LatticeOperationScope"/>.</summary>
    public const string LatticeOperationScope = "oio.sc";

    /// <summary>Alias for <see cref="LatticeOperationHandle"/>.</summary>
    public const string LatticeOperationHandle = "oio.hd";

    /// <summary>Alias for <see cref="LatticeOperationStatus"/>.</summary>
    public const string LatticeOperationStatus = "oio.st";

    /// <summary>Alias for <see cref="LatticeOperationListRequest"/>.</summary>
    public const string LatticeOperationListRequest = "oio.lr";

    /// <summary>Alias for <see cref="LatticeOperationPage"/>.</summary>
    public const string LatticeOperationPage = "oio.pg";

    /// <summary>Alias for <see cref="LatticeOperationState"/>.</summary>
    public const string LatticeOperationState = "oio.sa";
}
