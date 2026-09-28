namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// The closed set of reasons an app bridge request fails. The zero value is
/// <see cref="Denied"/>, so an uninitialised code fails closed.
/// </summary>
public enum AppBridgeFailure
{
    /// <summary>The caller's app-role grants, the consented bridge operations, or the caller's own rights refuse the request.</summary>
    Denied = 0,
    /// <summary>The app, install revision or app-local tree does not exist or no longer matches the enabled install.</summary>
    NotFound = 1,
    /// <summary>The request is malformed, such as an empty key or an out-of-range page size.</summary>
    Invalid = 2,
    /// <summary>A key, value or response exceeds the bridge's size bound.</summary>
    TooLarge = 3,
    /// <summary>The request conflicts with the tree's current state.</summary>
    Conflict = 4,
    /// <summary>The cluster could not serve the request; it may succeed if retried.</summary>
    Unavailable = 5,
}
