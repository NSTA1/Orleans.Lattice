using System.Collections.Frozen;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The canonical vocabulary of operations an app UI frame may request over the host bridge.
/// A manifest's <c>ui.bridge</c> section may name only these operations, and every other layer
/// (contracts, enforcement, the in-frame API) pins itself to this class rather than defining
/// its own list. The bridge deliberately has no lifecycle operations: an app UI can never
/// install, re-consent, rebind or uninstall anything.
/// </summary>
public static class AppUiBridgeOperations
{
    /// <summary>Reads the frame's launch context: the app slug, installed version, locale and theme.</summary>
    public const string ContextRead = "context.read";

    /// <summary>Reads the signed-in user's display name; consented separately because it is personal data.</summary>
    public const string ContextUser = "context.user";

    /// <summary>Reads keys and ranges from the app's own declared trees.</summary>
    public const string DataRead = "data.read";

    /// <summary>Writes keys to the app's own declared trees.</summary>
    public const string DataWrite = "data.write";

    /// <summary>Deletes keys from the app's own declared trees.</summary>
    public const string DataDelete = "data.delete";

    /// <summary>Synchronises the frame's in-app route with the host's address line.</summary>
    public const string NavSync = "nav.sync";

    /// <summary>Raises a host-rendered, text-only notification.</summary>
    public const string UiNotify = "ui.notify";

    /// <summary>Every known operation, compared ordinally.</summary>
    public static IReadOnlySet<string> All { get; } = new[]
    {
        ContextRead, ContextUser, DataRead, DataWrite, DataDelete, NavSync, UiNotify,
    }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>Returns whether <paramref name="operation"/> is a known bridge operation; null is never known.</summary>
    public static bool IsKnown(string? operation) => operation is not null && All.Contains(operation);

    /// <summary>
    /// Returns whether <paramref name="operation"/> is one of the tree-scoped <c>data.*</c> operations,
    /// the only operations that may name specific declared trees.
    /// </summary>
    public static bool IsDataOperation(string? operation) => operation is DataRead or DataWrite or DataDelete;
}
