using System.ComponentModel;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Well-known constants used across Lattice internals.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public static class LatticeConstants
{
    /// <summary>
    /// Prefix for system-internal tree IDs. Trees whose ID starts with this
    /// prefix are excluded from registry self-registration to avoid circular
    /// bootstrap (the registry tree itself uses this prefix).
    /// </summary>
    public const string SystemTreePrefix = "_lattice_";

    /// <summary>
    /// The tree ID of the internal registry tree that stores tree metadata
    /// (existence and per-tree <see cref="LatticeOptions"/> overrides).
    /// Each key is a user tree ID; each value is the serialized
    /// <see cref="TreeRegistryEntry"/>.
    /// </summary>
    public const string RegistryTreeId = "_lattice_trees";

    /// <summary>
    /// <see cref="RequestContext"/> key used by <see cref="LatticeCallContextFilter"/>
    /// to stamp grain-to-grain calls as internal.
    /// </summary>
    internal const string InternalCallTokenKey = "ol.internal";

    /// <summary>
    /// Expected value for <see cref="InternalCallTokenKey"/>.
    /// </summary>
    internal const string InternalCallTokenValue = "1";
}
