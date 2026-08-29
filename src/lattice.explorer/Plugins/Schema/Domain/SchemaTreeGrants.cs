using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Schema.Domain;

/// <summary>
/// The per-tree, per-action grey-out for one governed tree, read straight from
/// the keyed access store through scoped keys.
/// <para>
/// The plugin's per-tree probe files one decision per <see cref="SchemaCapability"/>
/// under <c>{pluginId}/{treeId}/{action}</c>; this type is the read side. It
/// binds the scope strings once when a tree is selected, so the render path
/// costs one dictionary lookup per control and allocates nothing - the store
/// stays the single source of truth rather than the panel caching a snapshot
/// that can drift from it.
/// </para>
/// <para>
/// Fail-closed by construction: an unprobed scope reads
/// <see cref="ExplorerPluginAccess.Denied"/>, and a scoped key never inherits
/// the plugin-level decision, so a coarse "the endpoint is reachable" admission
/// can never open a per-action control on its own.
/// </para>
/// </summary>
public sealed class SchemaTreeGrants
{
    private static readonly SchemaCapability[] AllCapabilities = Enum.GetValues<SchemaCapability>();

    private readonly IExplorerPluginAccessStore? _access;
    private readonly ExplorerPluginAccessKey[]? _keys;

    private SchemaTreeGrants()
    {
    }

    private SchemaTreeGrants(IExplorerPluginAccessStore access, string treeId)
    {
        _access = access;
        TreeId = treeId;

        var keys = new ExplorerPluginAccessKey[AllCapabilities.Length];
        for (var i = 0; i < AllCapabilities.Length; i++)
        {
            keys[i] = KeyFor(treeId, AllCapabilities[i]);
        }

        _keys = keys;
    }

    /// <summary>
    /// The grants for "no tree selected": every action denied. Also the value
    /// the panel holds before its first probe, so nothing is interactive until a
    /// tree has actually been probed.
    /// </summary>
    public static SchemaTreeGrants None { get; } = new();

    /// <summary>
    /// Every declared capability, in declaration order. The probe writes one
    /// decision per member, so adding a capability adds a key rather than
    /// widening a shared record.
    /// </summary>
    public static IReadOnlyList<SchemaCapability> Capabilities => AllCapabilities;

    /// <summary>
    /// The tree these grants were probed for, or <see langword="null"/> for
    /// <see cref="None"/>.
    /// </summary>
    public string? TreeId { get; }

    /// <summary>
    /// Binds the read side of the scoped decisions for <paramref name="treeId"/>.
    /// Does not probe; the caller files the decisions first.
    /// </summary>
    /// <param name="access">The keyed access store to read through. Must not be <see langword="null"/>.</param>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <exception cref="ArgumentNullException"><paramref name="access"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <see langword="null"/> or empty.</exception>
    public static SchemaTreeGrants For(IExplorerPluginAccessStore access, string treeId)
    {
        ArgumentNullException.ThrowIfNull(access);
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return new SchemaTreeGrants(access, treeId);
    }

    /// <summary>
    /// The scoped key one action on one tree is filed under.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/>.</param>
    /// <param name="capability">The action the key names.</param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <see langword="null"/>.</exception>
    public static ExplorerPluginAccessKey KeyFor(string treeId, SchemaCapability capability) =>
        new(SchemaPluginKeys.PluginId, SchemaPluginScopes.For(treeId, capability));

    /// <summary>
    /// Whether <paramref name="capability"/> is currently permitted on this
    /// tree. Always <see langword="false"/> for <see cref="None"/>.
    /// </summary>
    /// <param name="capability">The action to test.</param>
    public bool IsAllowed(SchemaCapability capability)
    {
        if (_access is null || _keys is null)
        {
            return false;
        }

        var index = (int)capability;
        return (uint)index < (uint)_keys.Length && _access.Get(_keys[index]).IsAllowed;
    }

    /// <summary>
    /// Files the decision for one capability through this tree's already-bound
    /// key, so a probe spends the scope strings once rather than rebuilding them
    /// to write and again to read. A no-op on <see cref="None"/>, which owns no
    /// tree to file against.
    /// </summary>
    /// <param name="capability">The action the decision applies to.</param>
    /// <param name="permitted">Whether the caller may perform it.</param>
    internal void Publish(SchemaCapability capability, bool permitted)
    {
        if (_access is null || _keys is null)
        {
            return;
        }

        var index = (int)capability;
        if ((uint)index < (uint)_keys.Length)
        {
            // The two results are cached statics, so filing a decision allocates
            // nothing beyond the store's own entry.
            _access.Set(_keys[index], permitted ? ExplorerPluginAccess.Allowed : ExplorerPluginAccess.Denied);
        }
    }
}
