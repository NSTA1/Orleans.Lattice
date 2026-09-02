namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The keyed, per-session store of plugin access decisions that replaces the
/// Explorer's former single fat capability record.
/// <para>
/// Each plugin owns its own entries, so adding a plugin adds keys rather than
/// editing a shared type, and one plugin's decision can never overwrite
/// another's. Reads are the render-path operation and are lock-free and
/// allocation-free; every unwritten key reads as
/// <see cref="ExplorerPluginAccess.Denied"/>, so the store is fail-closed
/// before any probe has run.
/// </para>
/// <para>
/// Gating is advisory. The server remains the sole enforcement point, so an
/// entry reading allowed never removes a plugin's duty to handle a runtime
/// denial.
/// </para>
/// </summary>
public interface IExplorerPluginAccessStore
{
    /// <summary>
    /// Raised after each individual key changes value. Carries the key, so a
    /// subscriber can filter to its own plugin. Not raised when a write leaves
    /// a key's value unchanged.
    /// </summary>
    event Action<ExplorerPluginAccessChange>? Changed;

    /// <summary>
    /// Reads the decision filed under <paramref name="key"/>, or
    /// <see cref="ExplorerPluginAccess.Denied"/> when nothing is filed there.
    /// </summary>
    /// <param name="key">The key to read.</param>
    ExplorerPluginAccess Get(ExplorerPluginAccessKey key);

    /// <summary>
    /// Reads the plugin-level decision for <paramref name="pluginId"/>, or
    /// <see cref="ExplorerPluginAccess.Denied"/> when nothing is filed for it.
    /// </summary>
    /// <param name="pluginId">The plugin id to read. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="pluginId"/> is <see langword="null"/>.</exception>
    ExplorerPluginAccess Get(string pluginId);

    /// <summary>
    /// Reports whether a plugin-level decision has actually been filed for
    /// <paramref name="pluginId"/> yet.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <strong>This is a presentation signal, never an authorization one.</strong>
    /// It answers "is the answer known yet", not "is the caller allowed", and no
    /// gate or enforcement path may branch on it. The fail-closed reads above are
    /// unchanged: an unprobed key still reads
    /// <see cref="ExplorerPluginAccess.Denied"/>, and that remains the only
    /// answer any access decision is entitled to use.
    /// </para>
    /// <para>
    /// It exists because a surface that renders a decision has to tell "refused"
    /// apart from "not asked yet", and reading the fail-closed default cannot.
    /// The rail did exactly that and so opened with every area demoted, each
    /// carrying a remedy naming a permission nobody had established was missing -
    /// a confident, wrong sentence that then vanished as the probes landed.
    /// Fail-closed is right for the decision and wrong as a caption.
    /// </para>
    /// </remarks>
    /// <param name="pluginId">The plugin id to test. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="pluginId"/> is <see langword="null"/>.</exception>
    bool HasReported(string pluginId);

    /// <summary>
    /// Reads the scoped decision for <paramref name="pluginId"/> and
    /// <paramref name="scope"/>, or <see cref="ExplorerPluginAccess.Denied"/>
    /// when nothing is filed there. A scoped key does <em>not</em> inherit the
    /// plugin-level decision: an unprobed scope is denied, not admitted by its
    /// plugin's coarse gate.
    /// </summary>
    /// <param name="pluginId">The plugin id to read. Must not be <see langword="null"/>.</param>
    /// <param name="scope">The scope to read. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    ExplorerPluginAccess Get(string pluginId, string scope);

    /// <summary>
    /// Reports whether any <em>scoped</em> decision filed for
    /// <paramref name="pluginId"/> whose scope satisfies
    /// <paramref name="scopeFilter"/> currently reads
    /// <see cref="ExplorerPluginAccess.Allowed"/>.
    /// <para>
    /// This is the derivation seam for a plugin whose coarse gate is "the
    /// plugin as a whole, <em>or</em> any single scope I can still reach".
    /// Deriving that at probe time from the entries a
    /// <see cref="Clear(string)"/>, a <see cref="Reset"/>, or the next scope
    /// probe overwrites keeps such a gate self-healing; caching the answer in
    /// the plugin instead latches an admission that outlives the grant behind
    /// it.
    /// </para>
    /// <para>
    /// The plugin-level entry is never consulted, so a plugin cannot re-admit
    /// itself through its own coarse decision.
    /// </para>
    /// </summary>
    /// <param name="pluginId">The plugin id whose scoped entries to consider. Must not be <see langword="null"/>.</param>
    /// <param name="scopeFilter">
    /// Selects which of the plugin's scope names count. Prefer a cached
    /// delegate: this runs on the probe path, not the render path, but a
    /// per-call lambda still allocates.
    /// </param>
    /// <returns><see langword="true"/> when at least one matching scope reads allowed.</returns>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    bool AnyScopeAllowed(string pluginId, Func<string, bool> scopeFilter);

    /// <summary>
    /// Files <paramref name="access"/> under <paramref name="key"/>, raising
    /// <see cref="Changed"/> when that alters the stored value.
    /// </summary>
    /// <param name="key">The key to write.</param>
    /// <param name="access">The decision to file.</param>
    void Set(ExplorerPluginAccessKey key, ExplorerPluginAccess access);

    /// <summary>
    /// Files the plugin-level decision for <paramref name="pluginId"/>.
    /// </summary>
    /// <param name="pluginId">The plugin id to write. Must not be <see langword="null"/>.</param>
    /// <param name="access">The decision to file.</param>
    /// <exception cref="ArgumentNullException"><paramref name="pluginId"/> is <see langword="null"/>.</exception>
    void Set(string pluginId, ExplorerPluginAccess access);

    /// <summary>
    /// Files a scoped decision for <paramref name="pluginId"/> and
    /// <paramref name="scope"/>.
    /// </summary>
    /// <param name="pluginId">The plugin id to write. Must not be <see langword="null"/>.</param>
    /// <param name="scope">The scope to write. Must not be <see langword="null"/>.</param>
    /// <param name="access">The decision to file.</param>
    /// <exception cref="ArgumentNullException"><paramref name="pluginId"/> or <paramref name="scope"/> is <see langword="null"/>.</exception>
    void Set(string pluginId, string scope, ExplorerPluginAccess access);

    /// <summary>
    /// Drops every decision filed for <paramref name="pluginId"/> - its
    /// plugin-level entry and all of its scoped entries - raising
    /// <see cref="Changed"/> once per dropped key. Use when a plugin's
    /// preconditions change and its cached per-scope decisions must not be
    /// trusted, rather than mutating them in place.
    /// </summary>
    /// <param name="pluginId">The plugin id to clear. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="pluginId"/> is <see langword="null"/>.</exception>
    void Clear(string pluginId);

    /// <summary>
    /// Drops every decision in the store, raising <see cref="Changed"/> once
    /// per dropped key. Call on sign-out or before a full re-probe so a stale
    /// admission cannot survive an identity change.
    /// </summary>
    void Reset();

    /// <summary>
    /// A point-in-time copy of every decision currently filed. Intended for
    /// diagnostics and tests; the render path reads individual keys through
    /// <see cref="Get(ExplorerPluginAccessKey)"/> instead, because a snapshot
    /// allocates.
    /// </summary>
    IReadOnlyDictionary<ExplorerPluginAccessKey, ExplorerPluginAccess> Snapshot();
}
