namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The payload of <see cref="IExplorerPluginAccessStore.Changed"/>: which key
/// changed and what it changed to.
/// <para>
/// Carrying the key means a component can ignore a change that is not its own
/// plugin's instead of re-rendering the whole shell on every probe completion.
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/>, so raising the event allocates nothing.
/// </para>
/// </summary>
/// <param name="Key">The key whose decision changed.</param>
/// <param name="Access">The decision now filed under <paramref name="Key"/>.</param>
public readonly record struct ExplorerPluginAccessChange(
    ExplorerPluginAccessKey Key,
    ExplorerPluginAccess Access);
