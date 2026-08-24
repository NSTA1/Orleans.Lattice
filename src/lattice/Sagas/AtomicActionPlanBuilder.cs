namespace Orleans.Lattice;

/// <summary>
/// A fluent builder for an <see cref="AtomicActionPlan"/> that lets a caller mix
/// custom handler steps and built-in tree-write steps in one ordered plan. Steps
/// run forward in the order they are added and, on a fault, compensate in the
/// reverse of that order.
/// <para>
/// Example:
/// <code>
/// var plan = new AtomicActionPlanBuilder()
///     .TreeWrite("inventory", w => w.Upsert("sku-42/onhand", qtyBytes))
///     .Step("charge-card", chargeArgs)
///     .Build();
/// </code>
/// </para>
/// </summary>
public sealed class AtomicActionPlanBuilder
{
    private readonly List<AtomicActionStep> _steps = [];

    /// <summary>
    /// Appends a custom step that runs the registered handler identified by
    /// <paramref name="handlerId"/>, passing <paramref name="args"/> to both its
    /// forward and compensating effects. The handler must be registered on the silo
    /// through <c>AddLatticeAtomicAction</c>; an unregistered id fails closed when
    /// the plan runs.
    /// </summary>
    /// <param name="handlerId">The id of a registered handler.</param>
    /// <param name="args">
    /// The opaque, size-bounded argument payload for the handler, or
    /// <see langword="null"/> for no args.
    /// </param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="System.ArgumentException"><paramref name="handlerId"/> is <see langword="null"/> or empty.</exception>
    public AtomicActionPlanBuilder Step(string handlerId, byte[]? args = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(handlerId);
        _steps.Add(new AtomicActionStep
        {
            Kind = AtomicActionStepKind.Custom,
            HandlerId = handlerId,
            ArgsPayload = args ?? [],
        });

        return this;
    }

    /// <summary>
    /// Appends a built-in tree-write step that atomically applies a batch of
    /// upserts and deletes to the Lattice tree identified by
    /// <paramref name="treeId"/>. The coordinator captures each affected key's
    /// pre-image before the write and, on a later fault, restores those pre-images -
    /// so the compensation is library-synthesized and the caller supplies no
    /// compensating effect. The forward write delegates to the tree's verified
    /// atomic-write machinery.
    /// </summary>
    /// <param name="treeId">The logical id of the target Lattice tree.</param>
    /// <param name="configure">A callback that declares the entries to write.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="System.ArgumentException"><paramref name="treeId"/> is <see langword="null"/> or empty.</exception>
    /// <exception cref="System.ArgumentNullException"><paramref name="configure"/> is <see langword="null"/>.</exception>
    public AtomicActionPlanBuilder TreeWrite(string treeId, Action<AtomicActionTreeWriteBuilder> configure)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(configure);

        var writeBuilder = new AtomicActionTreeWriteBuilder();
        configure(writeBuilder);
        _steps.Add(new AtomicActionStep
        {
            Kind = AtomicActionStepKind.TreeWrite,
            TreeId = treeId,
            Entries = writeBuilder.Build(),
        });

        return this;
    }

    /// <summary>
    /// Materializes the accumulated steps into an <see cref="AtomicActionPlan"/>.
    /// </summary>
    /// <returns>The built plan.</returns>
    public AtomicActionPlan Build() => new() { Steps = [.. _steps] };
}

/// <summary>
/// Declares the entries of a built-in tree-write step via
/// <see cref="AtomicActionPlanBuilder.TreeWrite"/>. Each declared upsert or delete
/// becomes an <see cref="AtomicActionEntry"/> applied atomically as one batch.
/// </summary>
public sealed class AtomicActionTreeWriteBuilder
{
    private readonly List<AtomicActionEntry> _entries = [];

    /// <summary>
    /// Declares an upsert of <paramref name="value"/> at <paramref name="key"/> in
    /// the target tree.
    /// </summary>
    /// <param name="key">The tree key to write.</param>
    /// <param name="value">The value bytes to write.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="System.ArgumentException"><paramref name="key"/> is <see langword="null"/> or empty.</exception>
    /// <exception cref="System.ArgumentNullException"><paramref name="value"/> is <see langword="null"/>.</exception>
    public AtomicActionTreeWriteBuilder Upsert(string key, byte[] value)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(value);
        _entries.Add(new AtomicActionEntry(key, value, Delete: false));
        return this;
    }

    /// <summary>
    /// Declares a delete (tombstone) of <paramref name="key"/> in the target tree.
    /// </summary>
    /// <param name="key">The tree key to delete.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="System.ArgumentException"><paramref name="key"/> is <see langword="null"/> or empty.</exception>
    public AtomicActionTreeWriteBuilder Delete(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _entries.Add(new AtomicActionEntry(key, [], Delete: true));
        return this;
    }

    /// <summary>Materializes the declared entries.</summary>
    /// <returns>The list of entries to write atomically.</returns>
    internal List<AtomicActionEntry> Build() => [.. _entries];
}
