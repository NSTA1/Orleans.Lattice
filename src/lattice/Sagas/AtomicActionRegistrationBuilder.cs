namespace Orleans.Lattice;

/// <summary>
/// The registration surface for custom atomic-action handlers, supplied to
/// <see cref="LatticeAtomicActionServiceCollectionExtensions.AddLatticeAtomicAction"/>
/// at silo start. Each registered handler becomes allow-listed: only a handler id
/// registered here can ever be invoked by a saga step. Registration is the only way
/// a handler enters the catalog, which is what makes handler resolution fail closed.
/// </summary>
public sealed class AtomicActionRegistrationBuilder
{
    private readonly Dictionary<string, AtomicActionHandlerRegistration> _handlers = new(StringComparer.Ordinal);

    /// <summary>
    /// The prefix reserved for library built-in handler ids (for example the
    /// tree-write step). Application handler ids must not start with it.
    /// </summary>
    internal const string ReservedIdPrefix = "ol.";

    internal IReadOnlyDictionary<string, AtomicActionHandlerRegistration> Handlers => _handlers;

    /// <summary>
    /// Registers a custom handler from a forward/compensate delegate pair under
    /// <paramref name="handlerId"/> with the given <paramref name="versionTag"/>.
    /// The version tag is stamped into a saga's steps when it starts and re-checked
    /// on resume; bump it whenever the effect semantics change in a way that is
    /// unsafe to replay against an in-flight saga.
    /// </summary>
    /// <param name="handlerId">The stable id the handler is resolved under.</param>
    /// <param name="versionTag">A non-empty version tag for the handler's contract.</param>
    /// <param name="forward">The forward effect.</param>
    /// <param name="compensate">The compensating effect (must fully, idempotently undo the forward effect).</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="System.ArgumentException">
    /// <paramref name="handlerId"/> or <paramref name="versionTag"/> is null/empty,
    /// <paramref name="handlerId"/> uses the reserved <c>ol.</c> prefix, or the id
    /// is already registered.
    /// </exception>
    /// <exception cref="System.ArgumentNullException">
    /// <paramref name="forward"/> or <paramref name="compensate"/> is <see langword="null"/>.
    /// </exception>
    public AtomicActionRegistrationBuilder AddHandler(
        string handlerId,
        string versionTag,
        Func<IAtomicActionContext, Task> forward,
        Func<IAtomicActionContext, Task> compensate)
    {
        ArgumentException.ThrowIfNullOrEmpty(handlerId);
        ArgumentException.ThrowIfNullOrEmpty(versionTag);
        ArgumentNullException.ThrowIfNull(forward);
        ArgumentNullException.ThrowIfNull(compensate);

        return AddHandler(new DelegateAtomicActionHandler(handlerId, versionTag, forward, compensate));
    }

    /// <summary>
    /// Registers a custom handler instance. Its
    /// <see cref="IAtomicActionHandler.HandlerId"/> is the id it resolves under and
    /// its <see cref="IAtomicActionHandler.VersionTag"/> its contract version.
    /// </summary>
    /// <param name="handler">The handler to register.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="System.ArgumentNullException"><paramref name="handler"/> is <see langword="null"/>.</exception>
    /// <exception cref="System.ArgumentException">
    /// The handler's id or version tag is null/empty, the id uses the reserved
    /// <c>ol.</c> prefix, or the id is already registered.
    /// </exception>
    public AtomicActionRegistrationBuilder AddHandler(IAtomicActionHandler handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentException.ThrowIfNullOrEmpty(handler.HandlerId);
        ArgumentException.ThrowIfNullOrEmpty(handler.VersionTag);

        if (handler.HandlerId.StartsWith(ReservedIdPrefix, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Handler id '{handler.HandlerId}' uses the reserved '{ReservedIdPrefix}' prefix, which is reserved for library built-in handlers.",
                nameof(handler));
        }

        if (!_handlers.TryAdd(handler.HandlerId, new AtomicActionHandlerRegistration(handler, handler.VersionTag)))
        {
            throw new ArgumentException(
                $"A handler with id '{handler.HandlerId}' is already registered.",
                nameof(handler));
        }

        return this;
    }
}
