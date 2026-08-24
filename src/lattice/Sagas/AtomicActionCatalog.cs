using System.Collections.Frozen;

namespace Orleans.Lattice;

/// <summary>
/// A resolved registration for a custom atomic-action handler: the
/// <see cref="Handler"/> and its <see cref="VersionTag"/> captured at registration
/// time. Returned by <see cref="IAtomicActionCatalog.TryResolve"/>.
/// </summary>
internal sealed class AtomicActionHandlerRegistration
{
    /// <summary>Initialises a registration.</summary>
    /// <param name="handler">The registered handler.</param>
    /// <param name="versionTag">The handler's version tag at registration time.</param>
    public AtomicActionHandlerRegistration(IAtomicActionHandler handler, string versionTag)
    {
        Handler = handler;
        VersionTag = versionTag;
    }

    /// <summary>The registered handler.</summary>
    public IAtomicActionHandler Handler { get; }

    /// <summary>The handler's version tag at registration time.</summary>
    public string VersionTag { get; }
}

/// <summary>
/// The registered-handler catalog for the silo: the single, narrow seam that
/// resolves a custom-step handler id to its <see cref="AtomicActionHandlerRegistration"/>.
/// It <b>is</b> the allow-list - it is built once at startup from the handlers the
/// application explicitly registered, and <see cref="TryResolve"/> returns
/// <see langword="null"/> for any id that was never registered. A saga can
/// therefore only ever invoke a pre-registered, allow-listed effect, whether the id
/// arrived from a fresh caller-built plan or a persisted saga step: resolution fails
/// closed by construction.
/// </summary>
internal interface IAtomicActionCatalog
{
    /// <summary>
    /// Resolves <paramref name="handlerId"/> to its registration, or returns
    /// <see langword="null"/> when the id is not registered (fail closed).
    /// </summary>
    /// <param name="handlerId">The handler id to resolve.</param>
    /// <returns>The registration, or <see langword="null"/> if unregistered.</returns>
    AtomicActionHandlerRegistration? TryResolve(string handlerId);
}

/// <summary>
/// The default <see cref="IAtomicActionCatalog"/>: an immutable
/// <see cref="FrozenDictionary{TKey,TValue}"/> of handler id to registration, built
/// once at silo start and never mutated, so resolution is lock-free and
/// allocation-free on the saga hot path.
/// </summary>
internal sealed class AtomicActionCatalog : IAtomicActionCatalog
{
    private readonly FrozenDictionary<string, AtomicActionHandlerRegistration> _handlers;

    /// <summary>
    /// Initialises the catalog from the registered handlers.
    /// </summary>
    /// <param name="handlers">The registered handlers, keyed by id.</param>
    public AtomicActionCatalog(IReadOnlyDictionary<string, AtomicActionHandlerRegistration> handlers)
    {
        ArgumentNullException.ThrowIfNull(handlers);
        _handlers = handlers.ToFrozenDictionary(StringComparer.Ordinal);
    }

    /// <inheritdoc />
    public AtomicActionHandlerRegistration? TryResolve(string handlerId)
    {
        ArgumentNullException.ThrowIfNull(handlerId);
        return _handlers.TryGetValue(handlerId, out var registration) ? registration : null;
    }
}

/// <summary>
/// An <see cref="IAtomicActionHandler"/> that adapts a pair of forward/compensate
/// delegates registered through
/// <see cref="AtomicActionRegistrationBuilder.AddHandler(string, string, System.Func{IAtomicActionContext, System.Threading.Tasks.Task}, System.Func{IAtomicActionContext, System.Threading.Tasks.Task})"/>.
/// </summary>
internal sealed class DelegateAtomicActionHandler : IAtomicActionHandler
{
    private readonly Func<IAtomicActionContext, Task> _forward;
    private readonly Func<IAtomicActionContext, Task> _compensate;

    /// <summary>Initialises the delegate-backed handler.</summary>
    /// <param name="handlerId">The handler id.</param>
    /// <param name="versionTag">The handler version tag.</param>
    /// <param name="forward">The forward effect.</param>
    /// <param name="compensate">The compensating effect.</param>
    public DelegateAtomicActionHandler(
        string handlerId,
        string versionTag,
        Func<IAtomicActionContext, Task> forward,
        Func<IAtomicActionContext, Task> compensate)
    {
        HandlerId = handlerId;
        VersionTag = versionTag;
        _forward = forward;
        _compensate = compensate;
    }

    /// <inheritdoc />
    public string HandlerId { get; }

    /// <inheritdoc />
    public string VersionTag { get; }

    /// <inheritdoc />
    public Task ForwardAsync(IAtomicActionContext context) => _forward(context);

    /// <inheritdoc />
    public Task CompensateAsync(IAtomicActionContext context) => _compensate(context);
}
