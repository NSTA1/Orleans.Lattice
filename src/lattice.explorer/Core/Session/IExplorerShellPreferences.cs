using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// <b>The Explorer shell's durable preference contract.</b> The one way the
/// shell remembers anything about you between sessions, and the one way it
/// forgets.
/// </summary>
/// <remarks>
/// <para>
/// <b>Division of labour with the URL.</b> The route
/// (<see cref="IExplorerShellRouter"/>) carries <em>where you are</em>;
/// preferences carry <em>how you like it</em> and <em>where you were last
/// time</em>. A bare <c>/</c> is the only address that restores from here - see
/// <see cref="GetRememberedRoute"/>. Any explicit URL wins, because a link
/// someone sent you must show what they saw, not what you left open.
/// </para>
/// <para>
/// <b>Everything goes through a declared key.</b> Reads and writes take an
/// <see cref="ExplorerPreferenceKey"/> registered in
/// <see cref="IExplorerPreferenceCatalog"/>, not a string. An unregistered key is
/// rejected, which is what keeps the contract enumerable - and therefore
/// resettable, and therefore explainable. Registering a key is how a feature
/// extends the contract without editing the shell.
/// </para>
/// <para>
/// <b>Scoped per user and per cluster.</b> The scope is folded into the stored
/// key, so signing in as somebody else, or pointing the Explorer at another
/// cluster, shows a clean view instead of resurrecting a view that was never
/// yours. <see cref="Changed"/> fires when that happens so components re-read.
/// </para>
/// <para>
/// <b>A remembered value is a hint, never an authority.</b> Restore through
/// <see cref="Resolve{T, TState}"/> or <see cref="RestoreAsync{T, TState}"/> so a
/// value that no longer resolves - a deleted tree, an area this identity may no
/// longer reach - falls back to a safe default and carries a sentence explaining
/// why, instead of pointing a surface at something that is not there.
/// </para>
/// </remarks>
public interface IExplorerShellPreferences
{
    /// <summary>
    /// Whether the durable store has hydrated. Until it is
    /// <see langword="true"/>, every read reports
    /// <see cref="ExplorerPreferenceFallbackReason.NotLoaded"/> and returns the
    /// caller's fallback, so a component must not persist that fallback back over
    /// the user's real choice.
    /// </summary>
    bool IsLoaded { get; }

    /// <summary>The keys this contract covers, for a "what does the Explorer remember?" affordance.</summary>
    IReadOnlyList<ExplorerPreferenceKey> Keys { get; }

    /// <summary>
    /// Raised when previously read values may no longer be correct: the scope
    /// changed (a different user or cluster), or the view was reset. Components
    /// that cached a resolution should re-read.
    /// </summary>
    event Action? Changed;

    /// <summary>
    /// Hydrates the durable store if it has not hydrated yet. Safe to await from
    /// component initialization and safe to call repeatedly.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task EnsureLoadedAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the remembered value for <paramref name="key"/> in the current
    /// scope, or <paramref name="fallback"/> when nothing usable is stored.
    /// Synchronous and cheap; safe on a render path.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="key">A registered preference key.</param>
    /// <param name="fallback">The value to use when nothing is remembered.</param>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="key"/> is not registered in the catalog.</exception>
    T GetOrDefault<T>(ExplorerPreferenceKey key, T fallback = default!);

    /// <summary>
    /// Restores <paramref name="key"/>, validating the remembered value with
    /// <paramref name="isResolvable"/> before handing it back.
    /// </summary>
    /// <remarks>
    /// The state-carrying shape exists so the predicate can be a cached static
    /// lambda rather than a closure allocated on each call; restore runs on the
    /// shell's start-up and navigation paths.
    /// </remarks>
    /// <typeparam name="T">The value type.</typeparam>
    /// <typeparam name="TState">The state the predicate needs, typically the live collection to validate against.</typeparam>
    /// <param name="key">A registered preference key.</param>
    /// <param name="fallback">The value to use when nothing usable is remembered.</param>
    /// <param name="state">Passed through to <paramref name="isResolvable"/>.</param>
    /// <param name="isResolvable">
    /// Answers whether the remembered value still means something. Return
    /// <see langword="false"/> for a tree that no longer exists or an area this
    /// identity may no longer reach.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> or <paramref name="isResolvable"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="key"/> is not registered in the catalog.</exception>
    ExplorerPreferenceResolution<T> Resolve<T, TState>(
        ExplorerPreferenceKey key,
        T fallback,
        TState state,
        Func<T, TState, bool> isResolvable);

    /// <summary>
    /// Restores <paramref name="key"/> with a closure predicate. Prefer
    /// <see cref="Resolve{T, TState}"/> where the predicate would capture.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="key">A registered preference key.</param>
    /// <param name="fallback">The value to use when nothing usable is remembered.</param>
    /// <param name="isResolvable">Answers whether the remembered value still means something.</param>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> or <paramref name="isResolvable"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="key"/> is not registered in the catalog.</exception>
    ExplorerPreferenceResolution<T> Resolve<T>(ExplorerPreferenceKey key, T fallback, Func<T, bool> isResolvable);

    /// <summary>
    /// <see cref="Resolve{T, TState}"/>, and additionally forgets a remembered
    /// value that no longer resolves so it cannot keep resurfacing on every
    /// later restore. The call most consumers want.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <typeparam name="TState">The state the predicate needs.</typeparam>
    /// <param name="key">A registered preference key.</param>
    /// <param name="fallback">The value to use when nothing usable is remembered.</param>
    /// <param name="state">Passed through to <paramref name="isResolvable"/>.</param>
    /// <param name="isResolvable">Answers whether the remembered value still means something.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> or <paramref name="isResolvable"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="key"/> is not registered in the catalog.</exception>
    Task<ExplorerPreferenceResolution<T>> RestoreAsync<T, TState>(
        ExplorerPreferenceKey key,
        T fallback,
        TState state,
        Func<T, TState, bool> isResolvable,
        CancellationToken cancellationToken = default);

    /// <summary>Remembers <paramref name="value"/> under <paramref name="key"/> in the current scope.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="key">A registered preference key.</param>
    /// <param name="value">The value to remember.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="key"/> is not registered in the catalog.</exception>
    Task SetAsync<T>(ExplorerPreferenceKey key, T value, CancellationToken cancellationToken = default);

    /// <summary>Forgets whatever is remembered under <paramref name="key"/> in the current scope.</summary>
    /// <param name="key">A registered preference key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="key"/> is not registered in the catalog.</exception>
    Task ClearAsync(ExplorerPreferenceKey key, CancellationToken cancellationToken = default);

    /// <summary>
    /// <b>The reset-view escape.</b> Forgets every registered key in the current
    /// scope and raises <see cref="Changed"/>, so the shell returns to its
    /// out-of-the-box view without touching another identity's preferences or the
    /// user's sign-in.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task ResetAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Composes the remembered shell keys into a route: where the user was last
    /// time. The answer to a bare <c>/</c>.
    /// </summary>
    /// <remarks>
    /// Returns <see cref="ExplorerRoute.Root"/> when nothing is remembered, which
    /// the caller should read as "show the default view". The route is a
    /// <em>candidate</em>: validate its parts through
    /// <see cref="Resolve{T, TState}"/> before navigating to it, because a
    /// remembered tree may since have been deleted.
    /// </remarks>
    ExplorerRoute GetRememberedRoute();

    /// <summary>
    /// Remembers <paramref name="route"/> as where the user is, writing only the
    /// keys whose values actually changed.
    /// </summary>
    /// <remarks>
    /// A bare route is ignored rather than persisted: arriving at <c>/</c> is the
    /// request to restore, so treating it as state to remember would erase the
    /// very thing being restored. So is any route offered before
    /// <see cref="IsLoaded"/>, because a comparison against an unhydrated mirror
    /// would read as "everything changed" and clear what was remembered.
    /// </remarks>
    /// <param name="route">The route to remember. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentNullException"><paramref name="route"/> is <see langword="null"/>.</exception>
    Task RememberRouteAsync(ExplorerRoute route, CancellationToken cancellationToken = default);
}
