namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// Process-wide registry mapping a view name to its <see cref="ViewRegistration"/>.
/// Populated at startup from <c>AddLatticeViews</c> registrations and at runtime
/// by <see cref="ILatticeViewFactory.Create"/>. The view maintainer grain reads
/// it to recover the source tree id and the projection instance for the view it
/// maintains, because a grain cannot receive a non-serializable projection
/// through its key or persisted state.
/// </summary>
internal interface IViewCatalog
{
    /// <summary>
    /// Registers (or replaces) the binding for <paramref name="registration"/>'s
    /// view name. Idempotent for an identical registration.
    /// </summary>
    void Register(ViewRegistration registration);

    /// <summary>
    /// Returns the registration for <paramref name="viewName"/>, or
    /// <see langword="null"/> when no view by that name has been registered.
    /// </summary>
    ViewRegistration? TryGet(string viewName);

    /// <summary>Returns a snapshot of every currently-registered view.</summary>
    IReadOnlyCollection<ViewRegistration> All();
}
