namespace Orleans.Lattice.Views;

/// <summary>
/// Process-wide registry mapping a view name to its <see cref="ViewRegistration"/>.
/// Populated at startup from <c>AddLatticeViews</c> registrations and at runtime
/// by
/// <see cref="ILatticeViewFactory.CreateAsync(ILattice,string,LatticeViewDefinition,CancellationToken)"/>.
/// The view maintainer grain reads it to recover the source tree id and the projection instance for the view it
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

    /// <summary>
    /// Removes the binding for <paramref name="viewName"/>, so a subsequent
    /// maintainer activation finds no registration and stays dormant. Idempotent:
    /// a no-op when no view by that name is registered. Used by view deletion.
    /// </summary>
    void Remove(string viewName);

    /// <summary>
    /// Replaces the binding for <paramref name="expected"/>'s view name with
    /// <paramref name="replacement"/> (or removes it when
    /// <paramref name="replacement"/> is <see langword="null"/>), but only while
    /// the current binding is still <paramref name="expected"/>. Returns
    /// <see langword="false"/> and changes nothing when a concurrent
    /// registration has already replaced or removed it, so a maintainer
    /// correcting a stale entry can never clobber a newer one.
    /// </summary>
    /// <remarks>
    /// The default implementation is a non-atomic check-then-act for simple
    /// catalogs; <see cref="ViewCatalog"/> overrides it with an atomic
    /// compare-and-swap.
    /// </remarks>
    bool TryReplace(ViewRegistration expected, ViewRegistration? replacement)
    {
        ArgumentNullException.ThrowIfNull(expected);
        if (!ReferenceEquals(TryGet(expected.ViewName), expected))
        {
            return false;
        }

        if (replacement is null)
        {
            Remove(expected.ViewName);
        }
        else
        {
            Register(replacement);
        }

        return true;
    }

    /// <summary>
    /// Marks <paramref name="registration"/> as registered in this catalog ahead
    /// of its durable runtime-registry record, which the synchronous factory path
    /// persists in the background. While marked, an absent or different durable
    /// record is not evidence that the entry is stale, so a maintainer activating
    /// in that window must leave the entry alone.
    /// </summary>
    void MarkDurablyPending(ViewRegistration registration)
    {
    }

    /// <summary>
    /// Clears the mark set by <see cref="MarkDurablyPending"/> once the durable
    /// record for <paramref name="registration"/> has been persisted.
    /// </summary>
    void ClearDurablyPending(ViewRegistration registration)
    {
    }

    /// <summary>
    /// Records that the background persist for <paramref name="registration"/>
    /// failed, moving its mark from
    /// <see cref="ViewCatalogDurability.PersistInFlight"/> to
    /// <see cref="ViewCatalogDurability.PersistFailed"/>. Has no effect once the
    /// entry has been replaced, removed, or its mark cleared.
    /// </summary>
    void MarkDurablePersistFailed(ViewRegistration registration)
    {
    }

    /// <summary>
    /// The durability mark of <paramref name="registration"/> (by reference):
    /// <see cref="ViewCatalogDurability.Settled"/> when it is unmarked. Replacing
    /// or removing an entry discards its mark.
    /// </summary>
    ViewCatalogDurability GetDurability(ViewRegistration registration) => ViewCatalogDurability.Settled;

    /// <summary>Returns a snapshot of every currently-registered view.</summary>
    IReadOnlyCollection<ViewRegistration> All();
}
