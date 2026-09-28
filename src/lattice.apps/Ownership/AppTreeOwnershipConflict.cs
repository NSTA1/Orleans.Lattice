namespace Orleans.Lattice.Apps;

/// <summary>
/// A tree an install cannot own, reported before install by
/// <see cref="IAppRegistry.GetTreeOwnershipConflictsAsync"/> and carried in the message of a
/// <see cref="AppRegistryTransitionError.TreeOwnershipConflict"/> or
/// <see cref="AppActivationFailure.TreeOwnershipConflict"/> failure.
/// </summary>
/// <param name="TreeName">The app-local tree name the manifest declares.</param>
/// <param name="Reason">Why the tree cannot be owned.</param>
/// <param name="OwningApp">The app that owns the tree, when <paramref name="Reason"/> is <see cref="AppTreeOwnershipConflictReason.OwnedByAnotherApp"/>; otherwise <c>null</c>.</param>
/// <param name="Message">A human-readable diagnostic naming the tree by its app-local name.</param>
public sealed record AppTreeOwnershipConflict(
    string TreeName,
    AppTreeOwnershipConflictReason Reason,
    AppSlug? OwningApp,
    string Message);
