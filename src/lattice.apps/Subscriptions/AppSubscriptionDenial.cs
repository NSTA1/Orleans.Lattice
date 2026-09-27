using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps;

/// <summary>
/// A declared subscription that activation refused because it observes a scope outside the app's
/// own namespace that no operator-approved exception in the install ceiling covers.
/// </summary>
/// <param name="SubscriptionName">The refused subscription's manifest name.</param>
/// <param name="ObservedApp">The app whose tree the subscription observes; the subscribing app itself for an adopted tree.</param>
/// <param name="Scope">
/// The uncovered scope in the tenant-local vocabulary (before tenant composition), which an operator
/// can approve verbatim as an <see cref="AppCapabilityCeiling.ApprovedExceptionScopes"/> entry.
/// </param>
/// <param name="Message">A human-readable explanation naming the observed app and tree.</param>
public sealed record AppSubscriptionDenial(string SubscriptionName, AppSlug ObservedApp, LatticeScope Scope, string Message);
