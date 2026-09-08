namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Probe grain used to pin the Orleans activation/deactivation hook contract
/// that the leaf's issue #2280 observation sites depend on. Its primary key
/// selects the activation behaviour, so no mutable static configuration is
/// shared between concurrently running tests.
/// </summary>
public interface IActivationFailureProbeGrain : IGrainWithStringKey
{
    /// <summary>
    /// Forces an activation. Faults when the grain's key selects a failing
    /// activation behaviour.
    /// </summary>
    Task PingAsync();

    /// <summary>
    /// Requests a graceful deactivation of this activation, so a test can
    /// demonstrate the deactivation hook is reachable at all.
    /// </summary>
    Task DeactivateSelfAsync();
}
