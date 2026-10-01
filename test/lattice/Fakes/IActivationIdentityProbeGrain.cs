namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// An ordinary (non-Lattice) grain that reports which activation answered, so a
/// multi-silo test can tell one activation per id from two without relying on
/// per-type activation counts.
/// </summary>
internal interface IActivationIdentityProbeGrain : IGrainWithStringKey
{
    /// <summary>Returns a tag unique to the answering activation: its silo and a per-activation id.</summary>
    /// <returns>The activation tag.</returns>
    Task<string> GetActivationTagAsync();
}
