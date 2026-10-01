namespace Orleans.Lattice.Tests.Fakes;

/// <inheritdoc cref="IActivationIdentityProbeGrain" />
internal sealed class ActivationIdentityProbeGrain : Grain, IActivationIdentityProbeGrain
{
    private readonly string _tag = Guid.NewGuid().ToString("N");

    /// <inheritdoc />
    public Task<string> GetActivationTagAsync() => Task.FromResult($"{RuntimeIdentity}/{_tag}");
}
