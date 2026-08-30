namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// A scripted <see cref="ILatticeMembershipContext"/> resolving a fixed subject,
/// either on the warm synchronous path or only through the asynchronous
/// directory-reading one, so both branches of subject resolution are exercised.
/// </summary>
internal sealed class StubMembershipContext(LatticeSubject subject, bool resolvesSynchronously = true)
    : ILatticeMembershipContext
{
    /// <summary>How many times the asynchronous path was taken.</summary>
    public int AsyncResolutions { get; private set; }

    /// <summary>Whether the asynchronous path ran under a system-origin scope.</summary>
    public bool ResolvedUnderSystemOrigin { get; private set; }

    /// <inheritdoc />
    public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
    {
        AsyncResolutions++;
        ResolvedUnderSystemOrigin = LatticeSystemOrigin.IsActive;
        return new ValueTask<LatticeSubject>(subject);
    }

    /// <inheritdoc />
    public bool TryResolveCurrent(out LatticeSubject resolved)
    {
        if (!resolvesSynchronously)
        {
            resolved = default;
            return false;
        }

        resolved = subject;
        return true;
    }
}
