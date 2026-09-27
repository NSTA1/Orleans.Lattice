using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>A membership context that always resolves a fixed, non-anonymous subject.</summary>
internal sealed class FixedMembershipContext : ILatticeMembershipContext
{
    private static readonly LatticeSubject Subject = new("operator", Array.Empty<string>());

    public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) => new(Subject);

    public bool TryResolveCurrent(out LatticeSubject subject)
    {
        subject = Subject;
        return true;
    }
}
