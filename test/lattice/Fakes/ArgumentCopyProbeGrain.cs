using System.Runtime.CompilerServices;
using Orleans.Concurrency;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Fakes;

/// <inheritdoc cref="IArgumentCopyProbeGrain" />
internal sealed class ArgumentCopyProbeGrain : Grain, IArgumentCopyProbeGrain
{
    /// <summary>The key both arms agree to carry the probe payload under.</summary>
    internal const string ProbeKey = "probe-key";

    /// <inheritdoc />
    public Task<int> AcceptCopiedAsync(Dictionary<string, LwwValue<byte[]>> entries)
        => Task.FromResult(IdentityOf(entries));

    /// <inheritdoc />
    public Task<int> AcceptImmutableAsync([Immutable] Dictionary<string, LwwValue<byte[]>> entries)
        => Task.FromResult(IdentityOf(entries));

    /// <summary>
    /// Reference identity of the received payload array. Uses
    /// <see cref="RuntimeHelpers.GetHashCode(object)"/> rather than
    /// <c>object.GetHashCode</c> so the value reflects object identity and not
    /// the array's contents.
    /// </summary>
    private static int IdentityOf(Dictionary<string, LwwValue<byte[]>> entries)
        => RuntimeHelpers.GetHashCode(entries[ProbeKey].Value!);
}
