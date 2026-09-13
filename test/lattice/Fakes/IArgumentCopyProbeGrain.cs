using Orleans.Concurrency;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Probe grain that reports the runtime identity of a payload array it received
/// through a grain call, so a test can tell whether Orleans deep-copied the
/// argument on the same-silo path or handed the caller's own array through.
/// <para>
/// The two methods differ only in the <see cref="ImmutableAttribute"/> on the
/// parameter and carry the same parameter shape as
/// <c>IBPlusLeafGrain.MergeEntriesAsync</c>, so the unmarked method is a
/// control: it must report a DIFFERENT identity (a copy was made), which is
/// what proves the probe is able to detect a copy at all. Without that arm a
/// green result on the marked method could equally mean the probe never
/// observed anything.
/// </para>
/// </summary>
internal interface IArgumentCopyProbeGrain : IGrainWithStringKey
{
    /// <summary>
    /// Accepts the batch WITHOUT an immutability marker, so Orleans performs
    /// its ordinary same-silo deep copy.
    /// </summary>
    /// <returns>The runtime identity hash of the received payload array.</returns>
    Task<int> AcceptCopiedAsync(Dictionary<string, LwwValue<byte[]>> entries);

    /// <summary>
    /// Accepts the batch WITH the immutability marker under test, which
    /// suppresses the same-silo deep copy.
    /// </summary>
    /// <returns>The runtime identity hash of the received payload array.</returns>
    Task<int> AcceptImmutableAsync([Immutable] Dictionary<string, LwwValue<byte[]>> entries);
}
