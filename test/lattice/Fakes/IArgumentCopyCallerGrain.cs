using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// In-silo caller for <see cref="IArgumentCopyProbeGrain"/>.
/// <para>
/// This indirection is load-bearing rather than decorative. A call made from
/// the test client travels the SERIALIZATION path, which necessarily produces a
/// fresh array on the far side whether or not any copier ran, so a client-side
/// probe reports "not aliased" in both arms and can never distinguish a
/// suppressed copy from a performed one. Only a caller that is itself a grain
/// on the same silo exercises the same-silo deep-copy path this test is about.
/// </para>
/// </summary>
internal interface IArgumentCopyCallerGrain : IGrainWithStringKey
{
    /// <summary>
    /// Calls the arm WITHOUT the immutability marker.
    /// </summary>
    /// <returns>
    /// <c>true</c> when the callee received this grain's own array. Expected to
    /// be <c>false</c>: this is the control arm that proves a copy is otherwise
    /// performed and that the probe can see it.
    /// </returns>
    Task<bool> CopiedArmAliasesCallerPayloadAsync();

    /// <summary>
    /// Calls the arm WITH the immutability marker under test.
    /// </summary>
    /// <returns>
    /// <c>true</c> when the callee received this grain's own array, i.e. the
    /// same-silo deep copy was suppressed.
    /// </returns>
    Task<bool> ImmutableArmAliasesCallerPayloadAsync();
}
