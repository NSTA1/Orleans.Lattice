using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Caps the per-silo WAL replay gate's ceiling for the whole test process when
/// <see cref="Variable"/> is set, so a run can be amplified to expose a replay
/// that waits on a permit while holding one (issue #5775). Unset, it does nothing.
/// </summary>
[SetUpFixture]
public sealed class ReplayCeilingCapSetUp
{
    /// <summary>The environment variable holding the cap, a positive permit count.</summary>
    public const string Variable = "LATTICE_TEST_REPLAY_CEILING";

    /// <summary>Applies the cap before any fixture sizes the gate.</summary>
    [OneTimeSetUp]
    public void Apply()
    {
        if (int.TryParse(Environment.GetEnvironmentVariable(Variable), out var cap) && cap > 0)
        {
            BPlusLeafGrain.ReplayCeilingCapForTest = cap;
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
