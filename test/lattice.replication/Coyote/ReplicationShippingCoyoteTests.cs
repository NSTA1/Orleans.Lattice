using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.Replication.Tests.Grains;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Replication.Tests.Coyote;

/// <summary>Controlled feedback, FIFO acknowledgement and saga-expiry choices over production paths.</summary>
[TestFixture]
[Category("Coyote")]
public sealed class ReplicationShippingCoyoteTests
{
    /// <summary>Exercises real shipper and receiver paths, not a second shipping state machine.</summary>
    [Test]
    public void Shipping_and_deferral_obey_controlled_production_opportunities() =>
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new ShippingModel(false));

    /// <summary>Proves the explorer reaches the terminal timeout arm.</summary>
    [Test]
    public void Exploration_reaches_terminal_deferral()
    {
        var result = CoyoteModelHarness.Explore(new ShippingModel(true), seed: CoyoteModelHarness.GuardSeed);
        Assert.That(result.BugsFound, Is.GreaterThan(0));
        Assert.That(result.BugReports, Has.All.Contains("PROBE: terminal deferral"));
    }

    private sealed class ShippingModel(bool probe) : ICoyoteModel
    {
        public void Run(ICoyoteRuntime runtime)
        {
            var shipper = new ReplicationShipperGrainTests();
            var receiver = new DeadLetterTrackingReplicationApplierTests();
            var prepare = runtime.RandomBoolean();
            var abort = runtime.RandomBoolean();
            shipper.Pipeline_stops_launching_after_receiver_feedback(runtime.RandomBoolean()).GetAwaiter().GetResult();
            shipper.Pipeline_preserves_pause_when_a_later_inflight_ack_is_healthy().GetAwaiter().GetResult();
            shipper.Released_terminal_waits_for_next_pump_after_mixed_window_feedback().GetAwaiter().GetResult();
            shipper.Pipeline_enforces_window_and_Fifo_cursor_across_out_of_order_completion(
                runtime.RandomBoolean()).GetAwaiter().GetResult();
            shipper.Retry_without_jitter_preserves_cursor_then_success_resets_backoff(
                runtime.RandomBoolean() ? 1 : 2).GetAwaiter().GetResult();
            var terminalExpired = false;
            receiver.Failing_saga_retry_expires_to_poison_then_quarantine_without_reseed_loop(
                prepare, abort, observed => terminalExpired = observed).GetAwaiter().GetResult();
            Specification.Assert(!probe || !terminalExpired, "PROBE: terminal deferral");
        }
    }
}
