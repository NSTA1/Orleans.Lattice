using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.SelfTests;

/// <summary>
/// Which failed migrations <see cref="U2Contract.IsStarvationFailure"/> attributes to #5569's
/// saturated estate: the two reasons recorded on run 45024429825, and nothing else.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class MigrationStarvationSelfTests
{
    /// <summary>The saturation failures the estate produced are attributed to #5569.</summary>
    /// <param name="reason">The recorded failure reason.</param>
    [TestCase("LatticeSaturatedException: WAL append dispatch to tree 'factory-orders/remediated/2e8f' partition 4 refused: the enclosing call exhausted its WalAdmissionSaturationCallBudget (00:00:15) waiting at this gate")]
    [TestCase("TimeoutException: The schema remediation slice timed out 5 consecutive times; the last attempt failed with: Response did not arrive on time in 00:00:30")]
    public void A_saturation_failure_is_starvation(string reason) =>
        Assert.That(U2Contract.IsStarvationFailure(Status(LatticeOperationState.Failed, reason)), Is.True);

    /// <summary>Any other failure, and a migration that did not fail, still fails the contract.</summary>
    /// <param name="state">The migration's state.</param>
    /// <param name="reason">Its failure reason.</param>
    [TestCase(LatticeOperationState.Failed, "InvalidOperationException: the registry rejected version 2")]
    [TestCase(LatticeOperationState.Failed, null)]
    [TestCase(LatticeOperationState.Cancelled, "LatticeSaturatedException: refused")]
    public void Any_other_outcome_is_not_starvation(LatticeOperationState state, string? reason) =>
        Assert.That(U2Contract.IsStarvationFailure(Status(state, reason)), Is.False);

    private static LatticeOperationStatus Status(LatticeOperationState state, string? reason) => new()
    {
        OperationId = "op",
        Kind = "SchemaMigration",
        Scope = default,
        State = state,
        Phase = "Build",
        FailureReason = reason,
    };
}
