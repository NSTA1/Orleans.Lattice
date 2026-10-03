using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// The circuit's schema-operation tracker, driven directly rather than through a
/// page: what it does once a start has been accepted, how it follows the
/// cluster's status, and - the part no page can show - what it deliberately does
/// <em>not</em> do when the circuit ends underneath an operation that is still in
/// the cluster's hands.
/// </summary>
/// <remarks>
/// <para>
/// Every start here hands <see cref="SchemaOperations.Start"/> a delegate that
/// completes synchronously, and the scripted facade answers a status read from a
/// completed task, so the whole start-accept-follow sequence runs inline on the
/// calling thread. That is what makes these assertions deterministic without a
/// poll barrier: when <c>Start</c> returns, the tracker has already settled.
/// </para>
/// <para>
/// One arm of <c>OnFollowerChanged</c> is deliberately not driven here. Its guard
/// declines when the tree's operation has gone or has not yet been accepted, and
/// the type's own invariants make that unreachable in sequence: a follower exists
/// only while its operation is active, a new start is refused while the previous
/// operation is active, and every read that settles the operation also settles the
/// poller in the same call. It guards a concurrent interleaving, not a reachable
/// ordering, so driving it would need a fabricated race rather than a scenario.
/// </para>
/// </remarks>
[TestFixture]
public sealed class SchemaOperationsTests
{
    private const string Tree = "orders";
    private const string Summary = "Migrate every value to version 4";

    private ServiceProvider _services = null!;
    private FakeSchemaControl _schema = null!;
    private ManualTimeProvider _time = null!;
    private SchemaOperations _operations = null!;

    [SetUp]
    public void SetUp()
    {
        _schema = new FakeSchemaControl();
        var services = new ServiceCollection();
        services.AddKeyedSingleton<ILatticeSchemaOperations>(ShellFacades.Key, _schema);
        _services = services.BuildServiceProvider();
        _time = new ManualTimeProvider();
        _operations = new SchemaOperations(_time, new SchemaFacades(_services));
    }

    [TearDown]
    public void TearDown()
    {
        _operations.Dispose();
        _services.Dispose();
    }

    [Test]
    public void The_tracker_rejects_missing_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new SchemaOperations(null!, new SchemaFacades(_services)), Throws.ArgumentNullException);
            Assert.That(() => new SchemaOperations(_time, null!), Throws.ArgumentNullException);
            Assert.That(() => _operations.Start(string.Empty, SchemaOperationKind.Migrate, Summary, Accept("op-1")), Throws.ArgumentException);
            Assert.That(() => _operations.Start(Tree, SchemaOperationKind.Migrate, "  ", Accept("op-1")), Throws.ArgumentException);
            Assert.That(() => _operations.Start(Tree, SchemaOperationKind.Migrate, Summary, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_second_start_is_refused_while_the_tree_s_operation_is_still_running()
    {
        Running("op-1");
        _operations.Start(Tree, SchemaOperationKind.Migrate, Summary, Accept("op-1"));

        Assert.Multiple(() =>
        {
            Assert.That(
                () => _operations.Start(Tree, SchemaOperationKind.Remediate, Summary, Accept("op-2")),
                Throws.InvalidOperationException.With.Message.Contains("still running"));
            Assert.That(_operations.Find(Tree)!.OperationId, Is.EqualTo("op-1"), "the running operation is kept");
        });
    }

    [Test]
    public void A_settled_operation_is_replaced_by_the_next_start_on_the_same_tree()
    {
        // The first start is refused by the cluster, so it settles without ever
        // being accepted - which is what frees the tree for a second attempt.
        _operations.Start(Tree, SchemaOperationKind.Migrate, Summary, Refuse());
        Assert.That(_operations.Find(Tree)!.IsActive, Is.False, "a refused start settles at once");

        Running("op-2");
        var retry = _operations.Start(Tree, SchemaOperationKind.Remediate, "Remediate every value", Accept("op-2"));

        Assert.Multiple(() =>
        {
            Assert.That(retry.Kind, Is.EqualTo(SchemaOperationKind.Remediate));
            Assert.That(_operations.Find(Tree), Is.SameAs(_operations.Find(Tree)));
            Assert.That(_operations.Find(Tree)!.OperationId, Is.EqualTo("op-2"), "the retry replaces the settled operation");
            Assert.That(_operations.Find(Tree)!.Stage, Is.EqualTo(SchemaOperationStage.Running));
        });
    }

    [Test]
    public void A_start_the_cluster_refuses_is_reported_as_a_failure()
    {
        var changed = new List<string>();
        _operations.Changed += changed.Add;

        _operations.Start(Tree, SchemaOperationKind.AdvanceAndMigrate, Summary, Refuse());

        var operation = _operations.Find(Tree)!;
        Assert.Multiple(() =>
        {
            Assert.That(operation.Stage, Is.EqualTo(SchemaOperationStage.Failed));
            Assert.That(operation.Failure, Does.Contain("advance and migrate this tree"), "the failure names what was being attempted");
            Assert.That(operation.FinishedAt, Is.EqualTo(_time.GetUtcNow()));
            Assert.That(changed, Is.EqualTo(new[] { Tree, Tree }), "the start and its outcome are both announced");
        });
    }

    [Test]
    public void A_circuit_that_ends_while_a_start_is_in_flight_is_not_reported_as_a_failure()
    {
        // Leaving first makes the start's cancellation synchronous, so the whole
        // abandon path has run by the time Start returns.
        _operations.Dispose();

        _operations.Start(Tree, SchemaOperationKind.Migrate, Summary, Cancel());

        var operation = _operations.Find(Tree)!;
        Assert.Multiple(() =>
        {
            Assert.That(operation.Stage, Is.EqualTo(SchemaOperationStage.Starting), "the cluster keeps the operation; the circuit simply stopped watching");
            Assert.That(operation.Failure, Is.Null);
            Assert.That(operation.FinishedAt, Is.Null);
            Assert.That(_schema.OperationStatusReads, Is.Zero, "nothing is read once the circuit has gone");
        });
    }

    [Test]
    public void A_start_cancelled_while_the_circuit_is_live_is_reported_as_a_failure()
    {
        _operations.Start(Tree, SchemaOperationKind.Migrate, Summary, Cancel());

        var operation = _operations.Find(Tree)!;
        Assert.Multiple(() =>
        {
            Assert.That(operation.Stage, Is.EqualTo(SchemaOperationStage.Failed), "a cancellation the circuit did not cause is a real outcome");
            Assert.That(operation.Failure, Is.Not.Null);
        });
    }

    [Test]
    public void An_accepted_operation_without_an_id_is_not_followed()
    {
        _operations.Start(
            Tree,
            SchemaOperationKind.Migrate,
            Summary,
            _ => Task.FromResult(new LatticeOperationHandle
            {
                OperationId = null!,
                Kind = SchemaOperationKinds.Migration,
                Scope = Scope,
            }));

        Assert.Multiple(() =>
        {
            Assert.That(_operations.Find(Tree)!.Stage, Is.EqualTo(SchemaOperationStage.Running));
            Assert.That(_operations.Find(Tree)!.OperationId, Is.Null);
            Assert.That(_operations.IsFollowing(Tree), Is.False);
            Assert.That(_schema.OperationStatusReads, Is.Zero, "there is no id to poll with");
        });
    }

    [Test]
    public void The_tree_s_previous_follower_is_released_when_its_next_operation_starts()
    {
        Terminal("op-1", LatticeOperationState.Succeeded);
        _operations.Start(Tree, SchemaOperationKind.Migrate, Summary, Accept("op-1"));
        Assert.That(_operations.Find(Tree)!.Stage, Is.EqualTo(SchemaOperationStage.Completed));
        Assert.That(_operations.IsFollowing(Tree), Is.False, "the first operation's follow ended with its terminal read");

        Running("op-2");
        _operations.Start(Tree, SchemaOperationKind.Remediate, "Remediate every value", Accept("op-2"));

        Assert.Multiple(() =>
        {
            Assert.That(_operations.Find(Tree)!.OperationId, Is.EqualTo("op-2"));

            // The tree's follower is now the second operation's. Had the first
            // one been left in place - it is released here rather than at the
            // circuit's end - the tree would still read as not followed.
            Assert.That(_operations.IsFollowing(Tree), Is.True);
            Assert.That(_schema.OperationStatusReads, Is.EqualTo(2), "one read each");
        });
    }

    [Test]
    public void A_circuit_that_ends_while_the_status_is_being_read_is_not_reported_as_a_failure()
    {
        _operations.Dispose();
        Running("op-1");
        _schema.OperationStatusFault = new OperationCanceledException("the circuit ended");

        _operations.Start(Tree, SchemaOperationKind.Migrate, Summary, Accept("op-1"));

        var operation = _operations.Find(Tree)!;
        Assert.Multiple(() =>
        {
            Assert.That(_schema.OperationStatusReads, Is.EqualTo(1), "the follow was attempted");
            Assert.That(operation.Stage, Is.EqualTo(SchemaOperationStage.Running), "the operation is still the cluster's");
            Assert.That(operation.Failure, Is.Null);
        });
    }

    [Test]
    public void An_operation_the_cluster_no_longer_shows_is_reported_as_no_longer_visible()
    {
        // No status is registered for the id, so the cluster answers "no such
        // operation" - which reads the same as an empty poll until the follower
        // reports it as not found.
        _operations.Start(Tree, SchemaOperationKind.Migrate, Summary, Accept("op-missing"));

        var operation = _operations.Find(Tree)!;
        Assert.Multiple(() =>
        {
            Assert.That(operation.Stage, Is.EqualTo(SchemaOperationStage.Failed));
            Assert.That(operation.Failure, Is.EqualTo("The operation is no longer visible."));
            Assert.That(operation.FinishedAt, Is.EqualTo(_time.GetUtcNow()));
            Assert.That(operation.Status, Is.Null);
        });
    }

    [Test]
    public void A_succeeded_cluster_state_becomes_the_completed_stage() =>
        AssertTerminalStage(LatticeOperationState.Succeeded, SchemaOperationStage.Completed);

    [Test]
    public void A_cancelled_cluster_state_becomes_the_cancelled_stage() =>
        AssertTerminalStage(LatticeOperationState.Cancelled, SchemaOperationStage.Cancelled);

    [Test]
    public void A_failed_cluster_state_becomes_the_failed_stage() =>
        AssertTerminalStage(LatticeOperationState.Failed, SchemaOperationStage.Failed);

    private void AssertTerminalStage(LatticeOperationState state, SchemaOperationStage stage)
    {
        Terminal("op-1", state);

        _operations.Start(Tree, SchemaOperationKind.Migrate, Summary, Accept("op-1"));

        var operation = _operations.Find(Tree)!;
        Assert.Multiple(() =>
        {
            Assert.That(operation.Stage, Is.EqualTo(stage));
            Assert.That(operation.Status, Is.Not.Null);
            Assert.That(operation.FinishedAt, Is.EqualTo(operation.Status!.FinishedAtUtc));
            Assert.That(
                operation.Failure,
                state == LatticeOperationState.Failed ? Is.EqualTo("the cluster refused it") : Is.Null,
                "only a failure carries the cluster's reason forward");
            Assert.That(_operations.IsFollowing(Tree), Is.False, "a terminal first read ends the follow");
        });
    }

    [Test]
    public void A_failed_operation_that_stopped_at_an_offending_value_is_an_abort()
    {
        Terminal("op-1", LatticeOperationState.Failed, aborted: true);

        _operations.Start(Tree, SchemaOperationKind.Remediate, "Remediate every value", Accept("op-1"));

        Assert.That(_operations.Find(Tree)!.Stage, Is.EqualTo(SchemaOperationStage.Aborted));
    }

    [Test]
    public void A_still_running_cluster_state_keeps_the_operation_followed()
    {
        Running("op-1");

        _operations.Start(Tree, SchemaOperationKind.Migrate, Summary, Accept("op-1"));

        Assert.Multiple(() =>
        {
            Assert.That(_operations.Find(Tree)!.Stage, Is.EqualTo(SchemaOperationStage.Running));
            Assert.That(_operations.IsFollowing(Tree), Is.True);
            Assert.That(_operations.IsFollowing("another-tree"), Is.False);
        });
    }

    [Test]
    public async Task Only_a_settled_operation_is_dismissed()
    {
        Running("op-1");
        _operations.Start(Tree, SchemaOperationKind.Migrate, Summary, Accept("op-1"));

        _operations.Dismiss(Tree);
        Assert.Multiple(() =>
        {
            Assert.That(_operations.Find(Tree), Is.Not.Null, "a running operation is kept");
            Assert.That(_operations.Find("never-started"), Is.Null);
        });

        // The follow re-reads on the circuit's clock, so the terminal status is
        // picked up by advancing past the cadence rather than by starting
        // anything. Both barriers are needed: the first waits for the next read
        // to be scheduled, the second for the read the clock released to land.
        await TestPoll.UntilAsync(() => _time.PendingTimerCount > 0, "the follow to schedule its next read");
        Terminal("op-1", LatticeOperationState.Succeeded);
        _time.Advance(ClusterStatusPollerInterval);
        await TestPoll.UntilAsync(
            () => _operations.Find(Tree)!.Stage == SchemaOperationStage.Completed,
            "the follow to pick the terminal status up");

        Assert.That(_schema.OperationStatusReads, Is.EqualTo(2), "the first read and the one the clock released");

        _operations.Dismiss(Tree);
        _operations.Dismiss(Tree);
        Assert.That(_operations.Find(Tree), Is.Null, "a settled operation is forgotten, and dismissing it again does nothing");
    }

    private static readonly TimeSpan ClusterStatusPollerInterval = TimeSpan.FromSeconds(2);

    private static LatticeOperationScope Scope => new() { TenantId = "default", TreeIds = [Tree] };

    private static Func<CancellationToken, Task<LatticeOperationHandle>> Accept(string operationId) =>
        _ => Task.FromResult(new LatticeOperationHandle
        {
            OperationId = operationId,
            Kind = SchemaOperationKinds.Migration,
            Scope = Scope,
            Created = true,
        });

    private static Func<CancellationToken, Task<LatticeOperationHandle>> Refuse() =>
        _ => throw new InvalidOperationException("the cluster refused the start");

    private static Func<CancellationToken, Task<LatticeOperationHandle>> Cancel() =>
        token => throw new OperationCanceledException(token);

    private void Running(string operationId) =>
        _schema.OperationStatuses[operationId] =
            FakeSchemaControl.RunningOperation(operationId, SchemaOperationKinds.Migration, Tree);

    private void Terminal(string operationId, LatticeOperationState state, bool aborted = false)
    {
        var started = FakeSchemaControl.RunningOperation(operationId, SchemaOperationKinds.Migration, Tree);
        _schema.OperationStatuses[operationId] = started with
        {
            State = state,
            FinishedAtUtc = started.StartedAtUtc.AddMinutes(1),
            FailureReason = state == LatticeOperationState.Failed ? "the cluster refused it" : null,
            Result = aborted
                ? new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    [SchemaOperationResultKeys.Outcome] = SchemaOperationResultKeys.Aborted,
                }
                : new Dictionary<string, string>(StringComparer.Ordinal),
        };
    }
}
