using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Operations;
using Orleans.Runtime;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="SchemaOperationService"/>: the synchronous refusals a
/// start makes before anything is accepted, and how a terminal remediation report
/// is recorded as an operation outcome (issue #4123).
/// </summary>
[TestFixture]
public sealed class SchemaOperationServiceTests
{
    private static SchemaOperationService Create(IServiceProvider? services = null)
    {
        var grains = Substitute.For<IGrainFactory>();
        var runner = new LatticeOperationRunner(
            grains,
            Substitute.For<ILocalSiloDetails>(),
            Options.Create(new LatticeOperationOptions()),
            NullLogger<LatticeOperationRunner>.Instance);
        return new SchemaOperationService(runner, grains, services ?? Substitute.For<IServiceProvider>());
    }

    [Test]
    public void StartRemediationAsync_refuses_an_uncompilable_policy_before_accepting_anything()
    {
        var service = Create();
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Regex("(") });

        Assert.That(
            () => service.StartRemediationAsync("tenant", "op", "orders", LatticeValueTransform.Passthrough(), policy),
            Throws.ArgumentException);
    }

    [Test]
    public void StartRemediationAsync_refuses_a_null_policy_and_a_reserved_tree()
    {
        var service = Create();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => service.StartRemediationAsync("tenant", "op", "orders", LatticeValueTransform.Passthrough(), null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => service.StartRemediationAsync(
                    "tenant", "op", LatticeSchemaReservedTrees.Prefix + "policies", LatticeValueTransform.Passthrough(),
                    new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() })),
                Throws.ArgumentException);
        });
    }

    [Test]
    public void StartMigrationAsync_and_StartAdvanceAndMigrateAsync_refuse_when_versioning_is_not_registered()
    {
        var service = Create();

        Assert.Multiple(() =>
        {
            Assert.That(() => service.StartMigrationAsync("tenant", "op", "orders"),
                Throws.InvalidOperationException.With.Message.Contains("AddLatticeSchemaVersioning"));
            Assert.That(() => service.StartAdvanceAndMigrateAsync("tenant", "op", "orders", 3),
                Throws.InvalidOperationException.With.Message.Contains("AddLatticeSchemaVersioning"));
        });
    }

    [Test]
    public void ToCompletion_records_a_completed_remediation_as_succeeded()
    {
        var completion = SchemaOperationService.ToCompletion(
            LatticeSchemaRemediationReport.Completed(7, "orders/remediated/x", "op-1"));

        Assert.Multiple(() =>
        {
            Assert.That(completion.State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(completion.Result[SchemaOperationResultKeys.Outcome], Is.EqualTo(SchemaOperationResultKeys.Completed));
            Assert.That(completion.Result[SchemaOperationResultKeys.ValuesProcessed], Is.EqualTo("7"));
            Assert.That(completion.Result[SchemaOperationResultKeys.RemediationOperationId], Is.EqualTo("op-1"));
            Assert.That(completion.Result.Values, Has.None.Contains("remediated"), "the destination physical tree is never disclosed");
        });
    }

    [Test]
    public void ToCompletion_records_an_aborted_remediation_as_failed_naming_the_offending_value()
    {
        var completion = SchemaOperationService.ToCompletion(
            LatticeSchemaRemediationReport.Aborted(3, "k3", "not JSON.", [1, 2], "op-1"));

        Assert.Multiple(() =>
        {
            Assert.That(completion.State, Is.EqualTo(LatticeOperationState.Failed));
            Assert.That(completion.FailureReason, Does.Contain("'k3'").And.Contain("not JSON.").And.Contain("Nothing was cut over"));
            Assert.That(completion.Result[SchemaOperationResultKeys.Outcome], Is.EqualTo(SchemaOperationResultKeys.Aborted));
            Assert.That(completion.Result[SchemaOperationResultKeys.OffendingKey], Is.EqualTo("k3"));
            Assert.That(completion.Result[SchemaOperationResultKeys.Reason], Is.EqualTo("not JSON."));
            Assert.That(completion.Result[SchemaOperationResultKeys.ValuesProcessed], Is.EqualTo("3"));
        });
    }

    [Test]
    public void ToCompletion_records_a_cancelled_remediation_as_cancelled()
    {
        var completion = SchemaOperationService.ToCompletion(LatticeSchemaRemediationReport.Cancelled(2, "op-1"));

        Assert.Multiple(() =>
        {
            Assert.That(completion.State, Is.EqualTo(LatticeOperationState.Cancelled));
            Assert.That(completion.Result[SchemaOperationResultKeys.Outcome], Is.EqualTo(SchemaOperationResultKeys.Cancelled));
        });
    }

    [Test]
    public void ToCompletion_of_a_non_terminal_report_records_a_failure() =>
        Assert.That(
            SchemaOperationService.ToCompletion(LatticeSchemaRemediationReport.Idle).State,
            Is.EqualTo(LatticeOperationState.Failed));

    [Test]
    public void The_declared_phases_lead_with_the_dry_run_or_the_advance()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaOperationService.RemediationPhases, Is.EqualTo(new[] { "DryRun", "Build", "Cutover" }));
            Assert.That(SchemaOperationService.AdvanceAndMigratePhases,
                Is.EqualTo(new[] { "Advance", "DryRun", "Build", "Cutover" }));
        });
    }
}
