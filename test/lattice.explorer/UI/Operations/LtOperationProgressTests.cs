using Bunit;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Explorer.Tests.UI.Design;
using Orleans.Lattice.Explorer.UI.Operations;

namespace Orleans.Lattice.Explorer.Tests.UI.Operations;

/// <summary>
/// The shared operation progress component (#4122): a running operation draws its
/// state, step and an LtProgress bar over the cluster's own units - determinate
/// only when the phase total is known - and a stopped one says where it stopped
/// and why. It refuses to render without a status or a label.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtOperationProgressTests : ShellDesignTestContext
{
    [Test]
    public void A_running_phase_with_a_total_draws_a_determinate_bar_over_its_units()
    {
        var status = OperationTestStatus.Of(LatticeOperationState.Running) with
        {
            PhaseIndex = 1,
            PhaseCount = 3,
            CompletedUnits = 3,
            TotalUnits = 8,
            UnitName = "shards",
        };

        var cut = Render<LtOperationProgress>(p => p.Add(x => x.Status, status).Add(x => x.Label, "Backup progress"));

        var bar = cut.Find("[role=progressbar]");
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-operation-progress").GetAttribute("data-lt-operation-state"), Is.EqualTo("Running"));
            Assert.That(cut.Find(".lt-operation-progress__state").TextContent, Does.Contain("Running"));
            Assert.That(cut.Find(".lt-operation-progress__step").TextContent, Is.EqualTo("Step 2 of 3"));
            Assert.That(bar.GetAttribute("aria-label"), Is.EqualTo("Backup progress"));
            Assert.That(bar.GetAttribute("aria-valuenow"), Is.EqualTo("37"));
            Assert.That(cut.Find(".lt-progress").GetAttribute("data-lt-progress"), Is.Not.EqualTo("indeterminate"));
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Capturing shards"));
            Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("3 of 8 shards"));
            Assert.That(cut.FindAll(".lt-operation-progress__stopped"), Is.Empty);
        });
    }

    [Test]
    public void A_running_phase_without_a_total_draws_an_indeterminate_bar_with_the_count_alone()
    {
        var status = OperationTestStatus.Of(LatticeOperationState.Running) with { CompletedUnits = 1200, UnitName = "entries" };

        var cut = Render<LtOperationProgress>(p => p.Add(x => x.Status, status).Add(x => x.Label, "Restore progress"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-progress").GetAttribute("data-lt-progress"), Is.EqualTo("indeterminate"));
            Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("1,200 entries so far"));
            Assert.That(cut.FindAll(".lt-progress__figure"), Is.Empty, "no invented percentage");
            Assert.That(cut.FindAll(".lt-operation-progress__step"), Is.Empty);
        });
    }

    [Test]
    public void A_cancel_request_reads_cancelling_while_the_operation_still_runs()
    {
        var status = OperationTestStatus.Of(LatticeOperationState.Running) with { CancelRequested = true };

        var cut = Render<LtOperationProgress>(p => p.Add(x => x.Status, status).Add(x => x.Label, "Backup progress"));

        Assert.That(cut.Find(".lt-operation-progress__state").TextContent, Does.Contain("Cancelling"));
    }

    [Test]
    public void A_failed_operation_says_where_it_stopped_and_why()
    {
        var status = OperationTestStatus.Of(LatticeOperationState.Failed, "RestoringShards") with
        {
            PhaseIndex = 1,
            PhaseCount = 2,
            CompletedUnits = 2,
            TotalUnits = 5,
            UnitName = "shards",
            FailureReason = "The artifact store refused the read.",
        };

        var cut = Render<LtOperationProgress>(p => p.Add(x => x.Status, status).Add(x => x.Label, "Restore progress"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=progressbar]"), Is.Empty);
            Assert.That(cut.FindAll(".lt-operation-progress__step"), Is.Empty, "a stopped operation is on no step");
            Assert.That(cut.Find(".lt-operation-progress__state").TextContent, Does.Contain("Failed"));
            Assert.That(cut.Find(".lt-operation-progress__stopped").TextContent, Is.EqualTo("Stopped during Restoring shards, after 2 of 5 shards."));
            Assert.That(cut.Find(".lt-operation-progress__reason").TextContent, Is.EqualTo("The artifact store refused the read."));
        });
    }

    [Test]
    public void A_cancelled_operation_without_units_says_the_phase_alone()
    {
        var status = OperationTestStatus.Of(LatticeOperationState.Cancelled, "Validating");

        var cut = Render<LtOperationProgress>(p => p.Add(x => x.Status, status).Add(x => x.Label, "Backup progress"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-operation-progress__stopped").TextContent, Is.EqualTo("Stopped during Validating."));
            Assert.That(cut.FindAll(".lt-operation-progress__reason"), Is.Empty);
        });
    }

    [Test]
    public void A_succeeded_operation_draws_its_state_and_nothing_stopped()
    {
        var cut = Render<LtOperationProgress>(p => p
            .Add(x => x.Status, OperationTestStatus.Of(LatticeOperationState.Succeeded))
            .Add(x => x.Label, "Backup progress"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-operation-progress__state").TextContent, Does.Contain("Succeeded"));
            Assert.That(cut.FindAll("[role=progressbar]"), Is.Empty);
            Assert.That(cut.FindAll(".lt-operation-progress__stopped"), Is.Empty);
        });
    }

    [Test]
    public void A_phase_mapper_names_the_phase()
    {
        var cut = Render<LtOperationProgress>(p => p
            .Add(x => x.Status, OperationTestStatus.Of(LatticeOperationState.Running))
            .Add(x => x.Label, "Backup progress")
            .Add(x => x.PhaseName, phase => phase == "CapturingShards" ? "Copying the shards" : phase));

        Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Copying the shards"));
    }

    [Test]
    public void It_refuses_to_render_without_a_status_or_a_label()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => Render<LtOperationProgress>(p => p.Add(x => x.Label, "Backup progress")),
                Throws.InvalidOperationException.With.Message.Contains("Status"));
            Assert.That(
                () => Render<LtOperationProgress>(p => p.Add(x => x.Status, OperationTestStatus.Of(LatticeOperationState.Running)).Add(x => x.Label, " ")),
                Throws.InvalidOperationException.With.Message.Contains("Label"));
        });
    }
}
