using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaRemediationAdmin"/>: parameter guards
/// (null / empty / reserved tree id, null policy) and delegation to the per-tree
/// <see cref="ILatticeSchemaRemediationGrain"/> coordinator.
/// </summary>
public class LatticeSchemaRemediationAdminTests
{
    private static (LatticeSchemaRemediationAdmin Admin, ILatticeSchemaRemediationGrain Grain) Create()
    {
        var grain = Substitute.For<ILatticeSchemaRemediationGrain>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILatticeSchemaRemediationGrain>("orders").Returns(grain);
        return (new LatticeSchemaRemediationAdmin(grainFactory), grain);
    }

    private static LatticeSchemaPolicy JsonPolicy() => new(new[] { LatticeSchemaRule.Json() });

    [Test]
    public async Task RemediateAsync_accepts_on_the_per_tree_grain_then_drives_it_one_slice_at_a_time()
    {
        var (admin, grain) = Create();
        var policy = JsonPolicy();
        var transform = LatticeValueTransform.Passthrough();
        grain.AcceptAsync(transform, policy, Arg.Any<string>())
            .Returns(LatticeSchemaRemediationReport.InFlight(LatticeSchemaRemediationPhase.DryRun, 0, "orders/remediated/x", "op"));
        grain.RunSliceAsync().Returns(
            new SchemaRemediationSlice(
                LatticeSchemaRemediationReport.InFlight(LatticeSchemaRemediationPhase.Build, 0, "orders/remediated/x", "op"), 2),
            new SchemaRemediationSlice(LatticeSchemaRemediationReport.Completed(2, "orders/remediated/x", "op"), null));

        var report = await admin.RemediateAsync("orders", transform, policy);

        Assert.That(report.Succeeded, Is.True);
        await grain.Received(1).AcceptAsync(transform, policy, Arg.Any<string>());
        await grain.Received(2).RunSliceAsync();
        await grain.DidNotReceiveWithAnyArgs().StartAsync(default, default!, default);
    }

    [Test]
    public async Task GetRemediationStatusAsync_delegates_to_the_per_tree_grain()
    {
        var (admin, grain) = Create();
        grain.GetStatusAsync().Returns(LatticeSchemaRemediationReport.Idle);

        var report = await admin.GetRemediationStatusAsync("orders");

        Assert.That(report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Idle));
        await grain.Received(1).GetStatusAsync();
    }

    [Test]
    public void RemediateAsync_null_or_empty_tree_id_throws()
    {
        var (admin, _) = Create();

        Assert.That(async () => await admin.RemediateAsync(null!, LatticeValueTransform.Passthrough(), JsonPolicy()),
            Throws.TypeOf<ArgumentNullException>());
        Assert.That(async () => await admin.RemediateAsync("", LatticeValueTransform.Passthrough(), JsonPolicy()),
            Throws.ArgumentException);
    }

    [Test]
    public void RemediateAsync_reserved_tree_id_throws()
    {
        var (admin, _) = Create();

        Assert.That(
            async () => await admin.RemediateAsync("sys-schema-policy", LatticeValueTransform.Passthrough(), JsonPolicy()),
            Throws.ArgumentException);
    }

    [Test]
    public void RemediateAsync_null_policy_throws()
    {
        var (admin, _) = Create();

        Assert.That(
            async () => await admin.RemediateAsync("orders", LatticeValueTransform.Passthrough(), null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void GetRemediationStatusAsync_null_or_empty_tree_id_throws()
    {
        var (admin, _) = Create();

        Assert.That(async () => await admin.GetRemediationStatusAsync(null!),
            Throws.TypeOf<ArgumentNullException>());
        Assert.That(async () => await admin.GetRemediationStatusAsync(""),
            Throws.ArgumentException);
    }
}
