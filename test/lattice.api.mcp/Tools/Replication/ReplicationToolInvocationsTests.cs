using Grpc.Core;
using NSubstitute;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="ReplicationToolInvocations"/>, the pure adapter
/// layer between the replication MCP tools and the
/// <see cref="ILatticeReplicationControl"/> facade. Proves each tool delegates to
/// the facade and shapes the result DTO, that the merge-mode name is parsed and
/// validated before the facade is touched, and that the fail-closed denial the
/// facade gate raises surfaces unchanged (the MCP layer adds none). All
/// deterministic against a substituted facade - no cluster, no ordering-by-timing.
/// </summary>
[TestFixture]
public sealed class ReplicationToolInvocationsTests
{
    [Test]
    public async Task Get_config_projects_the_permission_scoped_report()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.GetReplicationConfigAsync(Arg.Any<CancellationToken>())
            .Returns(new ReplicationConfigReport(new[]
            {
                new ReplicationTreeConfigEntry("orders", enabled: true, LatticeMergeMode.OrSet, ambiguous: false),
                new ReplicationTreeConfigEntry("inventory", enabled: false, mode: null, ambiguous: true),
            }));

        var config = await ReplicationToolInvocations.GetReplicationConfigAsync(control, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(config.Trees, Has.Count.EqualTo(2));
            Assert.That(config.Trees[0].TreeId, Is.EqualTo("orders"));
            Assert.That(config.Trees[0].Enabled, Is.True);
            Assert.That(config.Trees[0].Mode, Is.EqualTo(nameof(LatticeMergeMode.OrSet)));
            Assert.That(config.Trees[0].Ambiguous, Is.False);
            Assert.That(config.Trees[1].TreeId, Is.EqualTo("inventory"));
            Assert.That(config.Trees[1].Enabled, Is.False);
            Assert.That(config.Trees[1].Mode, Is.Null, "An ambiguous-mode tree reports a null mode.");
            Assert.That(config.Trees[1].Ambiguous, Is.True);
        });
    }

    [Test]
    public async Task Get_config_of_an_empty_estate_projects_no_trees()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.GetReplicationConfigAsync(Arg.Any<CancellationToken>())
            .Returns(ReplicationConfigReport.Empty);

        var config = await ReplicationToolInvocations.GetReplicationConfigAsync(control, CancellationToken.None);

        Assert.That(config.Trees, Is.Empty);
    }

    [Test]
    public async Task Enable_parses_the_mode_and_shapes_the_result()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.EnableReplicationAsync("orders", LatticeMergeMode.OrSet, "cluster-b", Arg.Any<CancellationToken>())
            .Returns(new ReplicationEnableResult("orders", LatticeMergeMode.OrSet, alreadyEnabled: false, bootstrapRequested: true));

        var result = await ReplicationToolInvocations.EnableReplicationAsync(
            control, "orders", "orset", "cluster-b", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.Mode, Is.EqualTo(nameof(LatticeMergeMode.OrSet)));
            Assert.That(result.AlreadyEnabled, Is.False);
            Assert.That(result.BootstrapRequested, Is.True);
        });
    }

    [Test]
    public async Task Enable_with_no_bootstrap_source_passes_null_to_the_facade()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.EnableReplicationAsync("orders", LatticeMergeMode.LwwRegister, null, Arg.Any<CancellationToken>())
            .Returns(new ReplicationEnableResult("orders", LatticeMergeMode.LwwRegister, alreadyEnabled: true, bootstrapRequested: false));

        var result = await ReplicationToolInvocations.EnableReplicationAsync(
            control, "orders", "LwwRegister", bootstrapSourceClusterId: "", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.AlreadyEnabled, Is.True);
            Assert.That(result.BootstrapRequested, Is.False);
        });
        await control.Received(1).EnableReplicationAsync(
            "orders", LatticeMergeMode.LwwRegister, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public void Enable_with_a_missing_mode_is_rejected_before_the_facade()
    {
        var control = Substitute.For<ILatticeReplicationControl>();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await ReplicationToolInvocations.EnableReplicationAsync(
                    control, "orders", mode: "", bootstrapSourceClusterId: null, CancellationToken.None),
                Throws.ArgumentException);
            Assert.That(
                async () => await ReplicationToolInvocations.EnableReplicationAsync(
                    control, "orders", mode: "Nonsense", bootstrapSourceClusterId: null, CancellationToken.None),
                Throws.ArgumentException);
        });

        control.DidNotReceiveWithAnyArgs().EnableReplicationAsync(
            default!, default, default, default);
    }

    [Test]
    public void Enable_propagates_an_in_silo_mode_change_rejection_unchanged_for_the_seam_to_translate()
    {
        // The adapter is now pure: it references only always-loaded types and does
        // not name LatticeReplicationModeChangeRejectedException (a satellite type
        // whose presence in a catch clause is exactly the JIT trap of issue #1352).
        // The in-silo domain rejection therefore propagates unchanged; the shared
        // CredentialStampingTool seam translates it into an actionable McpException.
        const string message =
            "Replication for tree 'orders' is already enabled under LwwRegister and its merge mode "
            + "cannot be changed in place; disable then re-enable it under OrSet.";
        var control = Substitute.For<ILatticeReplicationControl>();
        control.EnableReplicationAsync("orders", LatticeMergeMode.OrSet, null, Arg.Any<CancellationToken>())
            .Returns<Task<ReplicationEnableResult>>(_ => throw new LatticeReplicationModeChangeRejectedException(
                message, "orders", LatticeMergeMode.OrSet, LatticeMergeMode.LwwRegister, currentModeAmbiguous: false));

        var ex = Assert.ThrowsAsync<LatticeReplicationModeChangeRejectedException>(
            async () => await ReplicationToolInvocations.EnableReplicationAsync(
                control, "orders", "OrSet", null, CancellationToken.None));
        Assert.That(ex!.Message, Is.EqualTo(message),
            "the actionable guidance is preserved for the seam to surface");
    }

    [Test]
    public void Enable_propagates_a_remote_failed_precondition_unchanged_for_the_seam_to_translate()
    {
        // Remote topology: the gRPC binding maps the rejection to a
        // FailedPrecondition RpcException whose detail is the actionable message.
        // The pure adapter propagates it; the shared seam surfaces the detail
        // through an McpException (see McpToolFaultTranslatorTests).
        const string detail =
            "Replication for tree 'mcp-test-2' is already enabled; its merge mode cannot be changed "
            + "in place - disable then re-enable it.";
        var control = Substitute.For<ILatticeReplicationControl>();
        control.EnableReplicationAsync("mcp-test-2", LatticeMergeMode.OrSet, null, Arg.Any<CancellationToken>())
            .Returns<Task<ReplicationEnableResult>>(_ => throw new RpcException(
                new Status(StatusCode.FailedPrecondition, detail)));

        var ex = Assert.ThrowsAsync<RpcException>(
            async () => await ReplicationToolInvocations.EnableReplicationAsync(
                control, "mcp-test-2", "OrSet", null, CancellationToken.None));
        Assert.That(ex!.Status.Detail, Is.EqualTo(detail));
    }

    [Test]
    public async Task Disable_shapes_the_result()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.DisableReplicationAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new ReplicationDisableResult("orders", alreadyDisabled: true));

        var result = await ReplicationToolInvocations.DisableReplicationAsync(control, "orders", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.AlreadyDisabled, Is.True);
        });
    }

    [Test]
    public void Unauthorized_caller_is_denied_fail_closed_on_every_operation()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.GetReplicationConfigAsync(Arg.Any<CancellationToken>())
            .Returns<Task<ReplicationConfigReport>>(_ => throw new LatticeAuthorizationDeniedException());
        control.EnableReplicationAsync(
                Arg.Any<string>(), Arg.Any<LatticeMergeMode>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns<Task<ReplicationEnableResult>>(_ => throw new LatticeAuthorizationDeniedException());
        control.DisableReplicationAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<ReplicationDisableResult>>(_ => throw new LatticeAuthorizationDeniedException());

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await ReplicationToolInvocations.GetReplicationConfigAsync(control, CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await ReplicationToolInvocations.EnableReplicationAsync(
                    control, "orders", "OrSet", null, CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await ReplicationToolInvocations.DisableReplicationAsync(control, "orders", CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        });
    }

    [Test]
    public void Null_control_is_rejected_on_every_invocation()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await ReplicationToolInvocations.GetReplicationConfigAsync(null!, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await ReplicationToolInvocations.EnableReplicationAsync(
                    null!, "orders", "OrSet", null, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await ReplicationToolInvocations.DisableReplicationAsync(null!, "orders", CancellationToken.None),
                Throws.ArgumentNullException);
        });
    }
}
