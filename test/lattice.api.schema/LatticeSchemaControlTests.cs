using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaControl"/>: every facade operation gates
/// through the fail-closed <see cref="SchemaAccessAuthorizer"/> before touching the
/// in-process schema admin surface (mutations require
/// <see cref="LatticeOperation.SchemaAdmin"/>, reads require
/// <see cref="LatticeOperation.Read"/>), and delegates to the correct admin on an
/// allow. Denials surface as <see cref="LatticeAuthorizationDeniedException"/> and
/// never reach the admin. Driven purely with substitutes - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaControlTests
{
    private const string Tree = "orders";

    private sealed class Harness
    {
        public required ILatticeSchemaAdmin Admin { get; init; }
        public required ILatticeSchemaRemediationAdmin Remediation { get; init; }
        public required ILatticeSchemaComplianceAdmin Compliance { get; init; }
        public required ILatticeSchemaVersionAdmin? VersionAdmin { get; init; }
        public required LatticeSchemaControl Control { get; init; }
    }

    private static Harness CreateHarness(
        ILatticeAccessGate gate,
        bool withVersionAdmin = true)
    {
        var admin = Substitute.For<ILatticeSchemaAdmin>();
        var remediation = Substitute.For<ILatticeSchemaRemediationAdmin>();
        var compliance = Substitute.For<ILatticeSchemaComplianceAdmin>();
        var versionAdmin = withVersionAdmin ? Substitute.For<ILatticeSchemaVersionAdmin>() : null;

        var services = new ServiceCollection();
        if (versionAdmin is not null)
        {
            services.AddSingleton(versionAdmin);
        }

        var control = new LatticeSchemaControl(
            admin,
            remediation,
            compliance,
            new SchemaAccessAuthorizer(gate),
            Options.Create(new LatticeApiSchemaOptions()),
            services.BuildServiceProvider());

        return new Harness
        {
            Admin = admin,
            Remediation = remediation,
            Compliance = compliance,
            VersionAdmin = versionAdmin,
            Control = control,
        };
    }

    private static LatticeSchemaPolicy JsonPolicy() => new(new[] { LatticeSchemaRule.Json() });

    // ---- Delegation on allow --------------------------------------------

    [Test]
    public async Task SetPolicyAsync_allowed_delegates_to_admin_under_schema_admin_gate()
    {
        var gate = RecordingAccessGate.Allow();
        var h = CreateHarness(gate);
        var policy = JsonPolicy();

        await h.Control.SetPolicyAsync(Tree, policy);

        Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.SchemaAdmin));
        await h.Admin.Received(1).SetPolicyAsync(Tree, policy, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetPolicyAsync_allowed_delegates_to_admin_under_read_gate()
    {
        var gate = RecordingAccessGate.Allow();
        var h = CreateHarness(gate);
        h.Admin.GetPolicyAsync(Tree, Arg.Any<CancellationToken>()).Returns(JsonPolicy());

        var policy = await h.Control.GetPolicyAsync(Tree);

        Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Read));
        Assert.That(policy, Is.Not.Null);
    }

    [Test]
    public async Task ClearPolicyAsync_allowed_returns_admin_result()
    {
        var h = CreateHarness(RecordingAccessGate.Allow());
        h.Admin.ClearPolicyAsync(Tree, Arg.Any<CancellationToken>()).Returns(true);

        Assert.That(await h.Control.ClearPolicyAsync(Tree), Is.True);
    }

    [Test]
    public async Task CountDeadLettersAsync_allowed_returns_admin_result_under_read_gate()
    {
        var gate = RecordingAccessGate.Allow();
        var h = CreateHarness(gate);
        h.Admin.CountDeadLettersAsync(Tree, Arg.Any<CancellationToken>()).Returns(4);

        Assert.That(await h.Control.CountDeadLettersAsync(Tree), Is.EqualTo(4));
        Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Read));
    }

    [Test]
    public async Task ListDeadLettersAsync_allowed_streams_admin_entries()
    {
        var gate = RecordingAccessGate.Allow();
        var h = CreateHarness(gate);
        var entry = new LatticeSchemaDeadLetterEntry(
            "k1", new byte[] { 1 }, 3, "bad", LatticeSchemaDeadLetterSource.Replication, DateTimeOffset.UnixEpoch);
        h.Admin.ListDeadLettersAsync(Tree, Arg.Any<CancellationToken>()).Returns(ToAsync(entry));

        var seen = new List<LatticeSchemaDeadLetterEntry>();
        await foreach (var e in h.Control.ListDeadLettersAsync(Tree))
        {
            seen.Add(e);
        }

        Assert.That(seen, Has.Count.EqualTo(1));
        Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Read));
    }

    [Test]
    public async Task ScanComplianceAsync_allowed_delegates_to_compliance_under_read_gate()
    {
        var gate = RecordingAccessGate.Allow();
        var h = CreateHarness(gate);
        h.Compliance.ScanComplianceAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(LatticeSchemaComplianceReport.Ungoverned(Tree));

        var report = await h.Control.ScanComplianceAsync(Tree);

        Assert.That(report.TreeId, Is.EqualTo(Tree));
        Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Read));
        await h.Compliance.Received(1).ScanComplianceAsync(Tree, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RemediateAsync_allowed_delegates_to_remediation_under_schema_admin_gate()
    {
        var gate = RecordingAccessGate.Allow();
        var h = CreateHarness(gate);
        var transform = LatticeValueTransform.Passthrough();
        var policy = JsonPolicy();
        h.Remediation.RemediateAsync(Tree, transform, policy, Arg.Any<CancellationToken>())
            .Returns(LatticeSchemaRemediationReport.Idle);

        await h.Control.RemediateAsync(Tree, transform, policy);

        Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.SchemaAdmin));
        await h.Remediation.Received(1).RemediateAsync(Tree, transform, policy, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetRemediationStatusAsync_allowed_uses_read_gate()
    {
        var gate = RecordingAccessGate.Allow();
        var h = CreateHarness(gate);
        h.Remediation.GetRemediationStatusAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(LatticeSchemaRemediationReport.Idle);

        await h.Control.GetRemediationStatusAsync(Tree);

        Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Read));
    }

    // ---- Fail-closed on denial ------------------------------------------

    [Test]
    public void SetPolicyAsync_denied_throws_and_does_not_touch_admin()
    {
        var h = CreateHarness(RecordingAccessGate.Deny());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await h.Control.SetPolicyAsync(Tree, JsonPolicy()));

        h.Admin.DidNotReceive().SetPolicyAsync(
            Arg.Any<string>(), Arg.Any<LatticeSchemaPolicy>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void GetPolicyAsync_denied_throws_and_does_not_touch_admin()
    {
        var h = CreateHarness(RecordingAccessGate.Deny());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await h.Control.GetPolicyAsync(Tree));

        h.Admin.DidNotReceive().GetPolicyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ScanComplianceAsync_denied_throws_and_does_not_scan()
    {
        var h = CreateHarness(RecordingAccessGate.Deny());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await h.Control.ScanComplianceAsync(Tree));

        h.Compliance.DidNotReceive().ScanComplianceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void RemediateAsync_denied_throws_and_does_not_remediate()
    {
        var h = CreateHarness(RecordingAccessGate.Deny());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await h.Control.RemediateAsync(Tree, LatticeValueTransform.Passthrough(), JsonPolicy()));

        h.Remediation.DidNotReceive().RemediateAsync(
            Arg.Any<string>(), Arg.Any<LatticeValueTransform>(), Arg.Any<LatticeSchemaPolicy>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ListDeadLettersAsync_denied_throws_before_streaming()
    {
        var h = CreateHarness(RecordingAccessGate.Deny());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(async () =>
        {
            await foreach (var _ in h.Control.ListDeadLettersAsync(Tree))
            {
            }
        });

        h.Admin.DidNotReceive().ListDeadLettersAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // ---- Parameter guards -----------------------------------------------

    [Test]
    public void SetPolicyAsync_null_or_empty_tree_id_throws()
    {
        var h = CreateHarness(RecordingAccessGate.Allow());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await h.Control.SetPolicyAsync(null!, JsonPolicy()), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await h.Control.SetPolicyAsync("", JsonPolicy()), Throws.ArgumentException);
        });
    }

    [Test]
    public void SetPolicyAsync_null_policy_throws()
    {
        var h = CreateHarness(RecordingAccessGate.Allow());

        Assert.That(async () => await h.Control.SetPolicyAsync(Tree, null!), Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void RemediateAsync_null_target_policy_throws()
    {
        var h = CreateHarness(RecordingAccessGate.Allow());

        Assert.That(
            async () => await h.Control.RemediateAsync(Tree, LatticeValueTransform.Passthrough(), null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void ScanComplianceAsync_empty_tree_id_throws()
    {
        var h = CreateHarness(RecordingAccessGate.Allow());

        Assert.That(async () => await h.Control.ScanComplianceAsync(""), Throws.ArgumentException);
    }

    [Test]
    public void Constructor_null_dependencies_throw()
    {
        var admin = Substitute.For<ILatticeSchemaAdmin>();
        var remediation = Substitute.For<ILatticeSchemaRemediationAdmin>();
        var compliance = Substitute.For<ILatticeSchemaComplianceAdmin>();
        var authorizer = new SchemaAccessAuthorizer(RecordingAccessGate.Allow());
        var options = Options.Create(new LatticeApiSchemaOptions());
        var services = new ServiceCollection().BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeSchemaControl(null!, remediation, compliance, authorizer, options, services), Throws.ArgumentNullException);
            Assert.That(() => new LatticeSchemaControl(admin, null!, compliance, authorizer, options, services), Throws.ArgumentNullException);
            Assert.That(() => new LatticeSchemaControl(admin, remediation, null!, authorizer, options, services), Throws.ArgumentNullException);
            Assert.That(() => new LatticeSchemaControl(admin, remediation, compliance, null!, options, services), Throws.ArgumentNullException);
            Assert.That(() => new LatticeSchemaControl(admin, remediation, compliance, authorizer, null!, services), Throws.ArgumentNullException);
            Assert.That(() => new LatticeSchemaControl(admin, remediation, compliance, authorizer, options, null!), Throws.ArgumentNullException);
        });
    }

    private static async IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ToAsync(LatticeSchemaDeadLetterEntry entry)
    {
        yield return entry;
        await Task.CompletedTask;
    }
}
