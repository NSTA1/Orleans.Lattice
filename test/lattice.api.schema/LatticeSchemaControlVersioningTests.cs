using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Tests;

/// <summary>
/// Coverage for the facade's optional-version-admin handling. Schema versioning is
/// a separate add-on (<c>AddLatticeSchemaVersioning</c>), so
/// <see cref="LatticeSchemaControl"/> resolves <see cref="ILatticeSchemaVersionAdmin"/>
/// optionally: a version operation on a silo without versioning registered throws a
/// clear <see cref="InvalidOperationException"/> (after the gate admits it), and
/// delegates to the admin when versioning is present.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaControlVersioningTests
{
    private const string Tree = "orders";

    private static LatticeSchemaControl Create(ILatticeSchemaVersionAdmin? versionAdmin)
    {
        var services = new ServiceCollection();
        if (versionAdmin is not null)
        {
            services.AddSingleton(versionAdmin);
        }

        return new LatticeSchemaControl(
            Substitute.For<ILatticeSchemaAdmin>(),
            Substitute.For<ILatticeSchemaRemediationAdmin>(),
            Substitute.For<ILatticeSchemaComplianceAdmin>(),
            new SchemaAccessAuthorizer(RecordingAccessGate.Allow()),
            Options.Create(new LatticeApiSchemaOptions()),
            services.BuildServiceProvider(),
            new DefaultTenantContextResolver());
    }

    [Test]
    public void SetVersionConfig_without_versioning_registered_throws_invalid_operation()
    {
        var control = Create(versionAdmin: null);

        Assert.That(
            async () => await control.SetVersionConfigAsync(Tree, new LatticeSchemaVersionConfig(1, 2)),
            Throws.InvalidOperationException);
    }

    [Test]
    public void GetVersionConfig_without_versioning_registered_throws_invalid_operation()
    {
        var control = Create(versionAdmin: null);

        Assert.That(async () => await control.GetVersionConfigAsync(Tree), Throws.InvalidOperationException);
    }

    [Test]
    public void AdvanceTargetVersion_without_versioning_registered_throws_invalid_operation()
    {
        var control = Create(versionAdmin: null);

        Assert.That(async () => await control.AdvanceTargetVersionAsync(Tree, 3), Throws.InvalidOperationException);
    }

    [Test]
    public void MigrateToTargetVersion_without_versioning_registered_throws_invalid_operation()
    {
        var control = Create(versionAdmin: null);

        Assert.That(async () => await control.MigrateToTargetVersionAsync(Tree), Throws.InvalidOperationException);
    }

    [Test]
    public void ClearVersionConfig_without_versioning_registered_throws_invalid_operation()
    {
        var control = Create(versionAdmin: null);

        Assert.That(async () => await control.ClearVersionConfigAsync(Tree), Throws.InvalidOperationException);
    }

    [Test]
    public async Task SetVersionConfig_delegates_when_versioning_registered()
    {
        var admin = Substitute.For<ILatticeSchemaVersionAdmin>();
        var control = Create(admin);
        var config = new LatticeSchemaVersionConfig(1, 2);

        await control.SetVersionConfigAsync(Tree, config);

        await admin.Received(1).SetVersionConfigAsync(Tree, config, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AdvanceTargetVersion_delegates_and_returns_admin_result()
    {
        var admin = Substitute.For<ILatticeSchemaVersionAdmin>();
        var updated = new LatticeSchemaVersionConfig(1, 5);
        admin.AdvanceTargetVersionAsync(Tree, 5, Arg.Any<CancellationToken>()).Returns(updated);
        var control = Create(admin);

        var result = await control.AdvanceTargetVersionAsync(Tree, 5);

        Assert.That(result.TargetVersion, Is.EqualTo(5u));
    }

    [Test]
    public async Task AdvanceAndMigrate_delegates_when_versioning_registered()
    {
        var admin = Substitute.For<ILatticeSchemaVersionAdmin>();
        admin.AdvanceAndMigrateAsync(Tree, 4, Arg.Any<CancellationToken>())
            .Returns(LatticeSchemaRemediationReport.Idle);
        var control = Create(admin);

        await control.AdvanceAndMigrateAsync(Tree, 4);

        await admin.Received(1).AdvanceAndMigrateAsync(Tree, 4, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ClearVersionConfig_delegates_and_returns_admin_result()
    {
        var admin = Substitute.For<ILatticeSchemaVersionAdmin>();
        admin.ClearVersionConfigAsync(Tree, Arg.Any<CancellationToken>()).Returns(true);
        var control = Create(admin);

        Assert.That(await control.ClearVersionConfigAsync(Tree), Is.True);
    }

    [Test]
    public void Version_operations_denied_by_gate_fail_closed_before_touching_version_admin()
    {
        var admin = Substitute.For<ILatticeSchemaVersionAdmin>();
        var control = new LatticeSchemaControl(
            Substitute.For<ILatticeSchemaAdmin>(),
            Substitute.For<ILatticeSchemaRemediationAdmin>(),
            Substitute.For<ILatticeSchemaComplianceAdmin>(),
            new SchemaAccessAuthorizer(RecordingAccessGate.Deny()),
            Options.Create(new LatticeApiSchemaOptions()),
            new ServiceCollection().AddSingleton(admin).BuildServiceProvider(),
            new DefaultTenantContextResolver());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await control.SetVersionConfigAsync(Tree, new LatticeSchemaVersionConfig(1, 2)));

        admin.DidNotReceive().SetVersionConfigAsync(
            Arg.Any<string>(), Arg.Any<LatticeSchemaVersionConfig>(), Arg.Any<CancellationToken>());
    }
}
