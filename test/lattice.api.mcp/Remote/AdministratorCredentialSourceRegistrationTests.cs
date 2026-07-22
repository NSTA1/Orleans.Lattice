using Azure.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for the administrator-credential seam wiring: the static default
/// source reads the configured <see cref="LatticeApiMcpRemoteOptions.AdministratorCredential"/>,
/// the options validator enforces credential / scope / skew, and
/// <c>AddLatticeMcpManagedIdentityAdministrator</c> replaces the default source
/// with the managed-identity one regardless of registration order.
/// </summary>
[TestFixture]
public sealed class AdministratorCredentialSourceRegistrationTests
{
    [Test]
    public void Static_source_returns_the_configured_administrator_credential()
    {
        var admin = new LatticeCredential("admin-token");
        var source = new StaticAdministratorCredentialSource(
            RemoteTestSupport.OptionsMonitor(o => o.AdministratorCredential = admin));

        Assert.That(source.Resolve(), Is.EqualTo(admin));
    }

    [Test]
    public void Static_source_returns_null_when_unset()
    {
        var source = new StaticAdministratorCredentialSource(RemoteTestSupport.OptionsMonitor(_ => { }));

        Assert.That(source.Resolve(), Is.Null);
    }

    [Test]
    public void Static_source_rejects_null_options()
        => Assert.Throws<ArgumentNullException>(() => new StaticAdministratorCredentialSource(null!));

    [Test]
    public void Validator_passes_with_credential_scope_and_non_negative_skew()
    {
        var result = Validate(new LatticeApiMcpManagedIdentityAdministratorOptions
        {
            Credential = Credential(),
            Scope = "api://silo/.default",
            RefreshSkew = TimeSpan.FromMinutes(5),
        });

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validator_fails_without_a_credential()
    {
        var result = Validate(new LatticeApiMcpManagedIdentityAdministratorOptions { Scope = "api://silo/.default" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeApiMcpManagedIdentityAdministratorOptions.Credential)));
        });
    }

    [Test]
    public void Validator_fails_with_an_empty_scope()
    {
        var result = Validate(new LatticeApiMcpManagedIdentityAdministratorOptions { Credential = Credential(), Scope = "  " });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeApiMcpManagedIdentityAdministratorOptions.Scope)));
        });
    }

    [Test]
    public void Validator_fails_with_a_negative_skew()
    {
        var result = Validate(new LatticeApiMcpManagedIdentityAdministratorOptions
        {
            Credential = Credential(),
            Scope = "api://silo/.default",
            RefreshSkew = TimeSpan.FromSeconds(-1),
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeApiMcpManagedIdentityAdministratorOptions.RefreshSkew)));
        });
    }

    [Test]
    public void Extension_registers_the_managed_identity_source()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddLatticeMcpManagedIdentityAdministrator(o =>
        {
            o.Credential = Credential();
            o.Scope = "api://silo/.default";
        });

        using var provider = services.BuildServiceProvider();
        var source = provider.GetRequiredService<ILatticeApiMcpAdministratorCredentialSource>();

        Assert.That(source, Is.InstanceOf<ManagedIdentityAdministratorCredentialSource>());
    }

    [Test]
    public void Extension_replaces_a_previously_registered_static_source()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        // Simulate AddLatticeMcpRemote registering the static default first.
        services.Configure<LatticeApiMcpRemoteOptions>(_ => { });
        services.AddSingleton<ILatticeApiMcpAdministratorCredentialSource, StaticAdministratorCredentialSource>();

        services.AddLatticeMcpManagedIdentityAdministrator(o =>
        {
            o.Credential = Credential();
            o.Scope = "api://silo/.default";
        });

        using var provider = services.BuildServiceProvider();
        var sources = provider.GetServices<ILatticeApiMcpAdministratorCredentialSource>().ToList();

        Assert.Multiple(() =>
        {
            Assert.That(sources, Has.Count.EqualTo(1), "The static source must be replaced, not appended.");
            Assert.That(sources[0], Is.InstanceOf<ManagedIdentityAdministratorCredentialSource>());
        });
    }

    [Test]
    public void Extension_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(
                () => LatticeMcpManagedIdentityAdministratorServiceCollectionExtensions
                    .AddLatticeMcpManagedIdentityAdministrator(null!, _ => { }));
            Assert.Throws<ArgumentNullException>(
                () => new ServiceCollection().AddLatticeMcpManagedIdentityAdministrator(null!));
        });
    }

    private static ValidateOptionsResult Validate(LatticeApiMcpManagedIdentityAdministratorOptions options)
        => new LatticeApiMcpManagedIdentityAdministratorOptionsValidator().Validate(name: null, options);

    private static TokenCredential Credential()
        => new FakeTokenCredential(_ => new AccessToken("t", DateTimeOffset.UnixEpoch.AddHours(1)));
}
