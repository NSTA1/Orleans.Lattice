using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.State.Grpc;

namespace Orleans.Lattice.Api.State.Grpc.Tests.Security;

/// <summary>
/// Registration coverage for <see cref="EnvVarCredentialAuthorizerServiceCollectionExtensions"/>.
/// The turnkey wiring is a security control: it must <em>replace</em> the
/// default-deny authorizer (not stack alongside it, which would leave the
/// resolution order deciding whether the endpoint fails open) and bring its own
/// environment reader, so a host that calls it gets exactly one, predictable
/// active policy.
/// </summary>
[TestFixture]
public sealed class EnvVarCredentialAuthorizerServiceCollectionExtensionsTests
{
    [Test]
    public void AddEnvVarCredentialAuthorizer_replaces_the_default_deny_authorizer()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        // Stand in for the binding's TryAdd default-deny registration that the
        // extension is expected to remove.
        services.AddSingleton<ILatticeStateApiAuthorizer, DenyAllStateApiAuthorizer>();

        services.AddEnvVarCredentialAuthorizer();

        using var provider = services.BuildServiceProvider();
        var authorizers = provider.GetServices<ILatticeStateApiAuthorizer>().ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(authorizers, Has.Length.EqualTo(1), "the default-deny authorizer must be replaced, not stacked");
            Assert.That(authorizers[0], Is.TypeOf<EnvVarCredentialAuthorizer>());
            Assert.That(provider.GetService<IEnvironmentVariableReader>(), Is.Not.Null);
        });
    }

    [Test]
    public void AddEnvVarCredentialAuthorizer_resolves_a_working_authorizer()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddEnvVarCredentialAuthorizer();

        using var provider = services.BuildServiceProvider();

        Assert.That(
            () => provider.GetRequiredService<ILatticeStateApiAuthorizer>(),
            Throws.Nothing,
            "the registered authorizer must resolve with only logging and the extension's own registrations present");
    }

    [Test]
    public void AddEnvVarCredentialAuthorizer_honours_the_configure_delegate()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddEnvVarCredentialAuthorizer(o =>
        {
            o.EnvironmentVariablePrefix = "CUSTOM_PREFIX_";
            o.MaxFailedAttempts = 7;
        });

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<EnvVarCredentialAuthorizerOptions>>().CurrentValue;

        Assert.Multiple(() =>
        {
            Assert.That(options.EnvironmentVariablePrefix, Is.EqualTo("CUSTOM_PREFIX_"));
            Assert.That(options.MaxFailedAttempts, Is.EqualTo(7));
        });
    }

    [Test]
    public void AddEnvVarCredentialAuthorizer_does_not_overwrite_a_custom_environment_reader()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var custom = new ThrowingEnvironmentVariableReader();
        services.AddSingleton<IEnvironmentVariableReader>(custom);

        services.AddEnvVarCredentialAuthorizer();

        using var provider = services.BuildServiceProvider();

        // The reader is registered with TryAdd, so a host's own reader wins.
        Assert.That(provider.GetRequiredService<IEnvironmentVariableReader>(), Is.SameAs(custom));
    }

    [Test]
    public void AddEnvVarCredentialAuthorizer_null_services_throws()
    {
        Assert.That(
            () => ((IServiceCollection)null!).AddEnvVarCredentialAuthorizer(),
            Throws.ArgumentNullException);
    }

    private sealed class ThrowingEnvironmentVariableReader : IEnvironmentVariableReader
    {
        public string? GetVariable(string name) => throw new InvalidOperationException("should not be called");
    }
}
