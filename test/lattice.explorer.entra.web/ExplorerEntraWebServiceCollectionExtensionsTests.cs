using Microsoft.AspNetCore.Components.Server.Circuits;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Entra.Web.Tests;

/// <summary>
/// Unit tests for
/// <see cref="ExplorerEntraWebServiceCollectionExtensions.AddLatticeExplorerEntraWebAuth"/>:
/// the registration validates the options and wires the scoped auth method,
/// acquirer, and (by default) the auto-sign-in circuit handler.
/// </summary>
[TestFixture]
public sealed class ExplorerEntraWebServiceCollectionExtensionsTests
{
    private static void ConfigureValid(ExplorerEntraWebOptions options)
    {
        options.TenantId = "tenant";
        options.ClientId = "client";
    }

    [Test]
    public void Throws_on_null_services()
    {
        Assert.Throws<ArgumentNullException>(
            () => ExplorerEntraWebServiceCollectionExtensions.AddLatticeExplorerEntraWebAuth(null!, ConfigureValid));
    }

    [Test]
    public void Throws_on_null_configure()
    {
        Assert.Throws<ArgumentNullException>(
            () => new ServiceCollection().AddLatticeExplorerEntraWebAuth(null!));
    }

    [Test]
    public void Throws_when_options_are_invalid()
    {
        Assert.Throws<InvalidOperationException>(
            () => new ServiceCollection().AddLatticeExplorerEntraWebAuth(_ => { }));
    }

    [Test]
    public void Registers_the_auth_method_as_a_scoped_provider()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(ConfigureValid);

        var descriptor = services.SingleOrDefault(d =>
            d.ServiceType == typeof(IExplorerAuthMethod)
            && d.ImplementationType == typeof(EntraWebExplorerAuthMethod));

        Assert.That(descriptor, Is.Not.Null);
        Assert.That(descriptor!.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
    }

    [Test]
    public void Registers_the_token_acquirer_as_scoped()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(ConfigureValid);

        var descriptor = services.SingleOrDefault(d => d.ServiceType == typeof(IExplorerWebTokenAcquirer));

        Assert.That(descriptor, Is.Not.Null);
        Assert.That(descriptor!.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
    }

    [Test]
    public void Registers_the_auto_sign_in_circuit_handler_by_default()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(ConfigureValid);

        var registered = services.Any(d =>
            d.ServiceType == typeof(CircuitHandler)
            && d.ImplementationType == typeof(ExplorerEntraWebAutoSignInCircuitHandler));

        Assert.That(registered, Is.True);
    }

    [Test]
    public void Omits_the_circuit_handler_when_auto_sign_in_is_disabled()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(o =>
        {
            ConfigureValid(o);
            o.AutoSignIn = false;
        });

        var registered = services.Any(d =>
            d.ServiceType == typeof(CircuitHandler)
            && d.ImplementationType == typeof(ExplorerEntraWebAutoSignInCircuitHandler));

        Assert.That(registered, Is.False);
    }

    [Test]
    public void Registers_cascading_authentication_state_so_the_circuit_sees_the_signed_in_user()
    {
        // Regression for the anonymous-circuit defect: without
        // AddCascadingAuthenticationState the Blazor Server circuit's
        // AuthenticationStateProvider reports the OIDC-authenticated user as
        // anonymous, so the auto-sign-in handler and token acquirer short-circuit
        // and every cluster call is made anonymously. Assert every service
        // AddCascadingAuthenticationState registers is present after the call, so
        // the check survives internal type renames in the framework.
        var expected = new ServiceCollection()
            .AddCascadingAuthenticationState()
            .Select(d => d.ServiceType)
            .ToHashSet();

        var actual = new ServiceCollection()
            .AddLatticeExplorerEntraWebAuth(ConfigureValid)
            .Select(d => d.ServiceType)
            .ToHashSet();

        Assert.That(expected, Is.Not.Empty);
        Assert.That(expected.IsSubsetOf(actual), Is.True);
    }

    [Test]
    public void Registers_the_reauth_options_with_the_challenge_path()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(ConfigureValid);

        using var provider = services.BuildServiceProvider();
        var reauth = provider.GetService<ExplorerReauthOptions>();
        Assert.Multiple(() =>
        {
            Assert.That(reauth, Is.Not.Null);
            Assert.That(reauth!.ChallengePath, Is.EqualTo("/explorer-entra/reauth"));
        });
    }

    [Test]
    public void Registers_the_reauth_options_with_a_custom_challenge_path()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(o =>
        {
            ConfigureValid(o);
            o.ReauthChallengePath = "/custom/reauth";
        });

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ExplorerReauthOptions>().ChallengePath, Is.EqualTo("/custom/reauth"));
    }

    [Test]
    public void Omits_the_reauth_options_override_when_challenge_path_is_cleared()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(o =>
        {
            ConfigureValid(o);
            o.ReauthChallengePath = null;
        });

        var registered = services.Any(d => d.ServiceType == typeof(ExplorerReauthOptions));
        Assert.That(registered, Is.False, "clearing the path leaves the core default (a plain reload) in place");
    }

    [Test]
    public void Registers_the_sign_out_options_with_the_federated_path()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(ConfigureValid);

        using var provider = services.BuildServiceProvider();
        var signOut = provider.GetService<ExplorerSignOutOptions>();
        Assert.Multiple(() =>
        {
            Assert.That(signOut, Is.Not.Null);
            Assert.That(signOut!.FederatedSignOutPath, Is.EqualTo("/explorer-entra/signout"));
        });
    }

    [Test]
    public void Registers_the_sign_out_options_with_a_custom_federated_path()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(o =>
        {
            ConfigureValid(o);
            o.SignOutPath = "/custom/logout";
        });

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ExplorerSignOutOptions>().FederatedSignOutPath, Is.EqualTo("/custom/logout"));
    }

    [Test]
    public void Omits_the_sign_out_options_override_when_path_is_cleared()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(o =>
        {
            ConfigureValid(o);
            o.SignOutPath = null;
        });

        var registered = services.Any(d => d.ServiceType == typeof(ExplorerSignOutOptions));
        Assert.That(registered, Is.False, "clearing the path leaves the core default (a local-only sign-out) in place");
    }

    [Test]
    public void Contributes_the_default_instance_origin_as_a_form_action_source()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(ConfigureValid);

        using var provider = services.BuildServiceProvider();
        var csp = provider.GetRequiredService<IOptions<ExplorerContentSecurityPolicyOptions>>().Value;

        // The federated sign-out POST is redirected to the Entra end-session URL,
        // which browsers check against form-action; the provider must permit that
        // origin so the default `form-action 'self'` policy does not block it.
        Assert.That(
            csp.AdditionalFormActionSources,
            Does.Contain("https://login.microsoftonline.com"));
    }

    [Test]
    public void Contributes_a_custom_instance_origin_as_a_form_action_source()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(o =>
        {
            ConfigureValid(o);
            o.Instance = "https://login.microsoftonline.us/";
        });

        using var provider = services.BuildServiceProvider();
        var csp = provider.GetRequiredService<IOptions<ExplorerContentSecurityPolicyOptions>>().Value;

        // Only the origin (scheme + authority) is contributed, never the trailing
        // path, so national-cloud and sovereign authorities are handled too.
        Assert.That(
            csp.AdditionalFormActionSources,
            Does.Contain("https://login.microsoftonline.us"));
    }

    [Test]
    public void Omits_the_form_action_source_when_the_federated_sign_out_is_cleared()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(o =>
        {
            ConfigureValid(o);
            o.SignOutPath = null;
        });

        using var provider = services.BuildServiceProvider();
        var csp = provider.GetRequiredService<IOptions<ExplorerContentSecurityPolicyOptions>>().Value;

        // With no federated sign-out wired there is no cross-origin redirect to
        // permit, so the tightest form-action policy is left in place.
        Assert.That(csp.AdditionalFormActionSources, Is.Empty);
    }

    [Test]
    public void Returns_the_same_service_collection()
    {
        var services = new ServiceCollection();

        var result = services.AddLatticeExplorerEntraWebAuth(ConfigureValid);

        Assert.That(result, Is.SameAs(services));
    }
}
