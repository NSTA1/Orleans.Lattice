using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Shell.Session;

namespace Orleans.Lattice.Explorer.Tests.Shell.Session;

/// <summary>
/// Which sign-in methods an endpoint is offered, decided only through Core's
/// <see cref="IExplorerAuthMethod.CanHandle"/> seam.
/// </summary>
[TestFixture]
public sealed class SessionSignInChoiceTests
{
    private static readonly IExplorerAuthMethod Basic = new BasicExplorerAuthMethod();

    [Test]
    public void Resolve_rejects_missing_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => SessionSignInChoice.Resolve(null!, [Basic]), Throws.ArgumentNullException);
            Assert.That(() => SessionSignInChoice.Resolve(ExplorerAuthSchemeAdvertisement.Empty, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void An_empty_advertisement_offers_every_method_that_accepts_one()
    {
        var bespoke = new FakeAuthMethod("apikey", scheme => scheme.Length == 0);
        var entra = new FakeAuthMethod(ExplorerAuthSchemes.Entra);

        var choice = SessionSignInChoice.Resolve(ExplorerAuthSchemeAdvertisement.Empty, [Basic, entra, bespoke]);

        Assert.Multiple(() =>
        {
            Assert.That(choice.IsUnsupported, Is.False);
            Assert.That(choice.UnsupportedSchemes, Is.Empty);
            Assert.That(choice.Methods, Is.EqualTo(new[]
            {
                new SessionSignInMethod(ExplorerAuthSchemes.Basic, "Username and password", UsesPassword: true),
                new SessionSignInMethod("apikey", "apikey", UsesPassword: false),
            }));
        });
    }

    [Test]
    public void An_empty_advertisement_with_no_taker_still_falls_back_to_the_password_form()
    {
        var choice = SessionSignInChoice.Resolve(ExplorerAuthSchemeAdvertisement.Empty, [new FakeAuthMethod(ExplorerAuthSchemes.Entra)]);

        Assert.That(choice.Methods, Is.EqualTo(new[] { new SessionSignInMethod(ExplorerAuthSchemes.Basic, "Username and password", true) }));
    }

    [Test]
    public void Advertised_schemes_are_offered_in_the_endpoints_order_with_its_names()
    {
        var entra = new FakeAuthMethod(ExplorerAuthSchemes.Entra);
        var advertisement = Advertise((ExplorerAuthSchemes.Entra, "Microsoft Entra ID"), ("BASIC", " "));

        var choice = SessionSignInChoice.Resolve(advertisement, [Basic, entra]);

        Assert.That(choice.Methods, Is.EqualTo(new[]
        {
            new SessionSignInMethod(ExplorerAuthSchemes.Entra, "Microsoft Entra ID", false),
            new SessionSignInMethod("BASIC", "Username and password", true),
        }));
    }

    [Test]
    public void A_method_is_offered_once_even_when_it_handles_several_advertised_schemes()
    {
        var family = new FakeAuthMethod("oidc", scheme => scheme.StartsWith("oidc", StringComparison.Ordinal));
        var advertisement = Advertise(("oidc-a", "A"), ("oidc-b", "B"));

        var choice = SessionSignInChoice.Resolve(advertisement, [family]);

        Assert.That(choice.Methods.Select(method => method.SchemeId), Is.EqualTo(new[] { "oidc-a" }));
    }

    [Test]
    public void An_advertised_scheme_with_no_display_name_is_shown_by_its_id()
    {
        var choice = SessionSignInChoice.Resolve(Advertise(("custom", string.Empty)), [new FakeAuthMethod("custom")]);

        Assert.That(choice.Methods.Single().DisplayName, Is.EqualTo("custom"));
    }

    [Test]
    public void Only_unserviced_schemes_is_unsupported_and_names_them_all()
    {
        var choice = SessionSignInChoice.Resolve(Advertise((ExplorerAuthSchemes.Entra, "Entra"), ("custom", "Custom")), [new FakeAuthMethod("other")]);

        Assert.Multiple(() =>
        {
            Assert.That(choice.IsUnsupported, Is.True);
            Assert.That(choice.Methods, Is.Empty);
            Assert.That(choice.UnsupportedSchemes, Is.EqualTo(new[] { ExplorerAuthSchemes.Entra, "custom" }));
        });
    }

    private static ExplorerAuthSchemeAdvertisement Advertise(params (string Scheme, string DisplayName)[] schemes) => new()
    {
        Schemes = schemes.Select(scheme => new ExplorerAuthSchemeDescriptor { SchemeId = scheme.Scheme, DisplayName = scheme.DisplayName }).ToArray(),
    };
}
