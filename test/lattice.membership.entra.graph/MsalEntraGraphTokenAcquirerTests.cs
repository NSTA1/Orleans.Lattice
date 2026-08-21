using Microsoft.Identity.Client;
using NSubstitute;

namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Unit tests for <see cref="MsalEntraGraphTokenAcquirer"/> construction and its
/// argument validation. The MSAL confidential-client acquisition itself
/// (<see cref="MsalEntraGraphTokenAcquirer.AcquireAsync"/>) is an integration
/// concern against the sealed MSAL fluent builder and Azure AD, so it is not
/// exercised here; construction and its guards are.
/// </summary>
public class MsalEntraGraphTokenAcquirerTests
{
    private static IConfidentialClientApplication Application()
        => Substitute.For<IConfidentialClientApplication>();

    [Test]
    public void Constructor_null_application_throws()
    {
        Assert.That(
            () => new MsalEntraGraphTokenAcquirer(null!, new[] { "scope" }),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_scopes_throws()
    {
        Assert.That(
            () => new MsalEntraGraphTokenAcquirer(Application(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_with_application_and_scopes_succeeds()
    {
        Assert.That(
            () => new MsalEntraGraphTokenAcquirer(Application(), new[] { "https://graph.microsoft.com/.default" }),
            Throws.Nothing);
    }
}
