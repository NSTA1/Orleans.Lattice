using System.Reflection;

namespace Orleans.Lattice.Membership.Entra.Tests;

/// <summary>
/// Drift guard for the signature-algorithm pin seam this authenticator now relies
/// on instead of carrying its own copy of the guard.
/// </summary>
/// <remarks>
/// <see cref="JwtCredentialAuthenticator"/> applies the algorithm allow-list in a
/// single non-overridable step inside <c>AuthenticateAsync</c>, so a provider
/// override cannot be the place it is forgotten. That holds only while no subclass
/// declares <c>AuthenticateAsync</c> itself, which would route around the seam
/// entirely and silently restore the fail-open behaviour (CWE-347) that had to be
/// fixed once per call site five separate times. The guard lives in this project as
/// well as the core one because the core test project cannot see this assembly.
/// </remarks>
public class EntraAlgorithmPinSeamGuardTests
{
    [Test]
    public void No_entra_authenticator_overrides_the_method_that_applies_the_pin()
    {
        var subclasses = typeof(EntraCredentialAuthenticator).Assembly
            .GetTypes()
            .Where(t => typeof(JwtCredentialAuthenticator).IsAssignableFrom(t))
            .ToArray();

        var offenders = subclasses
            .Where(t => t.GetMethod(
                nameof(JwtCredentialAuthenticator.AuthenticateAsync),
                BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly) is not null)
            .Select(t => t.FullName)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                subclasses,
                Is.Not.Empty,
                "the guard scanned no authenticators, so it would pass vacuously");
            Assert.That(
                offenders,
                Is.Empty,
                "a subclass that declares AuthenticateAsync bypasses the algorithm-pin seam");
        });
    }

    /// <summary>
    /// The provider's fail-closed behaviour is now a property of the base options it
    /// builds rather than of an inline deny-all branch, so the flag that carries it
    /// must actually be set. Without it this authenticator would silently degrade
    /// from denying an empty allow-list to accepting any algorithm.
    /// </summary>
    [Test]
    public void Entra_authenticator_requires_an_algorithm_pin()
    {
        var options = new LatticeEntraAuthenticatorOptions
        {
            Authority = "https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000/v2.0",
        };
        options.TenantIds.Add("00000000-0000-0000-0000-000000000000");
        options.Audiences.Add("api://lattice");

        var authenticator = new EntraCredentialAuthenticator(options);

        var baseOptions = (JwtAuthenticatorOptions)typeof(JwtCredentialAuthenticator)
            .GetProperty("Options", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(authenticator)!;

        Assert.That(baseOptions.RequireAlgorithmPin, Is.True);
    }
}
