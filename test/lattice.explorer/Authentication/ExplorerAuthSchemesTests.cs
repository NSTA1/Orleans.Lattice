using System.Reflection;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// Guards <see cref="ExplorerAuthSchemes"/> as wire-visible contract. The scheme
/// ids, challenge input keys, and advertised-parameter keys are shared with the
/// server advertisement configuration of every host that publishes them, so a
/// rename silently breaks a deployment still advertising the old key.
/// </summary>
[TestFixture]
public sealed class ExplorerAuthSchemesTests
{
    /// <summary>
    /// The complete advertised-parameter vocabulary, keyed by constant name. A
    /// new key has to be added here deliberately; a renamed or re-valued key
    /// fails the guard.
    /// </summary>
    private static readonly Dictionary<string, string> ExpectedParameters = new(StringComparer.Ordinal)
    {
        [nameof(ExplorerAuthSchemes.AuthorityParameter)] = "authority",
        [nameof(ExplorerAuthSchemes.TenantIdParameter)] = "tenantId",
        [nameof(ExplorerAuthSchemes.ClientIdParameter)] = "clientId",
        [nameof(ExplorerAuthSchemes.AudienceParameter)] = "audience",
        [nameof(ExplorerAuthSchemes.ScopeParameter)] = "scope",
        [nameof(ExplorerAuthSchemes.MetadataAddressParameter)] = "metadataAddress",
    };

    [Test]
    public void ScopeParameter_is_the_oauth_scope_key()
    {
        Assert.That(ExplorerAuthSchemes.ScopeParameter, Is.EqualTo("scope"));
    }

    [Test]
    public void MetadataAddressParameter_is_the_discovery_document_key()
    {
        Assert.That(ExplorerAuthSchemes.MetadataAddressParameter, Is.EqualTo("metadataAddress"));
    }

    [Test]
    public void Scheme_ids_keep_their_advertised_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAuthSchemes.Basic, Is.EqualTo("basic"));
            Assert.That(ExplorerAuthSchemes.Entra, Is.EqualTo("entra"));
            Assert.That(ExplorerAuthSchemes.Oidc, Is.EqualTo("oidc"));
        });
    }

    [Test]
    public void Input_keys_keep_their_challenge_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAuthSchemes.UsernameInput, Is.EqualTo("username"));
            Assert.That(ExplorerAuthSchemes.PasswordInput, Is.EqualTo("password"));
        });
    }

    [Test]
    public void Advertised_parameter_keys_match_the_documented_vocabulary_exactly()
    {
        var declared = DeclaredParameterConstants();

        Assert.Multiple(() =>
        {
            Assert.That(
                declared.Keys,
                Is.EquivalentTo(ExpectedParameters.Keys),
                "a new advertised-parameter constant has to be added to this guard deliberately");

            foreach (var (name, value) in ExpectedParameters)
            {
                Assert.That(
                    declared.GetValueOrDefault(name),
                    Is.EqualTo(value),
                    $"{name} is wire-visible contract shared with server advertisement configuration");
            }
        });
    }

    [Test]
    public void Advertised_parameter_keys_are_distinct()
    {
        var values = DeclaredParameterConstants().Values;

        Assert.That(values, Is.Unique, "two parameter constants sharing a key would collide on the wire");
    }

    /// <summary>
    /// Reflects the advertised-parameter constants actually declared on
    /// <see cref="ExplorerAuthSchemes"/>, so the guard fails on an addition it
    /// does not know about as well as on a rename.
    /// </summary>
    private static Dictionary<string, string> DeclaredParameterConstants()
        => typeof(ExplorerAuthSchemes)
            .GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Where(field => field is { IsLiteral: true, IsInitOnly: false } && field.FieldType == typeof(string))
            .Where(field => field.Name.EndsWith("Parameter", StringComparison.Ordinal))
            .ToDictionary(field => field.Name, field => (string)field.GetRawConstantValue()!, StringComparer.Ordinal);
}
