using System.Security.Cryptography;
using System.Text;
using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Regression tests for the signature-algorithm pin <b>seam</b>: the single
/// non-overridable point every authentication funnels through, whichever subclass
/// supplied the <see cref="TokenValidationParameters"/>.
/// </summary>
/// <remarks>
/// <para>
/// The sibling fixture
/// <c>JwtCredentialAuthenticatorTests.AlgorithmPinning</c> pins the behaviour of
/// the derivation for a host that configures its keys statically. This fixture
/// pins the two structural properties that derivation alone never gave:
/// </para>
/// <list type="number">
/// <item><description>
/// The allow-list is established for parameters a <em>subclass</em> produced, so a
/// provider authenticator cannot hand back unpinned parameters - the failure mode
/// that had the same defect fixed five separate times, once per call site.
/// </description></item>
/// <item><description>
/// A <see cref="JsonWebKey"/> is classified by its declared key type. A JWKS
/// document is the ordinary way a host supplies keys, and such a key derives from
/// <see cref="SecurityKey"/> directly rather than from the family-specific types,
/// so it previously fell through the derivation and left every discovery-driven
/// deployment with an unrestricted allow-list.
/// </description></item>
/// </list>
/// <para>
/// The library inversion behind all of it: an empty or absent
/// <see cref="TokenValidationParameters.ValidAlgorithms"/> is read by the token
/// validator as "accept any algorithm", not as "accept none", so refusing every
/// algorithm has to be expressed as an explicit
/// <see cref="TokenValidationParameters.AlgorithmValidator"/>.
/// </para>
/// </remarks>
public partial class JwtCredentialAuthenticatorTests
{
    /// <summary>
    /// A second, distinct symmetric key. <c>NewSymmetricKey</c> is deterministic, so
    /// it cannot stand in for "a key the host does not trust".
    /// </summary>
    private static SymmetricSecurityKey NewUntrustedSymmetricKey()
        => new(Encoding.UTF8.GetBytes("membership-pin-seam-untrusted-key-9876543210"));

    private static JsonWebKey NewRsaJsonWebKey()        => JsonWebKeyConverter.ConvertFromRSASecurityKey(new RsaSecurityKey(RSA.Create(2048)));

    private static JsonWebKey NewEcJsonWebKey()
        => JsonWebKeyConverter.ConvertFromECDsaSecurityKey(
            new ECDsaSecurityKey(ECDsa.Create(ECCurve.NamedCurves.nistP256)));

    private static JsonWebKey NewOctetJsonWebKey()
        => JsonWebKeyConverter.ConvertFromSymmetricSecurityKey(NewSymmetricKey());

    private static TokenValidationParameters ParametersWithKeys(params SecurityKey[] keys)
        => new() { IssuerSigningKeys = keys };

    // -- The JsonWebKey arm: the half the derivation previously missed ----------

    /// <summary>
    /// The regression that matters most in practice. A JWKS endpoint yields
    /// <see cref="JsonWebKey"/> instances, which are not
    /// <see cref="RsaSecurityKey"/>, so the family-based derivation used to fall
    /// through to "leave unrestricted" for exactly the deployments that fetch their
    /// keys from a provider - while narrowing the static-key deployments that were
    /// never the interesting case.
    /// </summary>
    [Test]
    public void ApplyAlgorithmPin_derives_the_rsa_family_from_an_rsa_json_web_key()
    {
        var parameters = ParametersWithKeys(NewRsaJsonWebKey());

        JwtCredentialAuthenticator.ApplyAlgorithmPin(parameters, requirePin: false);

        Assert.Multiple(() =>
        {
            Assert.That(parameters.ValidAlgorithms, Does.Contain(SecurityAlgorithms.RsaSha256));
            Assert.That(parameters.ValidAlgorithms, Does.Not.Contain(SecurityAlgorithms.HmacSha256));
            Assert.That(parameters.ValidAlgorithms, Does.Not.Contain(SecurityAlgorithms.EcdsaSha256));
        });
    }

    [Test]
    public void ApplyAlgorithmPin_derives_the_ecdsa_family_from_an_ec_json_web_key()
    {
        var parameters = ParametersWithKeys(NewEcJsonWebKey());

        JwtCredentialAuthenticator.ApplyAlgorithmPin(parameters, requirePin: false);

        Assert.Multiple(() =>
        {
            Assert.That(parameters.ValidAlgorithms, Does.Contain(SecurityAlgorithms.EcdsaSha256));
            Assert.That(parameters.ValidAlgorithms, Does.Not.Contain(SecurityAlgorithms.HmacSha256));
            Assert.That(parameters.ValidAlgorithms, Does.Not.Contain(SecurityAlgorithms.RsaSha256));
        });
    }

    [Test]
    public void ApplyAlgorithmPin_derives_the_hmac_family_from_an_octet_json_web_key()
    {
        var parameters = ParametersWithKeys(NewOctetJsonWebKey());

        JwtCredentialAuthenticator.ApplyAlgorithmPin(parameters, requirePin: false);

        Assert.Multiple(() =>
        {
            Assert.That(parameters.ValidAlgorithms, Does.Contain(SecurityAlgorithms.HmacSha256));
            Assert.That(parameters.ValidAlgorithms, Does.Not.Contain(SecurityAlgorithms.RsaSha256));
        });
    }

    /// <summary>
    /// A key type the derivation cannot classify keeps the historical unrestricted
    /// behaviour rather than locking out a host that is working today. Narrowing is
    /// only safe when the family is actually known.
    /// </summary>
    [Test]
    public void ApplyAlgorithmPin_leaves_an_unclassifiable_json_web_key_unrestricted()
    {
        var parameters = ParametersWithKeys(new JsonWebKey { Kty = "OKP" });

        JwtCredentialAuthenticator.ApplyAlgorithmPin(parameters, requirePin: false);

        Assert.Multiple(() =>
        {
            Assert.That(parameters.ValidAlgorithms, Is.Null.Or.Empty);
            Assert.That(parameters.AlgorithmValidator, Is.Null);
        });
    }

    /// <summary>
    /// A JWKS document that mixes families is exactly the shape the derivation
    /// cannot narrow without risking a legitimate rejection, so it must not try.
    /// </summary>
    [Test]
    public void ApplyAlgorithmPin_leaves_a_mixed_json_web_key_set_unrestricted()
    {
        var parameters = ParametersWithKeys(NewRsaJsonWebKey(), NewOctetJsonWebKey());

        JwtCredentialAuthenticator.ApplyAlgorithmPin(parameters, requirePin: false);

        Assert.Multiple(() =>
        {
            Assert.That(parameters.ValidAlgorithms, Is.Null.Or.Empty);
            Assert.That(parameters.AlgorithmValidator, Is.Null);
        });
    }

    /// <summary>
    /// The derivation reads the singular key property as well as the collection, so
    /// a provider that resolves one key rather than a set is covered identically.
    /// </summary>
    [Test]
    public void ApplyAlgorithmPin_derives_from_the_singular_issuer_signing_key()
    {
        var parameters = new TokenValidationParameters { IssuerSigningKey = NewRsaKey() };

        JwtCredentialAuthenticator.ApplyAlgorithmPin(parameters, requirePin: false);

        Assert.That(parameters.ValidAlgorithms, Does.Contain(SecurityAlgorithms.RsaSha256));
    }

    // -- Precedence: an established allow-list is authoritative ----------------

    [Test]
    public void ApplyAlgorithmPin_does_not_widen_an_established_allow_list()
    {
        var parameters = ParametersWithKeys(NewRsaJsonWebKey());
        parameters.ValidAlgorithms = [SecurityAlgorithms.RsaSha512];

        JwtCredentialAuthenticator.ApplyAlgorithmPin(parameters, requirePin: true);

        Assert.Multiple(() =>
        {
            Assert.That(parameters.ValidAlgorithms, Is.EquivalentTo(new[] { SecurityAlgorithms.RsaSha512 }));
            Assert.That(parameters.AlgorithmValidator, Is.Null);
        });
    }

    [Test]
    public void ApplyAlgorithmPin_does_not_replace_an_established_algorithm_validator()
    {
        var parameters = ParametersWithKeys(NewRsaJsonWebKey());
        var called = false;
        parameters.AlgorithmValidator = (_, _, _, _) =>
        {
            called = true;
            return true;
        };

        JwtCredentialAuthenticator.ApplyAlgorithmPin(parameters, requirePin: true);
        _ = parameters.AlgorithmValidator!(SecurityAlgorithms.HmacSha256, null!, null!, parameters);

        Assert.Multiple(() =>
        {
            Assert.That(called, Is.True);
            Assert.That(parameters.ValidAlgorithms, Is.Null.Or.Empty);
        });
    }

    /// <summary>
    /// The inversion at the heart of this whole guard: an allow-list holding only
    /// blank entries restricts nothing, so it must not be mistaken for an
    /// established one.
    /// </summary>
    [Test]
    public void HasAlgorithmRestriction_treats_a_blank_allow_list_as_unrestricted()
    {
        var blank = new TokenValidationParameters { ValidAlgorithms = ["", "  "] };
        var empty = new TokenValidationParameters { ValidAlgorithms = [] };
        var absent = new TokenValidationParameters();
        var real = new TokenValidationParameters { ValidAlgorithms = [SecurityAlgorithms.RsaSha256] };
        var validated = new TokenValidationParameters { AlgorithmValidator = (_, _, _, _) => true };

        Assert.Multiple(() =>
        {
            Assert.That(JwtCredentialAuthenticator.HasAlgorithmRestriction(blank), Is.False);
            Assert.That(JwtCredentialAuthenticator.HasAlgorithmRestriction(empty), Is.False);
            Assert.That(JwtCredentialAuthenticator.HasAlgorithmRestriction(absent), Is.False);
            Assert.That(JwtCredentialAuthenticator.HasAlgorithmRestriction(real), Is.True);
            Assert.That(JwtCredentialAuthenticator.HasAlgorithmRestriction(validated), Is.True);
        });
    }

    /// <summary>
    /// A blank allow-list must be treated as the absence of one, right through the
    /// seam - not merely by the predicate in isolation.
    /// </summary>
    [Test]
    public void ApplyAlgorithmPin_establishes_an_allow_list_over_a_blank_one()
    {
        var parameters = ParametersWithKeys(NewRsaJsonWebKey());
        parameters.ValidAlgorithms = ["  "];

        JwtCredentialAuthenticator.ApplyAlgorithmPin(parameters, requirePin: false);

        Assert.That(parameters.ValidAlgorithms, Does.Contain(SecurityAlgorithms.RsaSha256));
    }

    // -- Not narrowing when another source can supply keys ---------------------

    /// <summary>
    /// The non-breaking bar: a resolver can hand the validator a key of a family the
    /// statically-visible keys never show, so deriving from what is visible could
    /// refuse a token that legitimately verifies. Derive nothing instead.
    /// </summary>
    [Test]
    public void ApplyAlgorithmPin_derives_nothing_when_a_signing_key_resolver_can_supply_other_keys()
    {
        var parameters = ParametersWithKeys(NewRsaKey());
        parameters.IssuerSigningKeyResolver = (_, _, _, _) => [NewSymmetricKey()];

        JwtCredentialAuthenticator.ApplyAlgorithmPin(parameters, requirePin: false);

        Assert.Multiple(() =>
        {
            Assert.That(parameters.ValidAlgorithms, Is.Null.Or.Empty);
            Assert.That(parameters.AlgorithmValidator, Is.Null);
        });
    }

    /// <summary>
    /// Same reasoning for a configuration manager, which is how the shipped
    /// discovery-driven providers supply their keys. They compensate by requiring a
    /// pin, which is asserted below.
    /// </summary>
    [Test]
    public void ApplyAlgorithmPin_derives_nothing_when_a_configuration_manager_can_supply_other_keys()
    {
        var parameters = ParametersWithKeys(NewRsaKey());
        parameters.ConfigurationManager = new StubConfigurationManager();

        JwtCredentialAuthenticator.ApplyAlgorithmPin(parameters, requirePin: false);

        Assert.Multiple(() =>
        {
            Assert.That(parameters.ValidAlgorithms, Is.Null.Or.Empty);
            Assert.That(parameters.AlgorithmValidator, Is.Null);
        });
    }

    // -- RequireAlgorithmPin: the opt-in fail-closed arm -----------------------

    /// <summary>
    /// The flag must default off, because turning it on by default would refuse
    /// traffic that authenticates today on any deployment whose family cannot be
    /// established.
    /// </summary>
    [Test]
    public void RequireAlgorithmPin_defaults_to_false()
        => Assert.That(new JwtAuthenticatorOptions().RequireAlgorithmPin, Is.False);

    [Test]
    public void ApplyAlgorithmPin_leaves_it_unrestricted_when_nothing_is_derivable_and_no_pin_is_required()
    {
        var parameters = new TokenValidationParameters { ConfigurationManager = new StubConfigurationManager() };

        JwtCredentialAuthenticator.ApplyAlgorithmPin(parameters, requirePin: false);

        Assert.Multiple(() =>
        {
            Assert.That(parameters.ValidAlgorithms, Is.Null.Or.Empty);
            Assert.That(parameters.AlgorithmValidator, Is.Null);
        });
    }

    /// <summary>
    /// The fail-closed arm the shipped providers rely on. It must deny rather than
    /// leave an empty allow-list behind, because an empty allow-list is permissive.
    /// </summary>
    [Test]
    public void ApplyAlgorithmPin_denies_every_algorithm_when_nothing_is_derivable_and_a_pin_is_required()
    {
        var parameters = new TokenValidationParameters { ConfigurationManager = new StubConfigurationManager() };

        JwtCredentialAuthenticator.ApplyAlgorithmPin(parameters, requirePin: true);

        Assert.That(parameters.AlgorithmValidator, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(
                parameters.AlgorithmValidator!(SecurityAlgorithms.RsaSha256, null!, null!, parameters),
                Is.False);
            Assert.That(
                parameters.AlgorithmValidator!(SecurityAlgorithms.HmacSha256, null!, null!, parameters),
                Is.False);
        });
    }

    /// <summary>
    /// Requiring a pin must not deny a deployment whose family the seam can
    /// establish - otherwise the flag would be an outage switch rather than a
    /// tightening.
    /// </summary>
    [Test]
    public void ApplyAlgorithmPin_requiring_a_pin_still_derives_rather_than_denying()
    {
        var parameters = ParametersWithKeys(NewRsaJsonWebKey());

        JwtCredentialAuthenticator.ApplyAlgorithmPin(parameters, requirePin: true);

        Assert.Multiple(() =>
        {
            Assert.That(parameters.ValidAlgorithms, Does.Contain(SecurityAlgorithms.RsaSha256));
            Assert.That(parameters.AlgorithmValidator, Is.Null);
        });
    }

    // -- The seam covers subclasses -------------------------------------------

    /// <summary>
    /// The structural point of the whole change. A subclass returns parameters with
    /// no allow-list, exactly as every provider override used to before its own
    /// point fix; the seam establishes one anyway, so the override cannot be the
    /// place the guard is forgotten.
    /// </summary>
    [Test]
    public async Task AuthenticateAsync_establishes_the_allow_list_on_parameters_a_subclass_returned()
    {
        var rsa = NewRsaKey();
        var supplied = new TokenValidationParameters
        {
            ValidateIssuer = false,
            ValidateAudience = false,
            ValidateLifetime = false,
            IssuerSigningKeys = [rsa],
        };
        var authenticator = new SuppliedParametersAuthenticator(NewBaseOptions(), supplied);

        _ = await authenticator.AuthenticateAsync(
            new LatticeCredential(MintTokenWithAlgorithm(rsa, SecurityAlgorithms.RsaSha256)),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(supplied.ValidAlgorithms, Does.Contain(SecurityAlgorithms.RsaSha256));
            Assert.That(supplied.ValidAlgorithms, Does.Not.Contain(SecurityAlgorithms.HmacSha256));
        });
    }

    /// <summary>
    /// The behavioural half of the same point: an unpinned subclass must actually
    /// refuse an out-of-family token, not merely carry a narrower allow-list.
    /// </summary>
    [Test]
    public async Task AuthenticateAsync_rejects_an_hmac_token_for_parameters_a_subclass_left_unpinned()
    {
        var symmetric = NewSymmetricKey();
        var supplied = new TokenValidationParameters
        {
            ValidateIssuer = false,
            ValidateAudience = false,
            ValidateLifetime = false,
            // The signing key is present and the token verifies against it, so the
            // only thing that can refuse this token is the algorithm allow-list the
            // seam derived from the RSA key sitting alongside it.
            IssuerSigningKeys = [NewRsaKey()],
        };
        var authenticator = new SuppliedParametersAuthenticator(NewBaseOptions(), supplied);

        var result = await authenticator.AuthenticateAsync(
            new LatticeCredential(MintTokenWithAlgorithm(symmetric, SecurityAlgorithms.HmacSha256)),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Null);
            Assert.That(supplied.ValidAlgorithms, Does.Not.Contain(SecurityAlgorithms.HmacSha256));
        });
    }

    /// <summary>
    /// A subclass that requires a pin and resolves its keys through a configuration
    /// manager fails closed - the shape both shipped provider authenticators rely on
    /// now that their inline deny-all copies are gone.
    /// </summary>
    [Test]
    public async Task AuthenticateAsync_denies_every_algorithm_for_an_unpinned_subclass_that_requires_a_pin()
    {
        var rsa = NewRsaKey();
        var supplied = new TokenValidationParameters
        {
            ValidateIssuer = false,
            ValidateAudience = false,
            ValidateLifetime = false,
            IssuerSigningKeys = [rsa],
            ConfigurationManager = new StubConfigurationManager(),
        };
        var options = NewBaseOptions();
        options.RequireAlgorithmPin = true;
        var authenticator = new SuppliedParametersAuthenticator(options, supplied);

        var result = await authenticator.AuthenticateAsync(
            new LatticeCredential(MintTokenWithAlgorithm(rsa, SecurityAlgorithms.RsaSha256)),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Null);
            Assert.That(supplied.AlgorithmValidator, Is.Not.Null);
        });
    }

    /// <summary>
    /// The same subclass authenticates normally with the flag left at its default,
    /// proving the test above failed on the required pin rather than on the token,
    /// the key, or the stub configuration manager.
    /// </summary>
    [Test]
    public async Task AuthenticateAsync_unpinned_subclass_still_authenticates_when_no_pin_is_required()
    {
        var rsa = NewRsaKey();
        var supplied = new TokenValidationParameters
        {
            ValidateIssuer = false,
            ValidateAudience = false,
            ValidateLifetime = false,
            IssuerSigningKeys = [rsa],
            ConfigurationManager = new StubConfigurationManager(),
        };
        var authenticator = new SuppliedParametersAuthenticator(NewBaseOptions(), supplied);

        var result = await authenticator.AuthenticateAsync(
            new LatticeCredential(MintTokenWithAlgorithm(rsa, SecurityAlgorithms.RsaSha256)),
            CancellationToken.None);

        Assert.That(result, Is.Not.Null);
    }

    /// <summary>
    /// The drift guard. Enforcement is only single-seam for as long as no subclass
    /// replaces <c>AuthenticateAsync</c> itself, which would route around it
    /// entirely - the exact regression the five previous point fixes kept
    /// re-introducing one call site at a time.
    /// </summary>
    [Test]
    public void No_authenticator_subclass_overrides_the_method_that_applies_the_pin()
    {
        var subclasses = typeof(JwtCredentialAuthenticator).Assembly
            .GetTypes()
            .Concat(typeof(JwtCredentialAuthenticatorTests).Assembly.GetTypes())
            .Where(t => t != typeof(JwtCredentialAuthenticator)
                && typeof(JwtCredentialAuthenticator).IsAssignableFrom(t))
            .ToArray();

        var offenders = subclasses
            .Where(t => t.GetMethod(
                nameof(JwtCredentialAuthenticator.AuthenticateAsync),
                System.Reflection.BindingFlags.Public
                | System.Reflection.BindingFlags.Instance
                | System.Reflection.BindingFlags.DeclaredOnly) is not null)
            .Select(t => t.FullName)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                subclasses,
                Is.Not.Empty,
                "the guard scanned no subclasses, so it would pass vacuously");
            Assert.That(
                offenders,
                Is.Empty,
                "a subclass that declares AuthenticateAsync bypasses the algorithm-pin seam");
        });
    }

    /// <summary>
    /// The post-validation hook that exists so a provider never has to override
    /// <c>AuthenticateAsync</c> to enrich a result - an override there would sit
    /// outside the pin seam and could route around it.
    /// </summary>
    [Test]
    public async Task AuthenticateAsync_invokes_the_enrichment_hook_with_the_validated_principal()
    {
        var key = NewSymmetricKey();
        var options = NewBaseOptions();
        options.SigningKeys.Add(key);
        var authenticator = new EnrichingAuthenticator(options);

        var result = await authenticator.AuthenticateAsync(
            new LatticeCredential(MintTokenWithAlgorithm(key, SecurityAlgorithms.HmacSha256)),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(authenticator.Enriched, Is.EqualTo(1));
            Assert.That(result, Is.Not.Null);
            Assert.That(result!.AssertedGroups, Does.Contain("enriched"));
        });
    }

    /// <summary>
    /// The hook is a post-validation step, so a credential that never authenticates
    /// must not reach it - an implementation may assume the principal is real.
    /// </summary>
    [Test]
    public async Task AuthenticateAsync_does_not_invoke_the_enrichment_hook_when_validation_fails()
    {
        var options = NewBaseOptions();
        options.SigningKeys.Add(NewSymmetricKey());
        var authenticator = new EnrichingAuthenticator(options);

        var result = await authenticator.AuthenticateAsync(
            new LatticeCredential(MintTokenWithAlgorithm(NewUntrustedSymmetricKey(), SecurityAlgorithms.HmacSha256)),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Null);
            Assert.That(authenticator.Enriched, Is.Zero);
        });
    }

    /// <summary>
    /// The base hook must be a pass-through, so a subclass that does not override it
    /// sees no change in behaviour.
    /// </summary>
    [Test]
    public async Task AuthenticateAsync_returns_the_mapped_principal_unchanged_without_an_enrichment_override()
    {
        var key = NewSymmetricKey();
        var authenticator = CreatePinningAuthenticator([key]);

        var result = await authenticator.AuthenticateAsync(
            new LatticeCredential(MintTokenWithAlgorithm(key, SecurityAlgorithms.HmacSha256)),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Not.Null);
            Assert.That(result!.SubjectId, Is.EqualTo("user-1"));
            Assert.That(result.AssertedGroups ?? [], Does.Not.Contain("enriched"));
        });
    }

    private static JwtAuthenticatorOptions NewBaseOptions()
    {
        var options = new JwtAuthenticatorOptions { Issuer = Issuer };
        options.Audiences.Add(Audience);
        return options;
    }

    /// <summary>
    /// Stands in for a provider authenticator that builds its own validation
    /// parameters and never establishes an algorithm allow-list. The instance it
    /// returns is the one the test holds, so the test can observe what the seam did
    /// to it.
    /// </summary>
    private sealed class SuppliedParametersAuthenticator(
        JwtAuthenticatorOptions options,
        TokenValidationParameters supplied)
        : JwtCredentialAuthenticator(options)
    {
        protected override ValueTask<TokenValidationParameters> ResolveValidationParametersAsync(
            LatticeCredential credential,
            CancellationToken cancellationToken) => new(supplied);
    }

    /// <summary>
    /// Stands in for a provider that enriches the principal from a source outside
    /// the token, the way the Entra authenticator resolves an overflowed groups
    /// claim out of band.
    /// </summary>
    private sealed class EnrichingAuthenticator(JwtAuthenticatorOptions options)
        : JwtCredentialAuthenticator(options)
    {
        public int Enriched { get; private set; }

        protected override ValueTask<LatticePrincipal?> EnrichPrincipalAsync(
            LatticePrincipal principal,
            LatticeCredential credential,
            CancellationToken cancellationToken)
        {
            Enriched++;
            var groups = principal.AssertedGroups is { } existing
                ? new List<string>(existing) { "enriched" }
                : ["enriched"];
            return new(principal with { AssertedGroups = groups });
        }
    }

    /// <summary>
    /// The smallest thing that satisfies <see cref="BaseConfigurationManager"/>, so
    /// the seam's "another source can supply keys" bail-out can be exercised without
    /// a live metadata endpoint.
    /// </summary>
    private sealed class StubConfigurationManager : BaseConfigurationManager
    {
        public override Task<BaseConfiguration> GetBaseConfigurationAsync(CancellationToken cancel)
            => Task.FromResult<BaseConfiguration>(new StubConfiguration());

        public override void RequestRefresh()
        {
        }

        private sealed class StubConfiguration : BaseConfiguration
        {
            public override string Issuer { get; set; } = JwtCredentialAuthenticatorTests.Issuer;
        }
    }
}
