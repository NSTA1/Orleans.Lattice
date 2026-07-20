using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Round-trips the transport-agnostic control-facade DTOs through the Orleans
/// serializer to prove the contract is coherent and stable. Every serializable
/// facade request / response record is covered so a field renumbering or alias
/// drift is caught here rather than at the wire.
/// </summary>
[TestFixture]
public sealed class AuthDtoSerializationTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    private static LatticeAuthorizationRule SampleRule(string ruleId = "r1", string treeId = "tree-a") =>
        new(
            ruleId,
            LatticeSubjectSelector.Group("g1"),
            LatticeScope.Prefix(treeId, "orders/"),
            LatticeOperation.Read | LatticeOperation.Write,
            LatticeEffect.Allow);

    [Test]
    public void AuthUser_round_trips_with_claims()
    {
        var original = new AuthUser
        {
            UserId = "u1",
            DisplayName = "User One",
            Claims = new Dictionary<string, string> { ["role"] = "ops" },
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.UserId, Is.EqualTo("u1"));
            Assert.That(copy.DisplayName, Is.EqualTo("User One"));
            Assert.That(copy.Claims, Is.Not.Null);
            Assert.That(copy.Claims!["role"], Is.EqualTo("ops"));
        });
    }

    [Test]
    public void AuthUser_round_trips_without_optional_fields()
    {
        var copy = RoundTrip(new AuthUser { UserId = "u2" });
        Assert.Multiple(() =>
        {
            Assert.That(copy.UserId, Is.EqualTo("u2"));
            Assert.That(copy.DisplayName, Is.Null);
            Assert.That(copy.Claims, Is.Null);
        });
    }

    [Test]
    public void AuthGroup_round_trips()
    {
        var original = new AuthGroup { GroupId = "g1", DisplayName = "Group One" };
        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void AuthPageRequest_round_trips_with_token()
    {
        var original = new AuthPageRequest { PageSize = 25, PageToken = "cursor-x" };
        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void AuthUserPage_round_trips()
    {
        var original = new AuthUserPage
        {
            Entries = [new AuthUser { UserId = "u1" }, new AuthUser { UserId = "u2" }],
            NextPageToken = "u2",
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Entries.Select(e => e.UserId), Is.EqualTo(new[] { "u1", "u2" }));
            Assert.That(copy.NextPageToken, Is.EqualTo("u2"));
        });
    }

    [Test]
    public void AuthGroupPage_round_trips()
    {
        var original = new AuthGroupPage
        {
            Entries = [new AuthGroup { GroupId = "g1" }],
            NextPageToken = null,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Entries.Select(e => e.GroupId), Is.EqualTo(new[] { "g1" }));
            Assert.That(copy.NextPageToken, Is.Null);
        });
    }

    [Test]
    public void AuthRulePage_round_trips()
    {
        var original = new AuthRulePage
        {
            Entries = [SampleRule("r1"), SampleRule("r2")],
            NextPageToken = "tree-a\u001fr2",
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Entries.Select(r => r.RuleId), Is.EqualTo(new[] { "r1", "r2" }));
            Assert.That(copy.NextPageToken, Is.EqualTo("tree-a\u001fr2"));
        });
    }

    [Test]
    public void AuthExplanation_round_trips()
    {
        var original = new AuthExplanation
        {
            SubjectId = "u1",
            GroupIds = ["g1", "g2"],
            Operation = LatticeOperation.Write,
            Scope = LatticeScope.Key("tree-a", "orders/1"),
            Allowed = true,
            Filtered = false,
            Reason = "matched rule r1",
            DefaultEffect = LatticeEffect.Deny,
            MatchedRules = [SampleRule("r1")],
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.SubjectId, Is.EqualTo("u1"));
            Assert.That(copy.GroupIds, Is.EqualTo(new[] { "g1", "g2" }));
            Assert.That(copy.Operation, Is.EqualTo(LatticeOperation.Write));
            Assert.That(copy.Scope.KeyOrPrefix, Is.EqualTo("orders/1"));
            Assert.That(copy.Allowed, Is.True);
            Assert.That(copy.Filtered, Is.False);
            Assert.That(copy.Reason, Is.EqualTo("matched rule r1"));
            Assert.That(copy.DefaultEffect, Is.EqualTo(LatticeEffect.Deny));
            Assert.That(copy.MatchedRules.Select(r => r.RuleId), Is.EqualTo(new[] { "r1" }));
        });
    }

    [Test]
    public void AuthEffectivePermissions_round_trips()
    {
        var original = new AuthEffectivePermissions
        {
            SubjectId = "u1",
            GroupIds = ["g1"],
            Rules = [SampleRule("r1"), SampleRule("r2", "tree-b")],
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.SubjectId, Is.EqualTo("u1"));
            Assert.That(copy.GroupIds, Is.EqualTo(new[] { "g1" }));
            Assert.That(copy.Rules.Select(r => r.RuleId), Is.EqualTo(new[] { "r1", "r2" }));
        });
    }

    [Test]
    public void AuthEffectivePermissions_round_trips_a_cluster_wide_telemetry_rule()
    {
        var telemetryRule = new LatticeAuthorizationRule(
            "r-telemetry",
            LatticeSubjectSelector.User("u1"),
            LatticeScope.ClusterWide(),
            LatticeOperation.Telemetry,
            LatticeEffect.Allow);
        var original = new AuthEffectivePermissions
        {
            SubjectId = "u1",
            Rules = [telemetryRule],
        };

        var copy = RoundTrip(original);
        var copied = copy.Rules.Single();
        Assert.Multiple(() =>
        {
            Assert.That(copied.Scope.TreeId, Is.EqualTo(LatticeScope.ClusterWideTreeId));
            Assert.That(copied.Scope.Kind, Is.EqualTo(LatticeScopeKind.Tree));
            Assert.That(copied.Operations, Is.EqualTo(LatticeOperation.Telemetry));
        });
    }

    [Test]
    public void DirectorySearchRequest_round_trips()
    {
        var original = new DirectorySearchRequest
        {
            Term = "ali",
            Kind = DirectoryPrincipalKind.Group,
            PageSize = 50,
            ContinuationToken = "cursor-1",
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void DirectoryPrincipalDescriptor_round_trips_with_claims()
    {
        var original = new DirectoryPrincipalDescriptor
        {
            Id = "u-1",
            DisplayName = "Alice",
            Kind = DirectoryPrincipalKind.User,
            Claims = new Dictionary<string, string> { ["team"] = "ops" },
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Id, Is.EqualTo("u-1"));
            Assert.That(copy.DisplayName, Is.EqualTo("Alice"));
            Assert.That(copy.Kind, Is.EqualTo(DirectoryPrincipalKind.User));
            Assert.That(copy.Claims!["team"], Is.EqualTo("ops"));
        });
    }

    [Test]
    public void DirectorySearchResult_round_trips()
    {
        var original = new DirectorySearchResult
        {
            Principals =
            [
                new DirectoryPrincipalDescriptor { Id = "u-1", DisplayName = "Alice", Kind = DirectoryPrincipalKind.User },
                new DirectoryPrincipalDescriptor { Id = "g-1", DisplayName = "Admins", Kind = DirectoryPrincipalKind.Group },
            ],
            ContinuationToken = "cursor-2",
            Available = true,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Available, Is.True);
            Assert.That(copy.ContinuationToken, Is.EqualTo("cursor-2"));
            Assert.That(copy.Principals.Select(p => p.Id), Is.EqualTo(new[] { "u-1", "g-1" }));
            Assert.That(copy.Principals[1].Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
        });
    }

    [Test]
    public void DirectorySearchResult_unavailable_round_trips_empty()
    {
        var copy = RoundTrip(DirectorySearchResult.Unavailable);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Available, Is.False);
            Assert.That(copy.Principals, Is.Empty);
            Assert.That(copy.ContinuationToken, Is.Null);
        });
    }

    [Test]
    public void AccessModelDescriptor_round_trips()
    {
        var original = new AccessModelDescriptor
        {
            AuthenticationMode = AccessAuthenticationMode.Claims,
            RulesEnforced = true,
            DirectoryAvailable = true,
            DirectoryProviderId = "entra",
            DirectoryExplanation = "Enter an Entra object id.",
            LocalMembershipEffective = true,
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }
}
