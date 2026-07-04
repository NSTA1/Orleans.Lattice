using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;
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
}
