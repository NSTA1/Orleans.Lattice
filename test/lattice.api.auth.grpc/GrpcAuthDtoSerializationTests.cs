using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;
using Orleans.Serialization;
using GrpcSerializationContext = Grpc.Core.SerializationContext;
using GrpcDeserializationContext = Grpc.Core.DeserializationContext;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// Round-trips the gRPC-layer wire DTOs (the <c>Model</c> request / response
/// records the binding marshals with the Orleans serializer) to prove the
/// transport contract is coherent and stable across the wire, and round-trips a
/// message through the actual <see cref="LatticeAuthApiGrpcMarshallers"/> to prove
/// the contextual serialize / deserialize hand-off the gRPC stream uses is
/// byte-faithful. The transport-agnostic facade DTOs (users, groups, pages,
/// explanation, and effective permissions) are covered in the
/// <c>Orleans.Lattice.Api.Auth</c> test project.
/// </summary>
[TestFixture]
public sealed class GrpcAuthDtoSerializationTests
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

    private T MarshalRoundTrip<T>(T value)
        where T : class
    {
        var marshaller = LatticeAuthApiGrpcMarshallers.Create(_services.GetRequiredService<Serializer<T>>());

        var serializationContext = new FakeSerializationContext();
        marshaller.ContextualSerializer(value, serializationContext);

        var deserializationContext = new FakeDeserializationContext(serializationContext.ToArray());
        return marshaller.ContextualDeserializer(deserializationContext);
    }

    [Test]
    public void AuthUserRef_round_trips()
    {
        var original = new AuthUserRef { UserId = "alice" };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void AuthGroupRef_round_trips()
    {
        var original = new AuthGroupRef { GroupId = "admins" };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void AuthMemberRef_round_trips()
    {
        var original = new AuthMemberRef { MemberId = "bob" };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void AuthRuleRef_round_trips()
    {
        var original = new AuthRuleRef { TreeId = "policy-tree", RuleId = "r1" };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void AuthMemberEdge_round_trips()
    {
        var original = new AuthMemberEdge
        {
            GroupId = "admins",
            MemberId = "bob",
            MemberKind = Membership.MembershipMemberKind.Group,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.GroupId, Is.EqualTo("admins"));
            Assert.That(copy.MemberId, Is.EqualTo("bob"));
            Assert.That(copy.MemberKind, Is.EqualTo(Membership.MembershipMemberKind.Group));
        });
    }

    [Test]
    public void AuthPutRule_round_trips_the_rule()
    {
        var original = new AuthPutRule
        {
            Rule = new LatticeAuthorizationRule(
                "r1",
                LatticeSubjectSelector.User("alice"),
                LatticeScope.Tree("policy-tree"),
                LatticeOperation.Read,
                LatticeEffect.Allow),
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Rule.RuleId, Is.EqualTo("r1"));
            Assert.That(copy.Rule.Scope.TreeId, Is.EqualTo("policy-tree"));
            Assert.That(copy.Rule.Effect, Is.EqualTo(LatticeEffect.Allow));
        });
    }

    [Test]
    public void AuthPutRule_round_trips_a_cluster_wide_telemetry_grant()
    {
        var original = new AuthPutRule
        {
            Rule = new LatticeAuthorizationRule(
                "r-telemetry",
                LatticeSubjectSelector.User("observer"),
                LatticeScope.ClusterWide(),
                LatticeOperation.Telemetry,
                LatticeEffect.Allow),
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Rule.RuleId, Is.EqualTo("r-telemetry"));
            Assert.That(copy.Rule.Scope.Kind, Is.EqualTo(LatticeScopeKind.Tree));
            Assert.That(copy.Rule.Scope.TreeId, Is.EqualTo(LatticeScope.ClusterWideTreeId));
            Assert.That(copy.Rule.Operations, Is.EqualTo(LatticeOperation.Telemetry));
            Assert.That(copy.Rule.Effect, Is.EqualTo(LatticeEffect.Allow));
        });
    }

    [Test]
    public void AuthTreeRulesPage_round_trips()
    {
        var original = new AuthTreeRulesPage
        {
            TreeId = "policy-tree",
            Page = new AuthPageRequest { PageSize = 25, PageToken = "tok" },
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("policy-tree"));
            Assert.That(copy.Page.PageSize, Is.EqualTo(25));
            Assert.That(copy.Page.PageToken, Is.EqualTo("tok"));
        });
    }

    [Test]
    public void AuthExplainQuery_round_trips()
    {
        var original = new AuthExplainQuery
        {
            SubjectId = "alice",
            Operation = LatticeOperation.Write,
            Scope = LatticeScope.Prefix("policy-tree", "a/"),
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.SubjectId, Is.EqualTo("alice"));
            Assert.That(copy.Operation, Is.EqualTo(LatticeOperation.Write));
            Assert.That(copy.Scope.TreeId, Is.EqualTo("policy-tree"));
            Assert.That(copy.Scope.KeyOrPrefix, Is.EqualTo("a/"));
        });
    }

    [Test]
    public void AuthSubjectRef_round_trips()
    {
        var original = new AuthSubjectRef { SubjectId = "alice" };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void AuthAck_round_trips()
    {
        Assert.That(RoundTrip(new AuthAck()), Is.EqualTo(new AuthAck()));
    }

    [Test]
    public void AuthUserResult_round_trips_present_and_absent()
    {
        var present = RoundTrip(new AuthUserResult { User = new AuthUser { UserId = "alice", DisplayName = "Alice" } });
        var absent = RoundTrip(new AuthUserResult { User = null });

        Assert.Multiple(() =>
        {
            Assert.That(present.User!.UserId, Is.EqualTo("alice"));
            Assert.That(absent.User, Is.Null);
        });
    }

    [Test]
    public void AuthGroupResult_round_trips_present_and_absent()
    {
        var present = RoundTrip(new AuthGroupResult { Group = new AuthGroup { GroupId = "admins" } });
        var absent = RoundTrip(new AuthGroupResult { Group = null });

        Assert.Multiple(() =>
        {
            Assert.That(present.Group!.GroupId, Is.EqualTo("admins"));
            Assert.That(absent.Group, Is.Null);
        });
    }

    [Test]
    public void AuthRuleResult_round_trips_present_and_absent()
    {
        var rule = new LatticeAuthorizationRule(
            "r1", LatticeSubjectSelector.User("u"), LatticeScope.Tree("t"), LatticeOperation.Read, LatticeEffect.Allow);
        var present = RoundTrip(new AuthRuleResult { Rule = rule });
        var absent = RoundTrip(new AuthRuleResult { Rule = null });

        Assert.Multiple(() =>
        {
            Assert.That(present.Rule!.RuleId, Is.EqualTo("r1"));
            Assert.That(absent.Rule, Is.Null);
        });
    }

    [Test]
    public void AuthStringList_round_trips()
    {
        var original = new AuthStringList { Values = new[] { "a", "b", "c" } };

        var copy = RoundTrip(original);
        Assert.That(copy.Values, Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public void AuthRuleRemoved_round_trips()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RoundTrip(new AuthRuleRemoved { Removed = true }).Removed, Is.True);
            Assert.That(RoundTrip(new AuthRuleRemoved { Removed = false }).Removed, Is.False);
        });
    }

    [Test]
    public void AuthPrincipalRef_round_trips()
    {
        var original = new AuthPrincipalRef { PrincipalId = "alice@contoso.com" };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void AuthDirectoryPrincipalResult_round_trips_present_and_absent()
    {
        var present = RoundTrip(new AuthDirectoryPrincipalResult
        {
            Principal = new DirectoryPrincipalDescriptor
            {
                Id = "alice@contoso.com",
                DisplayName = "Alice",
                Kind = Membership.DirectoryPrincipalKind.User,
            },
        });
        var absent = RoundTrip(new AuthDirectoryPrincipalResult { Principal = null });

        Assert.Multiple(() =>
        {
            Assert.That(present.Principal!.Id, Is.EqualTo("alice@contoso.com"));
            Assert.That(present.Principal!.DisplayName, Is.EqualTo("Alice"));
            Assert.That(absent.Principal, Is.Null);
        });
    }

    [Test]
    public void AuthAccessModelQuery_round_trips()
    {
        Assert.That(RoundTrip(new AuthAccessModelQuery()), Is.EqualTo(new AuthAccessModelQuery()));
    }

    [Test]
    public void DirectorySearchRequest_round_trips()
    {
        var original = new DirectorySearchRequest
        {
            Term = "al",
            Kind = Membership.DirectoryPrincipalKind.Group,
            PageSize = 25,
            ContinuationToken = "tok",
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Term, Is.EqualTo("al"));
            Assert.That(copy.Kind, Is.EqualTo(Membership.DirectoryPrincipalKind.Group));
            Assert.That(copy.PageSize, Is.EqualTo(25));
            Assert.That(copy.ContinuationToken, Is.EqualTo("tok"));
        });
    }

    [Test]
    public void DirectorySearchResult_round_trips_a_populated_page()
    {
        var original = new DirectorySearchResult
        {
            Principals = new[]
            {
                new DirectoryPrincipalDescriptor { Id = "alice", DisplayName = "Alice" },
            },
            ContinuationToken = "next",
            Available = true,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Principals, Has.Count.EqualTo(1));
            Assert.That(copy.Principals[0].Id, Is.EqualTo("alice"));
            Assert.That(copy.ContinuationToken, Is.EqualTo("next"));
            Assert.That(copy.Available, Is.True);
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
            DirectoryExplanation = "Use the object id.",
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.AuthenticationMode, Is.EqualTo(AccessAuthenticationMode.Claims));
            Assert.That(copy.RulesEnforced, Is.True);
            Assert.That(copy.DirectoryAvailable, Is.True);
            Assert.That(copy.DirectoryProviderId, Is.EqualTo("entra"));
            Assert.That(copy.DirectoryExplanation, Is.EqualTo("Use the object id."));
        });
    }

    [Test]
    public void Marshaller_round_trips_a_directory_search_result_through_the_grpc_contexts()
    {
        var original = new DirectorySearchResult
        {
            Principals = new[] { new DirectoryPrincipalDescriptor { Id = "g1", DisplayName = "Group 1" } },
            Available = true,
        };

        var copy = MarshalRoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Available, Is.True);
            Assert.That(copy.Principals[0].Id, Is.EqualTo("g1"));
        });
    }

    [Test]
    public void Marshaller_round_trips_a_put_rule_through_the_grpc_contexts()
    {
        var original = new AuthPutRule
        {
            Rule = new LatticeAuthorizationRule(
                "r1",
                LatticeSubjectSelector.User("alice"),
                LatticeScope.Tree("policy-tree"),
                LatticeOperation.Read,
                LatticeEffect.Allow),
        };

        var copy = MarshalRoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Rule.RuleId, Is.EqualTo("r1"));
            Assert.That(copy.Rule.Scope.TreeId, Is.EqualTo("policy-tree"));
        });
    }

    [Test]
    public void Marshaller_round_trips_a_string_list_through_the_grpc_contexts()
    {
        var original = new AuthStringList { Values = new[] { "x", "y" } };

        var copy = MarshalRoundTrip(original);
        Assert.That(copy.Values, Is.EqualTo(new[] { "x", "y" }));
    }

    /// <summary>
    /// Minimal <see cref="GrpcSerializationContext"/> that captures the encoded
    /// bytes written through the buffer-writer hand-off the marshaller uses.
    /// </summary>
    private sealed class FakeSerializationContext : GrpcSerializationContext
    {
        private readonly ArrayBufferWriter<byte> _writer = new();

        public override IBufferWriter<byte> GetBufferWriter() => _writer;

        public override void Complete()
        {
        }

        public override void Complete(byte[] payload) => _writer.Write(payload);

        public override void SetPayloadLength(int payloadLength)
        {
        }

        public byte[] ToArray() => _writer.WrittenSpan.ToArray();
    }

    /// <summary>
    /// Minimal <see cref="GrpcDeserializationContext"/> that presents a fixed
    /// payload as the read-only sequence the marshaller decodes.
    /// </summary>
    private sealed class FakeDeserializationContext(byte[] payload) : GrpcDeserializationContext
    {
        public override int PayloadLength => payload.Length;

        public override ReadOnlySequence<byte> PayloadAsReadOnlySequence() => new(payload);

        public override byte[] PayloadAsNewBuffer() => (byte[])payload.Clone();
    }
}
