using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for the identity-directory and access-model block of
/// <see cref="GrpcLatticeAuthAdmin"/> - the three members added by issues
/// #1248 / #1249 that ride the same wrap-the-argument / unwrap-the-envelope
/// convention as the membership and policy members covered by
/// <see cref="GrpcLatticeAuthAdminTests"/>.
/// </summary>
/// <remarks>
/// These members back the Explorer Access create form's principal picker and its
/// "what is a valid id here" explanation, so a silently broken adapter would
/// present an operator with an empty directory that looks like "nothing matched"
/// rather than "the call never reached the cluster". They are exercised here
/// against the same deterministic <see cref="FakeCallInvoker"/> the sibling
/// fixture uses.
/// </remarks>
[TestFixture]
public sealed class GrpcLatticeAuthAdminDirectoryTests
{
    private static GrpcLatticeAuthAdmin Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.AuthClient(invoker));

    private static DirectoryPrincipalDescriptor Principal(string id = "alice")
        => new() { Id = id, DisplayName = id.ToUpperInvariant(), Kind = DirectoryPrincipalKind.User };

    [Test]
    public async Task SearchDirectoryAsync_forwards_request_and_returns_page()
    {
        var page = new DirectorySearchResult
        {
            Principals = new[] { Principal() },
            ContinuationToken = "next",
            Available = true,
        };
        var invoker = new FakeCallInvoker(_ => page);
        var request = new DirectorySearchRequest { Term = "ali", PageSize = 25, Kind = DirectoryPrincipalKind.User };

        var result = await Adapter(invoker).SearchDirectoryAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastRequest, Is.SameAs(request), "the request record rides the wire unwrapped");
            Assert.That(result, Is.SameAs(page));
            Assert.That(invoker.CallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task SearchDirectoryAsync_propagates_unavailable_directory()
    {
        var invoker = new FakeCallInvoker(_ => DirectorySearchResult.Unavailable);

        var result = await Adapter(invoker).SearchDirectoryAsync(new DirectorySearchRequest());

        Assert.Multiple(() =>
        {
            Assert.That(result.Available, Is.False, "'no directory configured' must not be flattened to 'nothing matched'");
            Assert.That(result.Principals, Is.Empty);
        });
    }

    [Test]
    public async Task ResolveDirectoryPrincipalAsync_wraps_id_and_unwraps_principal()
    {
        var principal = Principal("bob");
        var invoker = new FakeCallInvoker(_ => new AuthDirectoryPrincipalResult { Principal = principal });

        var result = await Adapter(invoker).ResolveDirectoryPrincipalAsync("bob");

        Assert.Multiple(() =>
        {
            Assert.That(((AuthPrincipalRef)invoker.LastRequest!).PrincipalId, Is.EqualTo("bob"));
            Assert.That(result, Is.SameAs(principal));
        });
    }

    [Test]
    public async Task ResolveDirectoryPrincipalAsync_missing_returns_null()
    {
        var invoker = new FakeCallInvoker(_ => new AuthDirectoryPrincipalResult { Principal = null });

        var result = await Adapter(invoker).ResolveDirectoryPrincipalAsync("ghost");

        Assert.Multiple(() =>
        {
            Assert.That(((AuthPrincipalRef)invoker.LastRequest!).PrincipalId, Is.EqualTo("ghost"));
            Assert.That(result, Is.Null, "the non-null envelope must be unwrapped back to the facade's nullable shape");
        });
    }

    [Test]
    public async Task GetAccessModelAsync_sends_empty_query_and_returns_descriptor()
    {
        var descriptor = new AccessModelDescriptor
        {
            AuthenticationMode = AccessAuthenticationMode.Claims,
            RulesEnforced = true,
            DirectoryAvailable = true,
            DirectoryProviderId = "entra",
            DirectoryExplanation = "Enter an Entra object id.",
        };
        var invoker = new FakeCallInvoker(_ => descriptor);

        var result = await Adapter(invoker).GetAccessModelAsync();

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastRequest, Is.InstanceOf<AuthAccessModelQuery>(),
                "the no-argument facade call must still carry the empty wire envelope");
            Assert.That(result, Is.SameAs(descriptor));
        });
    }

    [Test]
    public void GetAccessModelAsync_propagates_transport_failure()
    {
        var invoker = new FakeCallInvoker(_ => new InvalidOperationException("transport down"));

        Assert.That(
            async () => await Adapter(invoker).GetAccessModelAsync(),
            Throws.InvalidOperationException.With.Message.EqualTo("transport down"));
    }

    [Test]
    public async Task Directory_members_honour_the_supplied_cancellation_token()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var adapter = Adapter(new FakeCallInvoker(_ => DirectorySearchResult.Unavailable));

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await adapter.SearchDirectoryAsync(new DirectorySearchRequest(), cts.Token),
                Throws.InstanceOf<OperationCanceledException>());
            Assert.That(
                async () => await adapter.ResolveDirectoryPrincipalAsync("alice", cts.Token),
                Throws.InstanceOf<OperationCanceledException>());
            Assert.That(
                async () => await adapter.GetAccessModelAsync(cts.Token),
                Throws.InstanceOf<OperationCanceledException>());
        });
    }
}
