using Grpc.Core;
using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tests.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// The Access plugin's own access gate. It reproduces the coarse auth-admin gate
/// the area used to read from the shared capability record - including its
/// distinction between a genuine denial and an unauthenticated connection, which
/// is now the four-state model's
/// <see cref="ExplorerPluginAccessState.AuthenticationRequired"/> - plus the
/// directory-availability sub-capability, now a scoped key.
/// </summary>
[TestFixture]
public class AuthAdminCapabilityServiceTests
{
    private static readonly IExplorerPluginHostContext Context =
        PluginTestHost.Context(AccessPluginKeys.PluginId);

    private static (AuthAdminCapabilityService Service, ExplorerPluginAccessStore Store) Create(
        FakeAuthAdminClient client,
        bool signedIn = true)
    {
        var store = new ExplorerPluginAccessStore();
        return (new AuthAdminCapabilityService(client, store, SignedIn(signedIn)), store);
    }

    private static IExplorerAuthSession SignedIn(bool value)
    {
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(value);
        return session;
    }

    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(
            () => new AuthAdminCapabilityService(null!, new ExplorerPluginAccessStore(), SignedIn(true)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_store_throws()
    {
        Assert.That(
            () => new AuthAdminCapabilityService(new FakeAuthAdminClient(), null!, SignedIn(true)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_session_throws()
    {
        Assert.That(
            () => new AuthAdminCapabilityService(new FakeAuthAdminClient(), new ExplorerPluginAccessStore(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ProbeAsync_null_context_throws()
    {
        var (service, _) = Create(new FakeAuthAdminClient());

        Assert.That(async () => await service.ProbeAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task ProbeAsync_probe_reachable_allows_the_plugin()
    {
        var client = new FakeAuthAdminClient { GroupsResult = new AuthGroupPage() };
        var (service, _) = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(client.LastGroupsRequest!.PageSize, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ProbeAsync_denied_while_signed_in_stays_an_advisory_deny()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new LatticeAuthorizationDeniedException("denied"),
        };
        var (service, _) = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(
                access.State,
                Is.EqualTo(ExplorerPluginAccessState.Denied),
                "an authenticated caller denied admin must stay an advisory deny, not a sign-in prompt");
            Assert.That(access.IsVisible, Is.True, "a denial greys out rather than hides");
        });
    }

    [Test]
    public async Task ProbeAsync_denied_while_signed_out_maps_to_authentication_required()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new LatticeAuthorizationDeniedException("denied"),
        };
        var (service, _) = Create(client, signedIn: false);

        var access = await service.ProbeAsync(Context);

        Assert.That(
            access.State,
            Is.EqualTo(ExplorerPluginAccessState.AuthenticationRequired),
            "a denial while no sign-in is applied means the connection is anonymous, so a sign-in is required");
    }

    [Test]
    public async Task ProbeAsync_unauthenticated_status_maps_to_authentication_required()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new RpcException(new Status(StatusCode.Unauthenticated, "no token")),
        };
        var (service, _) = Create(client, signedIn: true);

        var access = await service.ProbeAsync(Context);

        Assert.That(
            access.State,
            Is.EqualTo(ExplorerPluginAccessState.AuthenticationRequired),
            "gRPC Unauthenticated is an unambiguous unauthenticated-connection signal");
    }

    [Test]
    public async Task ProbeAsync_permission_denied_while_signed_out_maps_to_authentication_required()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new RpcException(new Status(StatusCode.PermissionDenied, "access denied")),
        };
        var (service, _) = Create(client, signedIn: false);

        var access = await service.ProbeAsync(Context);

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.AuthenticationRequired));
    }

    [Test]
    public async Task ProbeAsync_permission_denied_while_signed_in_stays_advisory_deny()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new RpcException(new Status(StatusCode.PermissionDenied, "access denied")),
        };
        var (service, _) = Create(client, signedIn: true);

        var access = await service.ProbeAsync(Context);

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
    }

    [Test]
    public async Task A_reachable_re_probe_clears_a_previously_required_sign_in()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new RpcException(new Status(StatusCode.Unauthenticated, "no token")),
        };
        var (service, _) = Create(client, signedIn: true);
        var before = await service.ProbeAsync(Context);

        client.ListThrows = null;
        client.GroupsResult = new AuthGroupPage();
        var after = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(before.State, Is.EqualTo(ExplorerPluginAccessState.AuthenticationRequired));
            Assert.That(after.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
        });
    }

    [Test]
    public async Task ProbeAsync_transport_failure_denies_without_prompting_a_sign_in()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };
        var (service, _) = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.That(
            access.State,
            Is.EqualTo(ExplorerPluginAccessState.Denied),
            "a transport failure is not an authentication signal");
    }

    [Test]
    public async Task ProbeAsync_unconfigured_session_denies_the_plugin()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new InvalidOperationException("explorer is not configured with an endpoint"),
        };
        var (service, _) = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
    }

    [Test]
    public async Task ProbeAsync_never_touches_another_plugins_keys()
    {
        var client = new FakeAuthAdminClient { GroupsResult = new AuthGroupPage() };
        var (service, store) = Create(client);
        store.Set("some.other.plugin", ExplorerPluginAccess.Allowed);

        await service.ProbeAsync(Context);

        Assert.That(
            store.Get("some.other.plugin").IsAllowed,
            Is.True,
            "the keyed store is what stops one plugin's probe overwriting another's decision");
    }

    [Test]
    public async Task ProbeAsync_reports_directory_availability_and_auth_mode()
    {
        var client = new FakeAuthAdminClient
        {
            GroupsResult = new AuthGroupPage(),
            AccessModelResult = new AccessModelDescriptor
            {
                AuthenticationMode = AccessAuthenticationMode.Claims,
                DirectoryAvailable = true,
                DirectoryProviderId = "entra",
                DirectoryExplanation = "Use the object id.",
            },
        };
        var (service, store) = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(Directory(store), Is.True);
            Assert.That(service.AuthenticationMode, Is.EqualTo(ExplorerAccessAuthenticationMode.Claims));
        });
    }

    [Test]
    public async Task ProbeAsync_maps_basic_auth_mode()
    {
        var client = new FakeAuthAdminClient
        {
            AccessModelResult = new AccessModelDescriptor
            {
                AuthenticationMode = AccessAuthenticationMode.Basic,
                DirectoryAvailable = false,
                DirectoryProviderId = "null",
                DirectoryExplanation = string.Empty,
            },
        };
        var (service, store) = Create(client);

        await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(service.AuthenticationMode, Is.EqualTo(ExplorerAccessAuthenticationMode.Basic));
            Assert.That(Directory(store), Is.False);
        });
    }

    [Test]
    public async Task ProbeAsync_maps_anonymous_auth_mode()
    {
        var client = new FakeAuthAdminClient
        {
            AccessModelResult = new AccessModelDescriptor
            {
                AuthenticationMode = AccessAuthenticationMode.Anonymous,
                DirectoryAvailable = false,
                DirectoryProviderId = "null",
                DirectoryExplanation = string.Empty,
            },
        };
        var (service, _) = Create(client);

        await service.ProbeAsync(Context);

        Assert.That(service.AuthenticationMode, Is.EqualTo(ExplorerAccessAuthenticationMode.Anonymous));
    }

    [Test]
    public void AuthenticationMode_is_unknown_before_any_probe()
    {
        var (service, _) = Create(new FakeAuthAdminClient());

        Assert.That(service.AuthenticationMode, Is.EqualTo(ExplorerAccessAuthenticationMode.Unknown));
    }

    [Test]
    public async Task ProbeAsync_denied_skips_the_access_model_probe_and_reports_unknown()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new LatticeAuthorizationDeniedException("denied"),
        };
        var (service, store) = Create(client);

        await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(Directory(store), Is.False);
            Assert.That(service.AuthenticationMode, Is.EqualTo(ExplorerAccessAuthenticationMode.Unknown));
            Assert.That(
                client.GetAccessModelCallCount,
                Is.EqualTo(0),
                "a denied caller must not trigger a second admin probe");
        });
    }

    [Test]
    public async Task ProbeAsync_access_model_probe_failure_yields_safe_snapshot()
    {
        var client = new FakeAuthAdminClient
        {
            GroupsResult = new AuthGroupPage(),
            AccessModelThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };
        var (service, store) = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed), "the coarse gate still passed");
            Assert.That(Directory(store), Is.False);
            Assert.That(service.AuthenticationMode, Is.EqualTo(ExplorerAccessAuthenticationMode.Unknown));
        });
    }

    private static bool Directory(ExplorerPluginAccessStore store) =>
        store.Get(AccessPluginKeys.PluginId, AccessPluginKeys.DirectoryScope).IsAllowed;
}
