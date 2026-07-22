using Grpc.Core;
using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Access;

[TestFixture]
public class AuthAdminCapabilityServiceTests
{
    private static (AuthAdminCapabilityService Service, ExplorerCapabilityStore Store) Create(
        FakeAuthAdminClient client,
        bool signedIn = true)
    {
        var store = new ExplorerCapabilityStore();
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(signedIn);
        return (new AuthAdminCapabilityService(client, store, session), store);
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
            () => new AuthAdminCapabilityService(null!, new ExplorerCapabilityStore(), SignedIn(true)),
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
            () => new AuthAdminCapabilityService(new FakeAuthAdminClient(), new ExplorerCapabilityStore(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task RefreshAsync_probe_reachable_sets_coarse_allowed()
    {
        var client = new FakeAuthAdminClient { GroupsResult = new AuthGroupPage() };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.AuthAdminAllowed, Is.True);
            Assert.That(client.LastGroupsRequest!.PageSize, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RefreshAsync_denied_leaves_coarse_disabled()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new LatticeAuthorizationDeniedException("denied"),
        };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.AuthAdminAllowed, Is.False);
            Assert.That(
                store.Current.AuthAdminAuthenticationRequired,
                Is.False,
                "an authenticated caller denied admin must stay an advisory deny, not a sign-in prompt");
        });
    }

    [Test]
    public async Task RefreshAsync_denied_while_signed_out_maps_to_authentication_required()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new LatticeAuthorizationDeniedException("denied"),
        };
        var (service, store) = Create(client, signedIn: false);

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.AuthAdminAllowed, Is.False);
            Assert.That(
                store.Current.AuthAdminAuthenticationRequired,
                Is.True,
                "a denial while no sign-in is applied means the connection is anonymous, so a sign-in is required");
        });
    }

    [Test]
    public async Task RefreshAsync_unauthenticated_status_maps_to_authentication_required()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new RpcException(new Status(StatusCode.Unauthenticated, "no token")),
        };
        var (service, store) = Create(client, signedIn: true);

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.AuthAdminAllowed, Is.False);
            Assert.That(
                store.Current.AuthAdminAuthenticationRequired,
                Is.True,
                "gRPC Unauthenticated is an unambiguous unauthenticated-connection signal");
        });
    }

    [Test]
    public async Task RefreshAsync_permission_denied_while_signed_out_maps_to_authentication_required()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new RpcException(new Status(StatusCode.PermissionDenied, "access denied")),
        };
        var (service, store) = Create(client, signedIn: false);

        await service.RefreshAsync();

        Assert.That(store.Current.AuthAdminAuthenticationRequired, Is.True);
    }

    [Test]
    public async Task RefreshAsync_permission_denied_while_signed_in_stays_advisory_deny()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new RpcException(new Status(StatusCode.PermissionDenied, "access denied")),
        };
        var (service, store) = Create(client, signedIn: true);

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.AuthAdminAllowed, Is.False);
            Assert.That(store.Current.AuthAdminAuthenticationRequired, Is.False);
        });
    }

    [Test]
    public async Task RefreshAsync_reachable_clears_authentication_required()
    {
        var store = new ExplorerCapabilityStore();
        store.Set(ExplorerCapabilities.Empty with { AuthAdminAuthenticationRequired = true });
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(true);
        var service = new AuthAdminCapabilityService(
            new FakeAuthAdminClient { GroupsResult = new AuthGroupPage() }, store, session);

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.AuthAdminAllowed, Is.True);
            Assert.That(store.Current.AuthAdminAuthenticationRequired, Is.False);
        });
    }

    [Test]
    public async Task RefreshAsync_transport_failure_leaves_coarse_disabled()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.AuthAdminAllowed, Is.False);
            Assert.That(
                store.Current.AuthAdminAuthenticationRequired,
                Is.False,
                "a transport failure is not an authentication signal");
        });
    }

    [Test]
    public async Task RefreshAsync_unconfigured_session_leaves_coarse_disabled()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new InvalidOperationException("explorer is not configured with an endpoint"),
        };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.That(store.Current.AuthAdminAllowed, Is.False);
    }

    [Test]
    public async Task RefreshAsync_preserves_other_capability_fields()
    {
        var client = new FakeAuthAdminClient();
        var store = new ExplorerCapabilityStore();
        store.Set(ExplorerCapabilities.Empty with { BackupListAllowed = true });
        var service = new AuthAdminCapabilityService(client, store, SignedIn(true));

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.AuthAdminAllowed, Is.True);
            Assert.That(store.Current.BackupListAllowed, Is.True);
        });
    }

    [Test]
    public async Task RefreshAsync_reports_directory_availability_and_auth_mode()
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

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.AuthAdminAllowed, Is.True);
            Assert.That(store.Current.AuthDirectoryAvailable, Is.True);
            Assert.That(store.Current.AuthAuthenticationMode, Is.EqualTo(ExplorerAccessAuthenticationMode.Claims));
        });
    }

    [Test]
    public async Task RefreshAsync_maps_basic_auth_mode()
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

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.AuthAuthenticationMode, Is.EqualTo(ExplorerAccessAuthenticationMode.Basic));
            Assert.That(store.Current.AuthDirectoryAvailable, Is.False);
        });
    }

    [Test]
    public async Task RefreshAsync_denied_skips_the_access_model_probe_and_reports_unknown()
    {
        var client = new FakeAuthAdminClient
        {
            ListThrows = new LatticeAuthorizationDeniedException("denied"),
        };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.AuthAdminAllowed, Is.False);
            Assert.That(store.Current.AuthDirectoryAvailable, Is.False);
            Assert.That(store.Current.AuthAuthenticationMode, Is.EqualTo(ExplorerAccessAuthenticationMode.Unknown));
            Assert.That(client.GetAccessModelCallCount, Is.EqualTo(0), "a denied caller must not trigger a second admin probe");
        });
    }

    [Test]
    public async Task RefreshAsync_access_model_probe_failure_yields_safe_snapshot()
    {
        var client = new FakeAuthAdminClient
        {
            GroupsResult = new AuthGroupPage(),
            AccessModelThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.AuthAdminAllowed, Is.True, "the coarse gate still passed");
            Assert.That(store.Current.AuthDirectoryAvailable, Is.False);
            Assert.That(store.Current.AuthAuthenticationMode, Is.EqualTo(ExplorerAccessAuthenticationMode.Unknown));
        });
    }
}
