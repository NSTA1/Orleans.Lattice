using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

public partial class LatticeStateConnectionTests
{
    [Test]
    public async Task AuthFailure_WithTokenProvider_SilentlyRefreshesThenRetries_AndSucceeds()
    {
        var client = new FakeStateClient();
        var provider = new FakeCredentialProvider { RefreshResult = true };
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings() with
        {
            Authentication = LatticeCallAuthentication.Bearer(provider),
        });

        var attempts = 0;
        client.ListTreesHandler = _ =>
        {
            attempts++;
            return attempts < 2 ? throw Permanent() : Task.FromResult(new TreeCatalogPage());
        };

        var page = await connection.ListTreesAsync(new CatalogRequest());

        Assert.That(page, Is.Not.Null);
        Assert.That(attempts, Is.EqualTo(2), "the call is retried once after a silent refresh");
        Assert.That(provider.RefreshCount, Is.EqualTo(1), "exactly one silent refresh precedes the retry");
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected));
    }

    [Test]
    public async Task AuthFailure_WhenRefreshImpossible_SurfacesAuthFailure_AfterSingleRefresh()
    {
        var client = new FakeStateClient();
        var provider = new FakeCredentialProvider { RefreshResult = false };
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings() with
        {
            Authentication = LatticeCallAuthentication.Bearer(provider),
        });
        client.ListTreesHandler = _ => throw Permanent();

        LatticeStateApiException? captured = null;
        try
        {
            await connection.ListTreesAsync(new CatalogRequest());
        }
        catch (LatticeStateApiException ex)
        {
            captured = ex;
        }

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.RequiresAuthentication, Is.True, "an unrefreshable token re-challenges the user");
        Assert.That(provider.RefreshCount, Is.EqualTo(1), "refresh is attempted exactly once, not retried in a loop");
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Faulted));
    }

    [Test]
    public async Task AuthFailure_WithoutTokenProvider_DoesNotRefresh_AndFaults()
    {
        // A static Basic credential (or anonymous) has no live provider, so the
        // connection must fault immediately on an auth failure exactly as before -
        // the silent-refresh path is token-only and must not change Basic behaviour.
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings() with
        {
            Authentication = LatticeCallAuthentication.Basic("alice", "Password1"),
        });
        client.ListTreesHandler = _ => throw Permanent();

        LatticeStateApiException? captured = null;
        try
        {
            await connection.ListTreesAsync(new CatalogRequest());
        }
        catch (LatticeStateApiException ex)
        {
            captured = ex;
        }

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.RequiresAuthentication, Is.True);
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Faulted));
    }
}
