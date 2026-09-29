using System.Collections.Concurrent;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// A token sign-in whose renewal the test controls, so the Explorer's
/// re-authentication interstitial can be raised on demand and without any clock.
/// </summary>
/// <remarks>
/// <para>
/// A token scheme is what re-authentication exists for: a password sign-in never
/// expires under the Explorer, but a token must be renewed, and when renewal is
/// refused Core raises <c>ReauthRequired</c>. The token issued here is already due for
/// renewal, so the Explorer asks for a fresh one on every call; while the user is
/// <see cref="Revoke">revoked</see> the renewal is refused, and the very next call the
/// Explorer makes raises the interstitial.
/// </para>
/// <para>
/// The token is sent as a Basic header naming the user, which is what the test world's
/// authenticator trusts, so the cluster sees the same identity a password sign-in
/// would. The re-authentication challenge path returns the browser to the address it
/// left, the contract a real sign-in provider's challenge endpoint keeps.
/// </para>
/// </remarks>
internal sealed class RenewableSignIn
{
    /// <summary>The token scheme's id.</summary>
    public const string SchemeId = "uitest-token";

    /// <summary>The name the sign-in dialog shows for the scheme.</summary>
    public const string DisplayName = "Test token";

    /// <summary>The head-relative path the interstitial's "Sign in again" goes to.</summary>
    public const string ChallengePath = "/uitest/reauth";

    private readonly ConcurrentDictionary<string, bool> _revoked = new(StringComparer.Ordinal);

    /// <summary>The user a token sign-in signs in as.</summary>
    public string User { get; set; } = WorldIdentities.Admin;

    /// <summary>Refuses every renewal for <paramref name="user"/> until <see cref="Restore"/>.</summary>
    /// <param name="user">The user.</param>
    public void Revoke(string user) => _revoked[user] = true;

    /// <summary>Allows renewals for <paramref name="user"/> again.</summary>
    /// <param name="user">The user.</param>
    public void Restore(string user) => _revoked.TryRemove(user, out _);

    /// <summary>Starts an Explorer head on the world's cluster that offers the token sign-in beside the password one.</summary>
    /// <param name="world">The world whose cluster the head connects to.</param>
    public async Task<ExplorerHead> StartHeadAsync(ExplorerWorld world)
    {
        ArgumentNullException.ThrowIfNull(world);

        return await ExplorerHead.StartAsync(new ExplorerHeadOptions
        {
            Endpoint = world.GrpcEndpoint,
            ConfigureServices = services =>
            {
                services.AddSingleton<IExplorerAuthMethod>(new Method(this));
                services.AddSingleton<IExplorerAuthSchemeProbe>(new Probe());
                services.AddSingleton(new ExplorerReauthOptions { ChallengePath = ChallengePath });
            },
            ConfigureApp = app => app.MapGet(ChallengePath, (HttpContext context) =>
            {
                // A local path only: never an open redirect, even in a test head.
                var target = context.Request.Query[ExplorerReauthOptions.DefaultReturnUrlParameter].ToString();
                var local = target.StartsWith('/') && !target.StartsWith("//", StringComparison.Ordinal) && !target.StartsWith("/\\", StringComparison.Ordinal);
                return Results.Redirect(local ? target : "/");
            }),
        });
    }

    private bool IsRevoked(string user) => _revoked.ContainsKey(user);

    private static ExplorerAccessToken Issue(string user) => new()
    {
        Token = Convert.ToBase64String(Encoding.UTF8.GetBytes(user + ":" + WorldIdentities.Password)),
        Scheme = "Basic",

        // Already due, so every call asks for a renewal: the refusal is observed on the
        // very next call, with no clock involved.
        ExpiresOn = DateTimeOffset.MinValue,
    };

    private sealed class Method(RenewableSignIn owner) : IExplorerAuthMethod
    {
        public string SchemeId => RenewableSignIn.SchemeId;

        public bool CanHandle(string advertisedScheme) =>
            string.Equals(advertisedScheme, SchemeId, StringComparison.OrdinalIgnoreCase);

        public Task<ExplorerAuthSignIn> ChallengeAsync(ExplorerAuthChallengeContext context, CancellationToken cancellationToken = default)
        {
            var user = owner.User;
            var source = new ExplorerAccessTokenSource(
                Issue(user),
                _ => ValueTask.FromResult<ExplorerAccessToken?>(owner.IsRevoked(user) ? null : Issue(user)),
                TimeProvider.System,
                TimeSpan.Zero);

            return Task.FromResult(new ExplorerAuthSignIn
            {
                SchemeId = SchemeId,
                DisplayName = user,
                Authentication = LatticeCallAuthentication.Bearer(source),
            });
        }
    }

    private sealed class Probe : IExplorerAuthSchemeProbe
    {
        private static readonly ExplorerAuthSchemeAdvertisement Advertisement = new()
        {
            Schemes =
            [
                new ExplorerAuthSchemeDescriptor { SchemeId = SchemeId, DisplayName = DisplayName },
                new ExplorerAuthSchemeDescriptor { SchemeId = ExplorerAuthSchemes.Basic },
            ],
        };

        public Task<ExplorerAuthSchemeAdvertisement> ProbeAsync(string address, bool allowUnencryptedHttp2 = false, CancellationToken cancellationToken = default) =>
            Task.FromResult(Advertisement);
    }
}
