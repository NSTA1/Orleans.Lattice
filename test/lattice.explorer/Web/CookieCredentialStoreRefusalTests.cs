using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Http;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.Tests.Web;

/// <summary>
/// What the cookie credential store does with a cookie it will not honour, and
/// how far its revocation ledger reaches.
/// </summary>
/// <remarks>
/// Every refusal below returns <see langword="null"/> - the same answer as "no
/// cookie at all" - so none of them is observable from a round trip through
/// <c>SetAsync</c>/<c>GetAsync</c>, which is the only path the sibling fixtures
/// take. They are reached by presenting a cookie this store did not mint, which
/// is exactly the adversarial case: a tampered payload, a payload that decrypts
/// to something other than a credential, and a credential bound to an endpoint
/// the host cannot confirm.
/// </remarks>
[TestFixture]
public sealed class CookieCredentialStoreRefusalTests
{
    private const string CookieName = "lattice-explorer-cred";

    /// <summary>The Data Protection purpose the store mints its payloads under.</summary>
    private const string Purpose = "Orleans.Lattice.Explorer.Credential.v1";

    [Test]
    public async Task A_request_carrying_no_credential_cookie_has_no_credential()
    {
        var store = Store(out var accessor, out var provider);
        _ = provider;
        accessor.HttpContext.Returns(new DefaultHttpContext());

        Assert.That(await store.GetAsync(), Is.Null);
    }

    [Test]
    public async Task A_request_with_no_context_at_all_has_no_credential()
    {
        var store = Store(out var accessor, out _);
        accessor.HttpContext.Returns((HttpContext?)null);

        Assert.That(await store.GetAsync(), Is.Null);
    }

    [Test]
    public async Task An_empty_cookie_value_has_no_credential()
    {
        var store = Store(out var accessor, out _);
        accessor.HttpContext.Returns(Presenting(string.Empty));

        Assert.That(await store.GetAsync(), Is.Null);
    }

    [Test]
    [TestCase("not-a-protected-payload", TestName = "A_payload_that_is_not_protected_text_is_refused")]
    [TestCase("!!!not base64url!!!", TestName = "A_payload_that_is_not_even_base64url_is_refused")]
    public async Task A_cookie_this_store_did_not_mint_is_refused(string payload)
    {
        // Unprotect throws CryptographicException for a well-formed payload under a
        // different key and FormatException for one that is not base64url at all;
        // both must read as "no credential" rather than faulting the sign-in.
        var store = Store(out var accessor, out _);
        accessor.HttpContext.Returns(Presenting(payload));

        Assert.That(async () => await store.GetAsync(), Throws.Nothing);
        Assert.That(await store.GetAsync(), Is.Null);
    }

    [Test]
    public async Task A_payload_minted_under_a_different_key_is_refused()
    {
        // The realistic shape of a key-ring roll: the cookie is a genuine protected
        // payload, just not one this store can open.
        var store = Store(out var accessor, out _);
        var foreign = new EphemeralDataProtectionProvider()
            .CreateProtector(Purpose)
            .Protect("""{"Username":"alice","Password":"hunter2"}""");

        accessor.HttpContext.Returns(Presenting(foreign));

        Assert.That(await store.GetAsync(), Is.Null);
    }

    [Test]
    public async Task A_payload_that_decrypts_to_something_that_is_not_json_is_refused()
    {
        var store = Store(out var accessor, out var provider);
        var cookie = provider.CreateProtector(Purpose).Protect("this is not json");

        accessor.HttpContext.Returns(Presenting(cookie));

        Assert.That(await store.GetAsync(), Is.Null);
    }

    [Test]
    public async Task A_legacy_payload_whose_credential_fields_are_the_wrong_shape_is_refused()
    {
        // Reads as an envelope carrying no credential (so it takes the legacy
        // unbound path), then fails to read as a bare credential because Username is
        // a number. A host that binds nothing has nothing else to fall back to.
        var store = Store(out var accessor, out var provider);
        var cookie = provider.CreateProtector(Purpose).Protect("""{"Credential":null,"Username":123}""");

        accessor.HttpContext.Returns(Presenting(cookie));

        Assert.That(await store.GetAsync(), Is.Null);
    }

    [Test]
    public async Task A_bound_credential_is_refused_by_a_host_that_can_resolve_no_endpoint()
    {
        // Fail closed. This host registers no configuration store, so it cannot
        // confirm which endpoint the console is pointed at - and a credential minted
        // for some other endpoint must not be replayed to whoever answers now.
        var store = Store(out var accessor, out var provider);
        var cookie = provider.CreateProtector(Purpose).Protect(
            """{"Endpoint":"https://cluster.example","Credential":{"Username":"alice","Password":"hunter2"}}""");

        accessor.HttpContext.Returns(Presenting(cookie));

        Assert.That(
            await store.GetAsync(),
            Is.Null,
            "an unresolvable endpoint refuses the credential rather than replaying it");
    }

    [Test]
    public async Task A_bound_credential_is_served_for_the_endpoint_it_was_minted_for()
    {
        var accessor = Substitute.For<IHttpContextAccessor>();
        var configStore = Substitute.For<IExplorerConfigStore>();
        configStore.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<ExplorerConfiguration?>(new ExplorerConfiguration { Endpoint = "https://cluster.example" }));

        var provider = new EphemeralDataProtectionProvider();
        var store = new CookieCredentialStore(accessor, provider, configStore);

        var write = new DefaultHttpContext();
        accessor.HttpContext.Returns(write);
        await store.SetAsync(new StoredCredential("alice", "hunter2"));

        accessor.HttpContext.Returns(Presenting(PayloadFrom(write)));

        var credential = await store.GetAsync();

        Assert.That(credential, Is.Not.Null);
        Assert.That(credential!.Username, Is.EqualTo("alice"));
    }

    [Test]
    public async Task Clearing_the_same_cookie_twice_revokes_it_once()
    {
        // The ledger is bounded, so a repeated sign-out of the same value must not
        // consume a second slot - that is what would let a caller flush a genuine
        // revocation by replaying one cookie.
        var store = Store(out var accessor, out _);

        var write = new DefaultHttpContext();
        accessor.HttpContext.Returns(write);
        await store.SetAsync(new StoredCredential("alice", "hunter2"));
        var payload = PayloadFrom(write);

        accessor.HttpContext.Returns(Presenting(payload));
        await store.ClearAsync();
        await store.ClearAsync();

        Assert.That(await store.GetAsync(), Is.Null, "the credential stays revoked");
    }

    [Test]
    public async Task A_cookie_this_store_never_minted_is_not_admitted_to_the_revocation_ledger()
    {
        // /auth/logout needs no sign-in, so the presented value is attacker-chosen.
        // Admitting junk would let an anonymous caller enqueue fabricated values and
        // evict a genuine revocation.
        var store = Store(out var accessor, out _);

        var write = new DefaultHttpContext();
        accessor.HttpContext.Returns(write);
        await store.SetAsync(new StoredCredential("alice", "hunter2"));
        var genuine = PayloadFrom(write);

        accessor.HttpContext.Returns(Presenting(genuine));
        await store.ClearAsync();

        // 2 x MaxRevocations fabricated sign-outs: none is minted here, so none takes
        // a slot and the genuine revocation above survives all of them.
        for (var i = 0; i < 2048; i++)
        {
            accessor.HttpContext.Returns(Presenting($"fabricated-{i}"));
            await store.ClearAsync();
        }

        accessor.HttpContext.Returns(Presenting(genuine));

        Assert.That(await store.GetAsync(), Is.Null, "a fabricated sign-out must not flush the ledger");
    }

    [Test]
    public async Task The_revocation_ledger_evicts_its_oldest_entry_once_it_is_full()
    {
        // The documented bound: eviction is oldest-first and only reaches values
        // revoked more than MaxRevocations revocations ago. Proven behaviourally -
        // the first revoked payload becomes readable again once it is pushed out.
        var store = Store(out var accessor, out _);

        var first = await MintAndRevoke(store, accessor);
        for (var i = 0; i < 1024; i++)
        {
            await MintAndRevoke(store, accessor);
        }

        accessor.HttpContext.Returns(Presenting(first));

        Assert.That(
            await store.GetAsync(),
            Is.Not.Null,
            "the ledger is bounded at 1024, so the oldest revocation is evicted by the 1025th");
    }

    [Test]
    public async Task A_revocation_still_inside_the_bound_is_kept()
    {
        var store = Store(out var accessor, out _);

        var first = await MintAndRevoke(store, accessor);
        for (var i = 0; i < 1023; i++)
        {
            await MintAndRevoke(store, accessor);
        }

        accessor.HttpContext.Returns(Presenting(first));

        Assert.That(
            await store.GetAsync(),
            Is.Null,
            "1024 revocations fit, so the first one is still held");
    }

    private static async Task<string> MintAndRevoke(CookieCredentialStore store, IHttpContextAccessor accessor)
    {
        var write = new DefaultHttpContext();
        accessor.HttpContext.Returns(write);
        await store.SetAsync(new StoredCredential("alice", "hunter2"));
        var payload = PayloadFrom(write);

        accessor.HttpContext.Returns(Presenting(payload));
        await store.ClearAsync();
        return payload;
    }

    private static CookieCredentialStore Store(out IHttpContextAccessor accessor, out IDataProtectionProvider provider)
    {
        accessor = Substitute.For<IHttpContextAccessor>();
        provider = new EphemeralDataProtectionProvider();
        return new CookieCredentialStore(accessor, provider);
    }

    private static string PayloadFrom(HttpContext write)
    {
        var setCookie = write.Response.Headers.SetCookie.ToString();
        return setCookie.Split(';')[0][(CookieName.Length + 1)..];
    }

    /// <summary>A request presenting <paramref name="payload"/> as its credential cookie, with a writable response.</summary>
    private static HttpContext Presenting(string payload)
    {
        var context = new DefaultHttpContext();
        context.Request.Headers.Cookie = $"{CookieName}={payload}";
        return context;
    }
}
