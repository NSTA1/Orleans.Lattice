using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.State.Grpc;

namespace Orleans.Lattice.Api.State.Grpc.Tests.Security;

/// <summary>
/// Coverage for the gRPC-facing entry point of
/// <see cref="EnvVarCredentialAuthorizer"/> - the
/// <see cref="ILatticeStateApiAuthorizer.IsAuthorizedAsync"/> implementation that
/// lifts the <c>authorization</c> header off the inbound
/// <see cref="LatticeStateApiAuthorizationContext"/> - plus the username-shape
/// guard that keeps a caller-supplied username from being pasted into an
/// environment-variable lookup.
/// </summary>
[TestFixture]
public sealed class EnvVarCredentialAuthorizerContextTests
{
    private const string Username = "alice";
    private const string Password = "Password1";

    private static string BasicHeader(string username, string password) =>
        "Basic " + Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{username}:{password}"));

    private static EnvVarCredentialAuthorizer CreateAuthorizer() =>
        new(
            new DictionaryEnvironmentVariableReader(new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["LATTICE_STATE_USER_" + Username] = LatticePasswordHash.Hash(Password),
            }),
            new StaticOptionsMonitor<EnvVarCredentialAuthorizerOptions>(new EnvVarCredentialAuthorizerOptions()),
            NullLogger<EnvVarCredentialAuthorizer>.Instance,
            new FixedTimeProvider(DateTimeOffset.UnixEpoch));

    private static LatticeStateApiAuthorizationContext ContextWithHeader(string? headerValue)
    {
        var headers = new global::Grpc.Core.Metadata();
        if (headerValue is not null)
        {
            headers.Add("authorization", headerValue);
        }

        return new LatticeStateApiAuthorizationContext(
            new StateGrpcCallContext("/orleans.lattice.api.state/ListTrees", headers),
            LatticeStateApiOperation.ListTrees,
            targetTreeId: null);
    }

    [Test]
    public async Task IsAuthorizedAsync_accepts_a_valid_basic_credential_from_the_call_headers()
    {
        var authorizer = CreateAuthorizer();

        var authorized = await authorizer.IsAuthorizedAsync(
            ContextWithHeader(BasicHeader(Username, Password)), CancellationToken.None);

        Assert.That(authorized, Is.True);
    }

    [Test]
    public async Task IsAuthorizedAsync_rejects_a_wrong_password_from_the_call_headers()
    {
        var authorizer = CreateAuthorizer();

        var authorized = await authorizer.IsAuthorizedAsync(
            ContextWithHeader(BasicHeader(Username, "WrongPassword1")), CancellationToken.None);

        Assert.That(authorized, Is.False);
    }

    [Test]
    public async Task IsAuthorizedAsync_rejects_a_call_that_carries_no_authorization_header()
    {
        var authorizer = CreateAuthorizer();

        var authorized = await authorizer.IsAuthorizedAsync(ContextWithHeader(null), CancellationToken.None);

        Assert.That(authorized, Is.False);
    }

    [Test]
    public void Authorize_rejects_a_username_that_does_not_start_with_a_letter_or_underscore()
    {
        // The username is concatenated into an environment-variable name, so a
        // shape that is not a legal variable name is refused before the lookup
        // rather than probing the host's environment with attacker-chosen text.
        var authorizer = CreateAuthorizer();

        Assert.That(authorizer.Authorize(BasicHeader("1alice", Password)), Is.False);
    }

    [Test]
    public void Authorize_accepts_a_username_that_starts_with_an_underscore()
    {
        var authorizer = CreateAuthorizer();

        // Shape-valid but unknown, so still denied - the point is that it gets
        // past the shape guard rather than being rejected for its first character.
        Assert.That(authorizer.Authorize(BasicHeader("_alice", Password)), Is.False);
    }

    [Test]
    public void Authorize_rejects_a_username_containing_a_non_word_character()
    {
        var authorizer = CreateAuthorizer();

        Assert.That(authorizer.Authorize(BasicHeader("al-ice", Password)), Is.False);
    }

    private sealed class DictionaryEnvironmentVariableReader(IDictionary<string, string> values)
        : IEnvironmentVariableReader
    {
        public string? GetVariable(string name) => values.TryGetValue(name, out var value) ? value : null;
    }

    private sealed class StaticOptionsMonitor<T>(T value) : Microsoft.Extensions.Options.IOptionsMonitor<T>
    {
        public T CurrentValue => value;

        public T Get(string? name) => value;

        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }

    private sealed class FixedTimeProvider(DateTimeOffset start) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => start;
    }
}
