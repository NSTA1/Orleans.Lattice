using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.State.Grpc;

namespace Orleans.Lattice.Api.State.Grpc.Tests.Security;

[TestFixture]
public class EnvVarCredentialAuthorizerTests
{
    private const string Username = "alice";
    private const string Password = "Password1";

    private static string BasicHeader(string username, string password)
    {
        var encoded = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{username}:{password}"));
        return $"Basic {encoded}";
    }

    private static EnvVarCredentialAuthorizer CreateAuthorizer(
        IDictionary<string, string> environment,
        out TestTimeProvider time,
        EnvVarCredentialAuthorizerOptions? options = null)
    {
        time = new TestTimeProvider(DateTimeOffset.UnixEpoch);
        var reader = new DictionaryEnvironmentVariableReader(environment);
        var monitor = new StaticOptionsMonitor<EnvVarCredentialAuthorizerOptions>(
            options ?? new EnvVarCredentialAuthorizerOptions());
        return new EnvVarCredentialAuthorizer(reader, monitor, NullLogger<EnvVarCredentialAuthorizer>.Instance, time);
    }

    private static Dictionary<string, string> WithCredential(string username, string password) =>
        new(StringComparer.Ordinal)
        {
            ["LATTICE_STATE_USER_" + username] = LatticePasswordHash.Hash(password),
        };

    [Test]
    public void Authorize_validCredential_returnsTrue()
    {
        var authorizer = CreateAuthorizer(WithCredential(Username, Password), out _);

        Assert.That(authorizer.Authorize(BasicHeader(Username, Password)), Is.True);
    }

    [Test]
    public void Authorize_wrongPassword_returnsFalse()
    {
        var authorizer = CreateAuthorizer(WithCredential(Username, Password), out _);

        Assert.That(authorizer.Authorize(BasicHeader(Username, "WrongPassword1")), Is.False);
    }

    [Test]
    public void Authorize_unknownUser_returnsFalse()
    {
        var authorizer = CreateAuthorizer(WithCredential(Username, Password), out _);

        Assert.That(authorizer.Authorize(BasicHeader("mallory", Password)), Is.False);
    }

    [Test]
    public void Authorize_missingHeader_returnsFalse()
    {
        var authorizer = CreateAuthorizer(WithCredential(Username, Password), out _);

        Assert.That(authorizer.Authorize(null), Is.False);
    }

    [TestCase("")]
    [TestCase("Bearer sometoken")]
    [TestCase("Basic !!notbase64!!")]
    [TestCase("Basic " + "bm9jb2xvbg==")] // base64("nocolon"), no ':' separator
    public void Authorize_malformedHeader_returnsFalse(string header)
    {
        var authorizer = CreateAuthorizer(WithCredential(Username, Password), out _);

        Assert.That(authorizer.Authorize(header), Is.False);
    }

    [Test]
    public void Authorize_usernameWithInvalidCharset_returnsFalse()
    {
        var authorizer = CreateAuthorizer(WithCredential(Username, Password), out _);

        Assert.That(authorizer.Authorize(BasicHeader("bad-user!", Password)), Is.False);
    }

    [Test]
    public void Authorize_locksOutAfterRepeatedFailures()
    {
        var options = new EnvVarCredentialAuthorizerOptions { MaxFailedAttempts = 3 };
        var authorizer = CreateAuthorizer(WithCredential(Username, Password), out _, options);

        for (var i = 0; i < 3; i++)
        {
            Assert.That(authorizer.Authorize(BasicHeader(Username, "WrongPassword1")), Is.False);
        }

        // Now locked out: even the correct password is denied.
        Assert.That(authorizer.Authorize(BasicHeader(Username, Password)), Is.False);
    }

    [Test]
    public void Authorize_lockoutExpires_afterLockoutDuration()
    {
        var options = new EnvVarCredentialAuthorizerOptions
        {
            MaxFailedAttempts = 3,
            LockoutDuration = TimeSpan.FromMinutes(1),
        };
        var authorizer = CreateAuthorizer(WithCredential(Username, Password), out var time, options);

        for (var i = 0; i < 3; i++)
        {
            authorizer.Authorize(BasicHeader(Username, "WrongPassword1"));
        }

        Assert.That(authorizer.Authorize(BasicHeader(Username, Password)), Is.False, "still locked out");

        time.Advance(TimeSpan.FromMinutes(2));

        Assert.That(authorizer.Authorize(BasicHeader(Username, Password)), Is.True, "lockout window elapsed");
    }

    [Test]
    public void Authorize_successResetsFailureCount()
    {
        var options = new EnvVarCredentialAuthorizerOptions { MaxFailedAttempts = 3 };
        var authorizer = CreateAuthorizer(WithCredential(Username, Password), out _, options);

        authorizer.Authorize(BasicHeader(Username, "WrongPassword1"));
        authorizer.Authorize(BasicHeader(Username, "WrongPassword1"));
        Assert.That(authorizer.Authorize(BasicHeader(Username, Password)), Is.True);

        // The counter reset, so two more failures must not trip the lockout.
        authorizer.Authorize(BasicHeader(Username, "WrongPassword1"));
        authorizer.Authorize(BasicHeader(Username, "WrongPassword1"));
        Assert.That(authorizer.Authorize(BasicHeader(Username, Password)), Is.True);
    }

    [Test]
    public void Authorize_respectsCustomEnvironmentPrefix()
    {
        var environment = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["CUSTOM_PREFIX_" + Username] = LatticePasswordHash.Hash(Password),
        };
        var options = new EnvVarCredentialAuthorizerOptions { EnvironmentVariablePrefix = "CUSTOM_PREFIX_" };
        var authorizer = CreateAuthorizer(environment, out _, options);

        Assert.That(authorizer.Authorize(BasicHeader(Username, Password)), Is.True);
    }

    [Test]
    public void Constructor_nullEnvironment_throws()
    {
        var monitor = new StaticOptionsMonitor<EnvVarCredentialAuthorizerOptions>(new());
        Assert.That(
            () => new EnvVarCredentialAuthorizer(null!, monitor, NullLogger<EnvVarCredentialAuthorizer>.Instance),
            Throws.ArgumentNullException);
    }

    private sealed class DictionaryEnvironmentVariableReader(IDictionary<string, string> values)
        : IEnvironmentVariableReader
    {
        public string? GetVariable(string name) => values.TryGetValue(name, out var value) ? value : null;
    }

    private sealed class StaticOptionsMonitor<T>(T value) : IOptionsMonitor<T>
    {
        public T CurrentValue { get; } = value;

        public T Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }

    private sealed class TestTimeProvider(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }
}
