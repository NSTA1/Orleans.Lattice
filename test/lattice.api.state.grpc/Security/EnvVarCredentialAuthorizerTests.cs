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

    /// <summary>
    /// Builds an authorizer over a credential store that resolves names
    /// case-insensitively, reproducing how Windows resolves process environment
    /// variables, and keys the failed-attempt map the same way. Lets the
    /// case-variance regression tests run deterministically on any host platform
    /// rather than silently passing off Windows.
    /// </summary>
    private static EnvVarCredentialAuthorizer CreateCaseInsensitiveAuthorizer(
        string username,
        string password,
        out TestTimeProvider time,
        EnvVarCredentialAuthorizerOptions? options = null)
    {
        time = new TestTimeProvider(DateTimeOffset.UnixEpoch);
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["LATTICE_STATE_USER_" + username] = LatticePasswordHash.Hash(password),
        };
        var monitor = new StaticOptionsMonitor<EnvVarCredentialAuthorizerOptions>(
            options ?? new EnvVarCredentialAuthorizerOptions());

        return new EnvVarCredentialAuthorizer(
            new DictionaryEnvironmentVariableReader(values),
            monitor,
            NullLogger<EnvVarCredentialAuthorizer>.Instance,
            time,
            StringComparer.OrdinalIgnoreCase);
    }

    [Test]
    public void Authorize_caseVariantOfLockedUser_sharesTheLockout()
    {
        var options = new EnvVarCredentialAuthorizerOptions { MaxFailedAttempts = 3 };
        var authorizer = CreateCaseInsensitiveAuthorizer(Username, Password, out _, options);

        for (var i = 0; i < 3; i++)
        {
            Assert.That(authorizer.Authorize(BasicHeader(Username, "WrongPassword1")), Is.False);
        }

        // The lockout must key on the same identity the credential lookup uses.
        // When the environment resolves names case-insensitively, "ALICE" is the
        // very same credential as "alice", so it must inherit the lockout instead
        // of being handed a fresh MaxFailedAttempts budget (CWE-307).
        Assert.Multiple(() =>
        {
            Assert.That(authorizer.Authorize(BasicHeader("ALICE", Password)), Is.False, "upper-case variant");
            Assert.That(authorizer.Authorize(BasicHeader("Alice", Password)), Is.False, "title-case variant");
            Assert.That(authorizer.Authorize(BasicHeader("aLiCe", Password)), Is.False, "mixed-case variant");
        });
    }

    [Test]
    public void Authorize_caseVariantsOfKnownUser_doNotGrowTheAttemptMap()
    {
        var authorizer = CreateCaseInsensitiveAuthorizer(Username, Password, out _);

        foreach (var variant in new[] { "alice", "ALICE", "Alice", "aLiCe", "AlIcE", "aliCE" })
        {
            authorizer.Authorize(BasicHeader(variant, "WrongPassword1"));
        }

        // Every variant resolves to one credential, so it must occupy one record.
        // Otherwise a caller who knows a single valid username grows the map by
        // 2^n records without ever presenting a valid password (CWE-770), which
        // is exactly the unbounded growth the unknown-user branch avoids.
        Assert.That(authorizer.TrackedUsernameCount, Is.EqualTo(1));
    }

    [Test]
    public void Authorize_caseInsensitiveEnvironment_stillAcceptsAValidCredential()
    {
        var authorizer = CreateCaseInsensitiveAuthorizer(Username, Password, out _);

        // Collapsing the attempt key must not break a legitimate login that uses
        // a different spelling from the configured variable.
        Assert.That(authorizer.Authorize(BasicHeader("ALICE", Password)), Is.True);
    }

    [Test]
    public void Authorize_caseSensitiveEnvironment_keepsDistinctUsersIndependent()
    {
        var time = new TestTimeProvider(DateTimeOffset.UnixEpoch);
        var values = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["LATTICE_STATE_USER_alice"] = LatticePasswordHash.Hash(Password),
            ["LATTICE_STATE_USER_ALICE"] = LatticePasswordHash.Hash("SecondUser1"),
        };
        var monitor = new StaticOptionsMonitor<EnvVarCredentialAuthorizerOptions>(
            new EnvVarCredentialAuthorizerOptions { MaxFailedAttempts = 3 });
        var authorizer = new EnvVarCredentialAuthorizer(
            new DictionaryEnvironmentVariableReader(values),
            monitor,
            NullLogger<EnvVarCredentialAuthorizer>.Instance,
            time,
            StringComparer.Ordinal);

        for (var i = 0; i < 3; i++)
        {
            authorizer.Authorize(BasicHeader("alice", "WrongPassword1"));
        }

        // On a case-sensitive environment the two names really are two separate
        // credentials, so locking one must not lock the other.
        Assert.Multiple(() =>
        {
            Assert.That(authorizer.Authorize(BasicHeader("alice", Password)), Is.False, "locked out");
            Assert.That(authorizer.Authorize(BasicHeader("ALICE", "SecondUser1")), Is.True, "independent user");
        });
    }

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
    public void Authorize_unknownUsers_doNotPopulateAttemptMap()
    {
        var authorizer = CreateAuthorizer(WithCredential(Username, Password), out _);

        for (var i = 0; i < 50; i++)
        {
            Assert.That(authorizer.Authorize(BasicHeader("probe_" + i, Password)), Is.False);
        }

        // Unknown-user probes must not grow the per-username attempt map
        // (CWE-770 pre-auth memory-exhaustion vector).
        Assert.That(authorizer.TrackedUsernameCount, Is.Zero);
    }

    [Test]
    public void Authorize_knownUserFailure_populatesAttemptMapOnce()
    {
        var authorizer = CreateAuthorizer(WithCredential(Username, Password), out _);

        authorizer.Authorize(BasicHeader(Username, "WrongPassword1"));
        authorizer.Authorize(BasicHeader(Username, "WrongPassword1"));

        // Only real credentials are tracked, so the map is bounded by the number
        // of configured users regardless of how many attempts they make.
        Assert.That(authorizer.TrackedUsernameCount, Is.EqualTo(1));
    }

    [Test]
    public void Authorize_publicConstructor_keysLockoutToMatchPlatformEnvironmentSemantics()
    {
        // The public constructor must pick the comparer that matches how this
        // platform's process environment resolves names, because that is the
        // lookup the lockout guards. Asserting it here stops the platform wiring
        // silently regressing to a flat Ordinal key.
        var caseInsensitive = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["LATTICE_STATE_USER_" + Username] = LatticePasswordHash.Hash(Password),
        };
        var monitor = new StaticOptionsMonitor<EnvVarCredentialAuthorizerOptions>(
            new EnvVarCredentialAuthorizerOptions { MaxFailedAttempts = 3 });
        var authorizer = new EnvVarCredentialAuthorizer(
            new DictionaryEnvironmentVariableReader(caseInsensitive),
            monitor,
            NullLogger<EnvVarCredentialAuthorizer>.Instance,
            new TestTimeProvider(DateTimeOffset.UnixEpoch));

        for (var i = 0; i < 3; i++)
        {
            authorizer.Authorize(BasicHeader(Username, "WrongPassword1"));
        }

        authorizer.Authorize(BasicHeader("ALICE", "WrongPassword1"));

        // On Windows the two spellings are one credential and must share one
        // record; elsewhere they are genuinely distinct names.
        Assert.That(
            authorizer.TrackedUsernameCount,
            Is.EqualTo(OperatingSystem.IsWindows() ? 1 : 2));
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
    public void Authorize_lockedOutUser_stillSpendsAVerification()
    {
        var options = new EnvVarCredentialAuthorizerOptions { MaxFailedAttempts = 3 };
        var authorizer = CreateAuthorizer(WithCredential(Username, Password), out _, options);

        for (var i = 0; i < 3; i++)
        {
            authorizer.Authorize(BasicHeader(Username, "WrongPassword1"));
        }

        var beforeLockedCall = authorizer.VerificationCount;

        // The now-locked-out call must still spend a (dummy) verification so its
        // response timing matches a verify-bearing call and does not leak the
        // lockout / user-existence state.
        Assert.That(authorizer.Authorize(BasicHeader(Username, Password)), Is.False, "locked out");
        Assert.That(authorizer.VerificationCount, Is.EqualTo(beforeLockedCall + 1));
    }

    [Test]
    public void Authorize_everyTerminalOutcome_spendsExactlyOneVerification()
    {
        var authorizer = CreateAuthorizer(WithCredential(Username, Password), out _);

        // Unknown user.
        authorizer.Authorize(BasicHeader("mallory", Password));
        Assert.That(authorizer.VerificationCount, Is.EqualTo(1), "unknown user");

        // Known user, wrong password.
        authorizer.Authorize(BasicHeader(Username, "WrongPassword1"));
        Assert.That(authorizer.VerificationCount, Is.EqualTo(2), "wrong password");

        // Known user, correct password.
        authorizer.Authorize(BasicHeader(Username, Password));
        Assert.That(authorizer.VerificationCount, Is.EqualTo(3), "valid credential");
    }

    [Test]
    public void Authorize_malformedInput_spendsNoVerification()
    {
        var authorizer = CreateAuthorizer(WithCredential(Username, Password), out _);

        // Requests rejected before the credential lookup (no username to probe)
        // do no PBKDF2 work; there is nothing to time against.
        authorizer.Authorize(null);
        authorizer.Authorize(BasicHeader("bad-user!", Password));
        Assert.That(authorizer.VerificationCount, Is.Zero);
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
