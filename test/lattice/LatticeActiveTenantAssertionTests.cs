namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeActiveTenantAssertion"/>, the single seam
/// every transport binding lifts a caller-asserted active tenant through. Covers
/// the fail-closed rules and the allocation behaviour, which matters because this
/// runs on every inbound request of every bound facade.
/// </summary>
[TestFixture]
public sealed class LatticeActiveTenantAssertionTests
{
    private static Func<string, string?> Header(string name, string? value)
        => probed => string.Equals(probed, name, StringComparison.Ordinal) ? value : null;

    private static TenantId? Resolve(string? headerValue, string headerName = LatticeActiveTenantAssertion.DefaultHeaderName)
        => LatticeActiveTenantAssertion.Resolve(
            Header(LatticeActiveTenantAssertion.DefaultHeaderName, headerValue),
            static (lookup, name) => lookup(name),
            headerName);

    // ----- Happy path -----

    [Test]
    public void A_valid_assertion_resolves()
        => Assert.That(Resolve("acme")?.Value, Is.EqualTo("acme"));

    [Test]
    public void A_padded_assertion_is_trimmed()
        => Assert.That(Resolve("  acme  ")?.Value, Is.EqualTo("acme"));

    [Test]
    public void Stamp_opens_an_ambient_scope_and_restores_it()
    {
        Assert.That(LatticeActiveTenantContext.Current, Is.Null);

        using (LatticeActiveTenantAssertion.Stamp(
                   Header(LatticeActiveTenantAssertion.DefaultHeaderName, "acme"),
                   static (lookup, name) => lookup(name),
                   LatticeActiveTenantAssertion.DefaultHeaderName))
        {
            Assert.That(LatticeActiveTenantContext.Current?.Value, Is.EqualTo("acme"));
        }

        Assert.That(LatticeActiveTenantContext.Current, Is.Null);
    }

    // ----- Fail-closed on every ambiguous input -----

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("NOT VALID")]
    [TestCase("Acme")]
    [TestCase("-leading")]
    [TestCase("trailing-")]
    [TestCase("has/slash")]
    public void An_absent_or_invalid_assertion_resolves_to_nothing(string? headerValue)
        => Assert.That(Resolve(headerValue), Is.Null);

    [Test]
    public void An_invalid_assertion_opens_no_scope()
    {
        var scope = LatticeActiveTenantAssertion.Stamp(
            Header(LatticeActiveTenantAssertion.DefaultHeaderName, "NOT VALID"),
            static (lookup, name) => lookup(name),
            LatticeActiveTenantAssertion.DefaultHeaderName);

        Assert.Multiple(() =>
        {
            Assert.That(scope, Is.Null, "no scope means no allocation on the absent path");
            Assert.That(LatticeActiveTenantContext.Current, Is.Null);
        });
    }

    [TestCase(null)]
    [TestCase("")]
    public void An_unconfigured_header_name_disables_the_assertion(string? headerName)
        => Assert.That(
            LatticeActiveTenantAssertion.Resolve(
                Header(LatticeActiveTenantAssertion.DefaultHeaderName, "acme"),
                static (lookup, name) => lookup(name),
                headerName),
            Is.Null);

    [Test]
    public void A_null_lookup_is_rejected()
        => Assert.That(
            () => LatticeActiveTenantAssertion.Resolve<object?>(null, null!, "h"),
            Throws.ArgumentNullException);

    // ----- Header-name normalisation -----

    [Test]
    public void The_header_name_is_matched_case_insensitively()
    {
        // gRPC lower-cases metadata keys, so a host that configures a mixed-case
        // name must still match the inbound entry.
        var resolved = LatticeActiveTenantAssertion.Resolve(
            Header("x-tenant", "acme"),
            static (lookup, name) => lookup(name),
            "X-Tenant");

        Assert.That(resolved?.Value, Is.EqualTo("acme"));
    }

    [Test]
    public void An_already_lower_case_header_name_is_passed_through_unchanged()
    {
        // The default is lower-case, so the hot path must not allocate a
        // normalised copy: the lookup receives the caller's own string.
        string? probed = null;
        var resolved = LatticeActiveTenantAssertion.Resolve(
            new object(),
            (_, name) => { probed = name; return "acme"; },
            LatticeActiveTenantAssertion.DefaultHeaderName);

        Assert.Multiple(() =>
        {
            Assert.That(resolved?.Value, Is.EqualTo("acme"));
            Assert.That(
                ReferenceEquals(probed, LatticeActiveTenantAssertion.DefaultHeaderName),
                Is.True,
                "an already-lower-case name must not be re-allocated per request");
        });
    }

    [Test]
    public void A_mixed_case_header_name_is_normalised_once_and_memoised()
    {
        const string configured = "X-Lattice-Tenant";
        string? first = null;
        string? second = null;

        _ = LatticeActiveTenantAssertion.Resolve(
            new object(), (_, name) => { first = name; return "acme"; }, configured);
        _ = LatticeActiveTenantAssertion.Resolve(
            new object(), (_, name) => { second = name; return "acme"; }, configured);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo("x-lattice-tenant"));
            Assert.That(
                ReferenceEquals(first, second),
                Is.True,
                "the normalised name must be memoised, not rebuilt on every request");
        });
    }

    // ----- Allocation behaviour on the value path -----

    [Test]
    public void An_untrimmed_value_reuses_the_header_string()
    {
        // The common case: the resolved tenant wraps the caller's own string
        // rather than copying it.
        var headerValue = string.Concat("ac", "me");
        var resolved = LatticeActiveTenantAssertion.Resolve(
            Header(LatticeActiveTenantAssertion.DefaultHeaderName, headerValue),
            static (lookup, name) => lookup(name),
            LatticeActiveTenantAssertion.DefaultHeaderName);

        Assert.That(ReferenceEquals(resolved?.Value, headerValue), Is.True);
    }
}
