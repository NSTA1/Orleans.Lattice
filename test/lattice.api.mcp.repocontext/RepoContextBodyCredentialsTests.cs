using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Unit tests for <see cref="RepoContextBodyCredentials"/> and for the guard it
/// backs on the two write seams, <see cref="RepoContextToolHandlers.RememberAsync"/>
/// and <see cref="RepoContextToolHandlers.UpdateAsync"/>.
/// </summary>
/// <remarks>
/// <para>
/// A redaction upstream of this store rewrites a URL carrying a password-bearing
/// userinfo component while it is in transit. The replaced span runs from the
/// scheme through the at-sign and reaches <em>backwards</em> past the scheme, so it
/// can consume a structural character belonging to the serialised record. The
/// stored value is then malformed in the worst available way: the entry still reads
/// back, so nothing looks wrong, while every later read-modify-write against it
/// fails forever.
/// </para>
/// <para>
/// <strong>The arms below are measured behaviour, not a guess.</strong> Each
/// positive and negative control corresponds to a shape whose treatment by the
/// rewrite was observed directly, by comparing a string's in-process length against
/// the text that arrived after transit. The guard's predicate was then chosen to
/// predict all of them. That is why it keys on structure and never asks whether a
/// value is secret.
/// </para>
/// <para>
/// <strong>The trigger is the shape, not the secret, and that inverts the obvious
/// design.</strong> A shell-variable placeholder is rewritten exactly as a literal
/// token is. So a guard that tried to tell a real credential from a placeholder -
/// by length, by character mix, by entropy - would have permitted precisely the
/// bodies that corrupt, including this repository's own documented push recipe.
/// <see cref="Inspect_rejects_the_documented_push_recipe_shape_despite_its_placeholder"/>
/// is the arm that pins that, and it is the single most important test in this file.
/// </para>
/// <para>
/// <strong>This fixture builds every URL from parts at runtime.</strong> A fixture
/// spelling the shape out would be a tracked file carrying the exact pattern the
/// rewrite fires on, and would corrupt any entry that quoted it. The production
/// detector contains no such URL for the same reason, as does
/// <see cref="RepoContextBodyFraming"/> for tool-call framing.
/// </para>
/// <para>
/// The negative controls are load-bearing rather than decoration. An over-broad
/// guard would block ordinary prose containing an ordinary URL, and would block the
/// remedy it recommends - which would leave an author with no way to capture this
/// defect at all.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextBodyCredentialsTests
{
    private const char Colon = ':';
    private const char Slash = '/';
    private const char At = '@';
    private const char Lt = '<';
    private const char Gt = '>';

    /// <summary>The scheme separator, assembled rather than spelled out.</summary>
    private static readonly string Sep = new([Colon, Slash, Slash]);

    /// <summary>A URL with a userinfo component, assembled from parts.</summary>
    private static string Authority(string scheme, string userinfo, string host) =>
        scheme + Sep + userinfo + At + host;

    /// <summary>A URL with no userinfo component, assembled from parts.</summary>
    private static string Plain(string scheme, string host) => scheme + Sep + host;

    /// <summary>A userinfo carrying a password segment.</summary>
    private static string WithPassword(string user, string secret) => user + Colon + secret;

    // ---- positive controls: shapes the rewrite was observed to rewrite -------

    /// <summary>
    /// The plainest offending shape: a userinfo with a password segment. Observed
    /// rewritten in transit, so it must never reach storage.
    /// </summary>
    [Test]
    public void Inspect_rejects_a_userinfo_carrying_a_password()
    {
        var body = Authority("https", WithPassword("user", "pass"), "example.invalid/x");

        var inspection = RepoContextBodyCredentials.Inspect(body);

        Assert.That(inspection.CarriesCredentialUrl, Is.True);
    }

    /// <summary>
    /// <strong>The arm that inverts the obvious design.</strong> This is the shape of
    /// this repository's own documented push convention, whose password segment is a
    /// shell variable and not a credential at all. It was observed rewritten in
    /// transit exactly as a literal token is.
    /// </summary>
    /// <remarks>
    /// A guard that permitted placeholders and refused only literals would let this
    /// through, and this is the very body that froze the memory entry written to warn
    /// about the trap. The population at risk is careful authors quoting a documented
    /// recipe, not careless ones pasting secrets.
    /// </remarks>
    [Test]
    public void Inspect_rejects_the_documented_push_recipe_shape_despite_its_placeholder()
    {
        var shellPlaceholder = "$" + "tok";
        var body = "Push with " + Authority(
            "https", WithPassword("x-access-token", shellPlaceholder), "github.com/o/r.git");

        var inspection = RepoContextBodyCredentials.Inspect(body);

        Assert.That(
            inspection.CarriesCredentialUrl,
            Is.True,
            "A shell-variable placeholder is rewritten exactly as a literal credential is, so "
            + "permitting it would permit the precise body that corrupts.");
    }

    /// <summary>The scheme is not part of the predicate, so a non-web scheme is caught too.</summary>
    [Test]
    public void Inspect_rejects_a_password_bearing_userinfo_under_any_scheme()
    {
        var body = Authority("ssh", WithPassword("git", "PLACEHOLDER"), "example.invalid/x");

        Assert.That(RepoContextBodyCredentials.Inspect(body).CarriesCredentialUrl, Is.True);
    }

    /// <summary>
    /// A long opaque password segment is caught for exactly the same structural
    /// reason as a short one. The guard never measures length, so this arm and the
    /// short-password arm must agree.
    /// </summary>
    [Test]
    public void Inspect_rejects_a_long_opaque_password_segment()
    {
        var body = Authority("https", WithPassword("user", new string('x', 40)), "example.invalid/x");

        Assert.That(RepoContextBodyCredentials.Inspect(body).CarriesCredentialUrl, Is.True);
    }

    /// <summary>
    /// The scan continues past a clean URL. A body whose first URL is harmless and
    /// whose second is not must still be refused, or a single innocuous link would
    /// shield everything after it.
    /// </summary>
    [Test]
    public void Inspect_keeps_scanning_past_a_clean_url()
    {
        var body = "See " + Plain("https", "example.invalid/docs")
            + " and then " + Authority("https", WithPassword("user", "pass"), "example.invalid/x");

        Assert.That(RepoContextBodyCredentials.Inspect(body).CarriesCredentialUrl, Is.True);
    }

    // ---- negative controls: shapes the rewrite was observed to leave alone ---

    /// <summary>A URL with no userinfo is not rewritten and must not be refused.</summary>
    [Test]
    public void Inspect_accepts_a_url_with_no_userinfo()
    {
        Assert.That(
            RepoContextBodyCredentials.Inspect("See " + Plain("https", "example.invalid/x")).CarriesCredentialUrl,
            Is.False);
    }

    /// <summary>
    /// A userinfo with no password segment was observed NOT rewritten, so refusing it
    /// would block a body that was never at risk. Requiring the colon is a deliberate
    /// clause, not an oversight.
    /// </summary>
    [Test]
    public void Inspect_accepts_a_userinfo_with_no_password_segment()
    {
        var body = Authority("https", "user", "example.invalid/x");

        Assert.That(RepoContextBodyCredentials.Inspect(body).CarriesCredentialUrl, Is.False);
    }

    /// <summary>
    /// An scp-style git remote has no scheme separator, so the rewrite does not fire
    /// on it. These appear constantly in ordinary notes about git.
    /// </summary>
    [Test]
    public void Inspect_accepts_an_scp_style_git_remote()
    {
        var body = "git" + At + "github.com" + Colon + "owner/repo.git";

        Assert.That(RepoContextBodyCredentials.Inspect(body).CarriesCredentialUrl, Is.False);
    }

    /// <summary>
    /// <strong>The remedy must pass.</strong> An angle-bracketed placeholder was
    /// observed NOT rewritten, because an angle bracket cannot appear in a userinfo
    /// component, so no URL of the offending shape is present. This is the form the
    /// rejection message recommends; were it refused, the message would be advising
    /// an author to do something the guard forbids.
    /// </summary>
    [Test]
    public void Inspect_accepts_an_angle_bracketed_placeholder()
    {
        var body = Authority(
            "https", WithPassword("user", Lt + "TOKEN" + Gt), "example.invalid/x");

        Assert.That(RepoContextBodyCredentials.Inspect(body).CarriesCredentialUrl, Is.False);
    }

    /// <summary>A host:port authority carries a colon but no at-sign, so it is untouched.</summary>
    [Test]
    public void Inspect_accepts_a_host_and_port()
    {
        var body = "Listening on " + Plain("http", "localhost" + Colon + "8443/x");

        Assert.That(RepoContextBodyCredentials.Inspect(body).CarriesCredentialUrl, Is.False);
    }

    /// <summary>Prose describing the shape in words is how the defect gets documented.</summary>
    [Test]
    public void Inspect_accepts_prose_describing_the_shape()
    {
        const string body = "A URL whose userinfo carries a password segment is rewritten in transit, "
            + "which is why this guard refuses one.";

        Assert.That(RepoContextBodyCredentials.Inspect(body).CarriesCredentialUrl, Is.False);
    }

    /// <summary>An absent or empty body carries nothing and must be reported clean.</summary>
    [TestCase(null)]
    [TestCase("")]
    public void Inspect_accepts_an_absent_or_empty_body(string? body)
    {
        var inspection = RepoContextBodyCredentials.Inspect(body);

        Assert.Multiple(() =>
        {
            Assert.That(inspection.CarriesCredentialUrl, Is.False);
            Assert.That(inspection.Offset, Is.EqualTo(-1));
            Assert.That(inspection.UserinfoLength, Is.EqualTo(0));
        });
    }

    // ---- what the inspection reports ----------------------------------------

    /// <summary>
    /// The offset and length locate the URL precisely enough to find it in a long
    /// body. The expected values are asserted exactly rather than merely as
    /// non-defaults, so an off-by-one or a wrong unit cannot pass.
    /// </summary>
    [Test]
    public void Inspect_reports_the_offset_of_the_scheme_separator_and_the_userinfo_length()
    {
        const string prefix = "see ";
        var userinfo = WithPassword("user", "pass");
        var body = prefix + "https" + Sep + userinfo + At + "example.invalid/x";

        var inspection = RepoContextBodyCredentials.Inspect(body);

        Assert.Multiple(() =>
        {
            Assert.That(inspection.Offset, Is.EqualTo(prefix.Length + "https".Length));
            Assert.That(inspection.UserinfoLength, Is.EqualTo(userinfo.Length));
        });
    }

    // ---- the rejection message ----------------------------------------------

    /// <summary>
    /// <strong>The message must carry no body content whatsoever.</strong> This is
    /// stricter than the bounded byte window the decode diagnostic prints, and it is
    /// stricter deliberately: the population this guard fires on carries credentials
    /// by construction, and an exception message propagates into logs, telemetry, CI
    /// transcripts and pull requests.
    /// </summary>
    [Test]
    public void DescribeRejection_quotes_no_part_of_the_body()
    {
        var secret = "sekrit" + "Value" + "1234567890";
        var userinfo = WithPassword("user", secret);
        var body = Authority("https", userinfo, "example.invalid/x");
        var inspection = RepoContextBodyCredentials.Inspect(body);

        var message = RepoContextBodyCredentials.DescribeRejection(
            RepoContextBodyFraming.RememberBodyLocation, inspection);

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Not.Contain(secret));
            Assert.That(message, Does.Not.Contain(userinfo));
            Assert.That(message, Does.Not.Contain("example.invalid"));
        });
    }

    /// <summary>The message locates the URL by offset and length, which is all it may reveal.</summary>
    [Test]
    public void DescribeRejection_reports_the_offset_and_the_userinfo_length()
    {
        var body = Authority("https", WithPassword("user", "pass"), "example.invalid/x");
        var inspection = RepoContextBodyCredentials.Inspect(body);

        var message = RepoContextBodyCredentials.DescribeRejection(
            RepoContextBodyFraming.RememberBodyLocation, inspection);

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("offset " + inspection.Offset));
            Assert.That(message, Does.Contain("userinfo length " + inspection.UserinfoLength));
        });
    }

    /// <summary>
    /// <strong>The message is the only channel that survives the rewrite.</strong>
    /// Every other route to teaching this - a memory entry, a captured gotcha, a doc
    /// line quoting the recipe - is itself subject to the rewrite, so the message
    /// must carry the whole lesson rather than being terse. It must say that a
    /// shell-variable placeholder is not sufficient, which is the trap, and it must
    /// name the remedy that the angle-bracket arm proves actually works.
    /// </summary>
    [Test]
    public void DescribeRejection_states_that_a_placeholder_is_not_sufficient_and_names_the_remedy()
    {
        var body = Authority("https", WithPassword("user", "pass"), "example.invalid/x");
        var inspection = RepoContextBodyCredentials.Inspect(body);

        var message = RepoContextBodyCredentials.DescribeRejection(
            RepoContextBodyFraming.RememberBodyLocation, inspection);

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("shell-variable placeholder is NOT sufficient"));
            Assert.That(message, Does.Contain("angle-bracketed"));
            Assert.That(message, Does.Contain("never inspects the value"));

            // The remedy without its reason is a superstition: a reader told to use
            // angle brackets, but not told why they work, has no way to judge any
            // other placeholder and will reach for the next plausible one.
            Assert.That(message, Does.Contain("illegal in an RFC 3986 userinfo component"));
        });
    }

    /// <summary>
    /// <strong>The message must not assert that the body held a credential.</strong>
    /// The guard fires on a shape and never inspects the value, so a message reading
    /// as a secret-detection finding would be a claim the code cannot support - and
    /// worse, it would invite the reader to reason about whether their value is
    /// really a secret, which is precisely the reasoning that permits the shape that
    /// corrupts. The disclaimer is load-bearing, not politeness.
    /// </summary>
    [Test]
    public void DescribeRejection_disclaims_any_finding_that_the_value_is_a_credential()
    {
        var body = Authority("https", WithPassword("user", "pass"), "example.invalid/x");
        var inspection = RepoContextBodyCredentials.Inspect(body);

        var message = RepoContextBodyCredentials.DescribeRejection(
            RepoContextBodyFraming.RememberBodyLocation, inspection);

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("refused for its SHAPE"));
            Assert.That(message, Does.Contain("No claim is made that it holds a real credential"));
        });
    }

    /// <summary>
    /// <strong>The message must explain where the symptom will appear.</strong> The
    /// visible failure is never at the URL: the replaced span reaches backwards past
    /// the scheme, so what breaks is whatever structural character sat in front of
    /// it - a code fence, a line ending, a backslash. A reader who knows to suspect
    /// the string finds it in minutes; a reader who does not spends an hour in the
    /// file where the symptom surfaced. That is the difference this paragraph buys,
    /// and it is why the message is long.
    /// </summary>
    [Test]
    public void DescribeRejection_explains_the_backwards_span_and_where_the_symptom_appears()
    {
        var body = Authority("https", WithPassword("user", "pass"), "example.invalid/x");
        var inspection = RepoContextBodyCredentials.Inspect(body);

        var message = RepoContextBodyCredentials.DescribeRejection(
            RepoContextBodyFraming.RememberBodyLocation, inspection);

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("BACKWARDS past the"));
            Assert.That(message, Does.Contain("undecodable"));
            Assert.That(message, Does.Contain("somewhere OTHER than the URL"));
        });
    }

    /// <summary>The message names how the body was supplied, so the caller can find it.</summary>
    [Test]
    public void DescribeRejection_names_where_the_body_came_from()
    {
        var body = Authority("https", WithPassword("user", "pass"), "example.invalid/x");
        var inspection = RepoContextBodyCredentials.Inspect(body);

        Assert.That(
            RepoContextBodyCredentials.DescribeRejection(
                RepoContextBodyFraming.UpdateBodyLocation, inspection),
            Does.Contain("'fields'"));
    }

    // ---- seam: remember ------------------------------------------------------

    /// <summary>The guard is wired into the <c>remember</c> seam, not merely available.</summary>
    [Test]
    public void RememberAsync_rejects_a_body_carrying_a_password_bearing_url()
    {
        var body = "Push with " + Authority(
            "https", WithPassword("x-access-token", "$" + "tok"), "github.com/o/r.git");

        Assert.That(
            () => RepoContextToolHandlers.RememberAsync(null!, "lattice", "gotchas", body: body),
            Throws.InstanceOf<McpException>()
                .With.Message.Contains("'body'")
                .And.Message.Contains("userinfo"));
    }

    /// <summary>
    /// The remedy reaches service resolution - which then fails only because this
    /// test supplies no provider - proving the guard let it through. Without this
    /// arm the guard could reject everything and still look correct.
    /// </summary>
    [Test]
    public async Task RememberAsync_accepts_a_body_using_the_recommended_placeholder()
    {
        var context = await RepoContextRequestContexts.CreateAsync(services: null);
        var body = "Push with " + Authority(
            "https", WithPassword("x-access-token", Lt + "token" + Gt), "github.com/o/r.git");

        Assert.That(
            () => RepoContextToolHandlers.RememberAsync(context, "lattice", "gotchas", body: body),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("no service provider"),
            "The guard must not block the remedy its own rejection message recommends.");
    }

    // ---- seam: update --------------------------------------------------------

    /// <summary>
    /// <c>update</c> is asserted directly rather than inferred from <c>remember</c>,
    /// because it is the path a repair pass writes through: an unguarded
    /// <c>update</c> would let the shape straight back in behind the guard.
    /// </summary>
    [Test]
    public void UpdateAsync_rejects_a_patched_body_carrying_a_password_bearing_url()
    {
        var body = "Repaired text. " + Authority(
            "https", WithPassword("user", "pass"), "example.invalid/x");

        Assert.That(
            () => RepoContextToolHandlers.UpdateAsync(
                null!,
                "repo/lattice/mem/gotchas/example",
                fields: new Dictionary<string, string> { ["body"] = body }),
            Throws.InstanceOf<McpException>().With.Message.Contains("'fields'"));
    }

    /// <summary>The field name is matched without regard to case, so no casing evades the guard.</summary>
    [Test]
    public void UpdateAsync_rejects_a_patched_body_whatever_the_case_of_the_field_name()
    {
        var body = Authority("https", WithPassword("user", "pass"), "example.invalid/x");

        Assert.That(
            () => RepoContextToolHandlers.UpdateAsync(
                null!,
                "repo/lattice/mem/gotchas/example",
                fields: new Dictionary<string, string> { ["Body"] = body }),
            Throws.InstanceOf<McpException>());
    }

    /// <summary>
    /// When a body carries both defects, the framing rejection wins. Framing means
    /// the <em>call</em> was malformed and arguments were silently dropped, which the
    /// caller must learn about before anything else; the credential URL is a problem
    /// with content that arrived intact.
    /// </summary>
    [Test]
    public void RememberAsync_reports_framing_first_when_a_body_carries_both_defects()
    {
        var body = "Note. " + Authority("https", WithPassword("user", "pass"), "example.invalid/x")
            + "\n" + Lt + "/body" + Gt + "\n" + Lt + "parameter name=\"author\"" + Gt + "someone";

        Assert.That(
            () => RepoContextToolHandlers.RememberAsync(null!, "lattice", "gotchas", body: body),
            Throws.InstanceOf<McpException>()
                .With.Message.Contains("author")
                .And.Message.Contains("framing"));
    }

    // ---- seam: the other stored free-text fields ------------------------------

    /// <summary>
    /// <strong>The shape corrupts whichever field carries it.</strong> The rewrite
    /// happens in transit, before this store sees the call, and the structural
    /// character it swallows belongs to the serialised record rather than to any
    /// particular field. A title freezes an entry exactly as a body does, so
    /// guarding only the body would leave a live path open while reading as closed.
    /// </summary>
    [TestCase("title")]
    [TestCase("author")]
    [TestCase("provenance")]
    public void RememberAsync_rejects_a_password_bearing_url_in_any_stored_text_field(string field)
    {
        var offending = Authority("https", WithPassword("user", "pass"), "example.invalid/x");

        Assert.That(
            () => RepoContextToolHandlers.RememberAsync(
                null!,
                "lattice",
                "gotchas",
                title: field == "title" ? offending : null,
                author: field == "author" ? offending : null,
                provenance: field == "provenance" ? offending : null),
            Throws.InstanceOf<McpException>().With.Message.Contains("'" + field + "'"),
            "The guard must name the field it refused, and must cover it at all.");
    }

    /// <summary>
    /// <c>update</c> patches these fields too, so an unguarded patch path would let
    /// the shape straight back in behind the guard - and a repair pass is precisely
    /// the caller most likely to be carrying one.
    /// </summary>
    [TestCase("title")]
    [TestCase("provenance")]
    public void UpdateAsync_rejects_a_password_bearing_url_in_any_patched_field(string field)
    {
        var offending = Authority("https", WithPassword("user", "pass"), "example.invalid/x");

        Assert.That(
            () => RepoContextToolHandlers.UpdateAsync(
                null!,
                "repo/lattice/mem/gotchas/example",
                fields: new Dictionary<string, string> { [field] = offending }),
            Throws.InstanceOf<McpException>().With.Message.Contains("'" + field + "'"));
    }

    /// <summary>
    /// <strong>A rejection message must never echo caller-supplied text, and a field
    /// name is caller-supplied.</strong> The inspection record carries no strings at
    /// all, so the only way body content could reach a message is through the field
    /// name, and an unrecognised name is therefore described rather than quoted.
    /// </summary>
    [Test]
    public void UpdateAsync_does_not_echo_an_unrecognised_field_name_back_to_the_caller()
    {
        var unrecognised = "not" + "AKnownField" + "Name";
        var offending = Authority("https", WithPassword("user", "pass"), "example.invalid/x");

        Assert.That(
            () => RepoContextToolHandlers.UpdateAsync(
                null!,
                "repo/lattice/mem/gotchas/example",
                fields: new Dictionary<string, string> { [unrecognised] = offending }),
            Throws.InstanceOf<McpException>()
                .With.Message.Contains("An entry of the 'fields' argument")
                .And.Message.Not.Contains(unrecognised));
    }

    /// <summary>
    /// The widened guard must not block ordinary values in those same fields, or it
    /// would refuse the very captures it exists to keep writable.
    /// </summary>
    [Test]
    public async Task RememberAsync_accepts_ordinary_values_in_the_other_text_fields()
    {
        var context = await RepoContextRequestContexts.CreateAsync(services: null);

        Assert.That(
            () => RepoContextToolHandlers.RememberAsync(
                context,
                "lattice",
                "gotchas",
                title: "Pushing with a scoped token",
                author: "backlog-worker",
                provenance: Plain("https", "github.com/o/r/pull/1")),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("no service provider"),
            "An ordinary title, author, and plain URL provenance must all pass.");
    }
}
