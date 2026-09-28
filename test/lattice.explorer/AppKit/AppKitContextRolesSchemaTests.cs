using Orleans.Lattice.Explorer.AppKit;

namespace Orleans.Lattice.Explorer.Tests.AppKit;

/// <summary>
/// The optional <c>roles</c> member of the <c>context.read</c> result (issue #3817 follow-up):
/// the caller's app role names, covered by the existing <c>context.read</c> consent. It is
/// optional, so a result from a host that predates it stays valid, and closed, so nothing
/// but well-formed role names can ride in it.
/// </summary>
[TestFixture]
public sealed class AppKitContextRolesSchemaTests
{
    private const string HostToFrame = "hostToFrame";

    private static readonly ProtocolSchema Schema = ProtocolSchema.Load();

    private static string Result(string roles) =>
        $$"""{ "id": 1, "ok": true, "result": { "slug": "task-board", "version": "1.2.0", "protocol": 1, "theme": "paper", "contrast": "standard", "density": "comfortable", "reducedMotion": false, "tenant": null{{roles}} } }""";

    private static IEnumerable<TestCaseData> ValidRoles()
    {
        yield return new TestCaseData(string.Empty).SetArgDisplayNames("absent (an older host)");
        yield return new TestCaseData(""", "roles": [] """).SetArgDisplayNames("no roles");
        yield return new TestCaseData(""", "roles": ["viewer"] """).SetArgDisplayNames("one role");
        yield return new TestCaseData(""", "roles": ["viewer", "task_editor", "admin-2"] """).SetArgDisplayNames("several roles");
        yield return new TestCaseData($$""", "roles": ["{{new string('r', AppKitProtocol.Limits.MaxRoleNameLength)}}"] """).SetArgDisplayNames("longest name");
        yield return new TestCaseData($$""", "roles": [{{string.Join(", ", Enumerable.Range(0, AppKitProtocol.Limits.MaxRoles).Select(i => $"\"r{i}\""))}}] """).SetArgDisplayNames("most roles");
    }

    private static IEnumerable<TestCaseData> InvalidRoles()
    {
        yield return new TestCaseData(""", "roles": null """).SetArgDisplayNames("null");
        yield return new TestCaseData(""", "roles": "viewer" """).SetArgDisplayNames("a string");
        yield return new TestCaseData(""", "roles": [""] """).SetArgDisplayNames("an empty name");
        yield return new TestCaseData(""", "roles": ["Viewer"] """).SetArgDisplayNames("upper case");
        yield return new TestCaseData(""", "roles": ["1st"] """).SetArgDisplayNames("leading digit");
        yield return new TestCaseData(""", "roles": ["a/b"] """).SetArgDisplayNames("a slash");
        yield return new TestCaseData(""", "roles": ["app:task-board:viewer"] """).SetArgDisplayNames("a rule id");
        yield return new TestCaseData(""", "roles": ["<b>x</b>"] """).SetArgDisplayNames("markup");
        yield return new TestCaseData(""", "roles": [{ "name": "viewer" }] """).SetArgDisplayNames("an object");
        yield return new TestCaseData(""", "roles": [7] """).SetArgDisplayNames("a number");
        yield return new TestCaseData($$""", "roles": ["{{new string('r', AppKitProtocol.Limits.MaxRoleNameLength + 1)}}"] """).SetArgDisplayNames("name too long");
        yield return new TestCaseData($$""", "roles": [{{string.Join(", ", Enumerable.Range(0, AppKitProtocol.Limits.MaxRoles + 1).Select(i => $"\"r{i}\""))}}] """).SetArgDisplayNames("too many roles");
        yield return new TestCaseData(""", "roles": ["viewer"], "groups": ["g"] """).SetArgDisplayNames("a sibling member");
    }

    [TestCaseSource(nameof(ValidRoles))]
    public void A_context_read_result_with_well_formed_roles_is_valid(string roles)
    {
        Assert.That(Schema.Validate(HostToFrame, Result(roles)), Is.Empty);
    }

    [TestCaseSource(nameof(InvalidRoles))]
    public void A_context_read_result_with_malformed_roles_is_invalid(string roles)
    {
        Assert.That(Schema.IsValid(HostToFrame, Result(roles)), Is.False);
    }

    [Test]
    public void Roles_are_optional_and_bounded_by_the_protocol_limits()
    {
        var result = Schema.Definition("contextReadResult");
        var roles = result.GetProperty("properties").GetProperty("roles");
        var name = Schema.Definition("roleName");
        var limits = Schema.Root.GetProperty("x-lattice-limits");

        Assert.Multiple(() =>
        {
            Assert.That(result.GetProperty("required").EnumerateArray().Select(e => e.GetString()), Does.Not.Contain("roles"));
            Assert.That(roles.GetProperty("maxItems").GetInt32(), Is.EqualTo(AppKitProtocol.Limits.MaxRoles));
            Assert.That(name.GetProperty("maxLength").GetInt32(), Is.EqualTo(AppKitProtocol.Limits.MaxRoleNameLength));
            Assert.That(name.GetProperty("pattern").GetString(), Is.EqualTo(AppKitProtocol.TreeNamePattern), "a role name follows the manifest name rule, as a tree name does");
            Assert.That(limits.GetProperty("maxRoles").GetInt32(), Is.EqualTo(AppKitProtocol.Limits.MaxRoles));
            Assert.That(limits.GetProperty("maxRoleNameLength").GetInt32(), Is.EqualTo(AppKitProtocol.Limits.MaxRoleNameLength));
            Assert.That(AppKitProtocol.Limits.MaxRoles, Is.EqualTo(256), "the manifest's section bound");
            Assert.That(AppKitProtocol.Limits.MaxRoleNameLength, Is.EqualTo(128), "the manifest's name bound");
        });
    }
}
