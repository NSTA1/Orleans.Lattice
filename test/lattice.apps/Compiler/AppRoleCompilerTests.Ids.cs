using System.Text.RegularExpressions;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppRoleCompilerTests
{
    [Test]
    public void Rule_ids_start_with_the_app_owned_prefix_and_follow_the_documented_shape()
    {
        var result = Compile(
            Manifest(Role("writer", ReadWrite, TreeScope("docs"), KeyScope("audit", "k")), Role("reader", LatticeOperation.Read, TreeScope("docs"))),
            Bind("writer", "g-a"), Bind("reader", "g-a"));

        Assert.That(result.Rules, Has.Count.EqualTo(3));
        foreach (var rule in result.Rules)
        {
            Assert.That(LatticeAppRuleIds.IsAppOwned(rule.RuleId), Is.True);
            Assert.That(rule.RuleId, Does.StartWith(AppRoleCompiler.GetOwnedRuleIdPrefix(AppSlug.Parse(Slug))));
            Assert.That(rule.RuleId, Does.Match(new Regex("^app:notes:(writer|reader):[0-9a-f]{32}$")));
        }
    }

    [Test]
    public void Rule_ids_are_pinned_to_the_documented_derivation()
    {
        // Golden values computed independently of the implementation from the documented encoding:
        // SHA-256 over length-prefixed UTF-8 fields, first 16 bytes as lowercase hex.
        var manifest = Manifest(Role("writer", ReadWrite, TreeScope("docs")), Role("reader", LatticeOperation.Read, PrefixScope("docs", "drafts/")));

        var plain = Compile(manifest, Bind("writer", "g-writers"));
        var tenanted = AppRoleCompiler.Compile(
            manifest, TenantId.Parse("acme"), [Bind("reader", "g-readers")], Ceiling(LatticeAuthOperations.All));

        Assert.That(plain.Rules.Single().RuleId, Is.EqualTo("app:notes:writer:ea7a090e6feea2bd15f86e7b69ca7023"));
        Assert.That(tenanted.Rules.Single().RuleId, Is.EqualTo("app:notes:reader:c451110902229ff0b420f686d21809b5"));
    }

    [Test]
    public void The_same_group_bound_to_two_roles_yields_distinct_ids()
    {
        var result = Compile(
            Manifest(Role("writer", ReadWrite, TreeScope("docs")), Role("reader", LatticeOperation.Read, TreeScope("docs"))),
            Bind("writer", "g-a"), Bind("reader", "g-a"));

        Assert.That(result.Rules, Has.Count.EqualTo(2));
        Assert.That(result.Rules.Select(r => r.RuleId).Distinct().Count(), Is.EqualTo(2));
    }

    [Test]
    public void Distinct_groups_and_scopes_of_one_role_yield_distinct_ids()
    {
        var result = Compile(
            Manifest(Role("writer", ReadWrite, TreeScope("docs"), KeyScope("docs", "k"), PrefixScope("docs", "k"), TreeScope("audit"))),
            Bind("writer", "g-a"), Bind("writer", "g-b"));

        Assert.That(result.Rules, Has.Count.EqualTo(8));
        Assert.That(result.Rules.Select(r => r.RuleId).Distinct().Count(), Is.EqualTo(8));
    }

    [Test]
    public void Rule_ids_do_not_depend_on_operations_so_an_operation_change_updates_in_place()
    {
        var narrow = Compile(Manifest(Role("writer", LatticeOperation.Read, TreeScope("docs"))), Bind("writer", "g-a"));
        var wide = Compile(Manifest(Role("writer", ReadWrite, TreeScope("docs"))), Bind("writer", "g-a"));

        Assert.That(wide.Rules.Single().RuleId, Is.EqualTo(narrow.Rules.Single().RuleId));
        Assert.That(wide.Rules.Single().Operations, Is.EqualTo(ReadWrite));
    }

    [Test]
    public void ComputeRuleId_is_stable_for_inputs_larger_than_the_stack_buffer()
    {
        var prefix = AppRoleCompiler.GetOwnedRuleIdPrefix(AppSlug.Parse(Slug));
        var scope = LatticeScope.Key("a/notes/docs", new string('k', 4096));

        var first = AppRoleCompiler.ComputeRuleId(prefix, Slug, "writer", "g-a", scope);
        var second = AppRoleCompiler.ComputeRuleId(prefix, Slug, "writer", "g-a", scope with { KeyOrPrefix = new string('k', 4096) });
        var shorter = AppRoleCompiler.ComputeRuleId(prefix, Slug, "writer", "g-a", scope with { KeyOrPrefix = new string('k', 4095) });

        Assert.That(second, Is.EqualTo(first));
        Assert.That(shorter, Is.Not.EqualTo(first));
        Assert.That(first, Does.Match("^app:notes:writer:[0-9a-f]{32}$"));
    }

    [Test]
    public void ComputeRuleId_separates_a_tree_scope_from_an_empty_field_boundary_shift()
    {
        var prefix = AppRoleCompiler.GetOwnedRuleIdPrefix(AppSlug.Parse(Slug));

        var a = AppRoleCompiler.ComputeRuleId(prefix, Slug, "writer", "g-ab", LatticeScope.Tree("a/notes/c"));
        var b = AppRoleCompiler.ComputeRuleId(prefix, Slug, "writer", "g-a", LatticeScope.Tree("ba/notes/c"));

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void GetOwnedRuleIdPrefix_returns_the_slug_scoped_app_prefix()
    {
        Assert.That(AppRoleCompiler.GetOwnedRuleIdPrefix(AppSlug.Parse("notes")), Is.EqualTo("app:notes:"));
        Assert.That(AppRoleCompiler.GetOwnedRuleIdPrefix(AppSlug.Parse("notes-archive")), Is.EqualTo("app:notes-archive:"));
    }

    [Test]
    public void GetOwnedRuleIdPrefix_rejects_an_uninitialised_slug()
    {
        Assert.That(() => AppRoleCompiler.GetOwnedRuleIdPrefix(default), Throws.ArgumentException);
    }
}
