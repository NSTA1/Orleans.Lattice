using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppRoleCompilerTests
{
    private static readonly AppSlug NotesSlug = AppSlug.Parse(Slug);

    private static IReadOnlyList<LatticeAuthorizationRule> TwoRoleSet() => Compile(
        Manifest(Role("writer", ReadWrite, TreeScope("docs"), TreeScope("audit")), Role("reader", LatticeOperation.Read, TreeScope("docs"))),
        Bind("writer", "g-w"), Bind("reader", "g-r")).Rules;

    private static LatticeAuthorizationRule Foreign(string ruleId) =>
        new(ruleId, LatticeSubjectSelector.Group("g-x"), LatticeScope.Tree("a/notes/docs"), LatticeOperation.Read, LatticeEffect.Allow);

    [Test]
    public void ComputeDiff_upserts_the_whole_set_on_first_install()
    {
        var compiled = TwoRoleSet();

        var diff = AppRoleCompiler.ComputeDiff(NotesSlug, compiled, []);

        Assert.That(diff.ToUpsert, Is.EqualTo(compiled));
        Assert.That(diff.ToDelete, Is.Empty);
        Assert.That(diff.IsEmpty, Is.False);
    }

    [Test]
    public void ComputeDiff_of_an_unchanged_set_is_empty()
    {
        var diff = AppRoleCompiler.ComputeDiff(NotesSlug, TwoRoleSet(), TwoRoleSet().Reverse());

        Assert.That(diff.IsEmpty, Is.True);
        Assert.That(diff.ToUpsert, Is.Empty);
        Assert.That(diff.ToDelete, Is.Empty);
    }

    [Test]
    public void Whole_set_replacement_leaves_no_stale_rule_after_a_role_is_removed()
    {
        var store = new Dictionary<(string Tree, string Id), LatticeAuthorizationRule>();
        void Apply(AppRuleSetDiff diff)
        {
            foreach (var rule in diff.ToUpsert)
                store[(rule.Scope.TreeId, rule.RuleId)] = rule;
            foreach (var rule in diff.ToDelete)
                store.Remove((rule.Scope.TreeId, rule.RuleId));
        }

        var operatorRule = Foreign("ops-readers");
        store[(operatorRule.Scope.TreeId, operatorRule.RuleId)] = operatorRule;
        Apply(AppRoleCompiler.ComputeDiff(NotesSlug, TwoRoleSet(), store.Values.ToList()));

        var upgradedOk = Compile(Manifest(Role("writer", ReadWrite, TreeScope("docs"), TreeScope("audit"))), Bind("writer", "g-w")).Rules;
        var diff = AppRoleCompiler.ComputeDiff(NotesSlug, upgradedOk, store.Values.ToList());
        Apply(diff);

        Assert.That(diff.ToUpsert, Is.Empty);
        Assert.That(diff.ToDelete.Select(r => r.RuleId), Is.All.StartWith("app:notes:reader:"));
        Assert.That(diff.ToDelete, Has.Count.EqualTo(1));
        Assert.That(store.Values.Where(r => r.RuleId.StartsWith("app:notes:", StringComparison.Ordinal)), Is.EquivalentTo(upgradedOk));
        Assert.That(store.Values, Does.Contain(operatorRule));
        Assert.That(AppRoleCompiler.ComputeDiff(NotesSlug, upgradedOk, store.Values.ToList()).IsEmpty, Is.True);
    }

    [Test]
    public void ComputeDiff_replaces_a_rebound_role_by_deleting_the_old_group_rules()
    {
        var before = Compile(Manifest(Role("writer", ReadWrite, TreeScope("docs"))), Bind("writer", "g-old")).Rules;
        var after = Compile(Manifest(Role("writer", ReadWrite, TreeScope("docs"))), Bind("writer", "g-new")).Rules;

        var diff = AppRoleCompiler.ComputeDiff(NotesSlug, after, before);

        Assert.That(diff.ToUpsert, Is.EqualTo(after));
        Assert.That(diff.ToDelete, Is.EqualTo(before));
    }

    [Test]
    public void ComputeDiff_updates_an_operation_change_in_place_without_a_delete()
    {
        var before = Compile(Manifest(Role("writer", LatticeOperation.Read, TreeScope("docs"))), Bind("writer", "g-a")).Rules;
        var after = Compile(Manifest(Role("writer", ReadWrite, TreeScope("docs"))), Bind("writer", "g-a")).Rules;

        var diff = AppRoleCompiler.ComputeDiff(NotesSlug, after, before);

        Assert.That(diff.ToUpsert, Is.EqualTo(after));
        Assert.That(diff.ToDelete, Is.Empty);
    }

    [Test]
    public void ComputeDiff_ignores_operator_rules_and_other_apps_rules_including_a_slug_extension()
    {
        var compiled = TwoRoleSet();
        var stored = compiled
            .Append(Foreign("ops-readers"))
            .Append(Foreign("app:notes-archive:reader:00000000000000000000000000000000"))
            .Append(Foreign("app:other:reader:00000000000000000000000000000000"))
            .ToList();

        var diff = AppRoleCompiler.ComputeDiff(NotesSlug, compiled, stored);

        Assert.That(diff.IsEmpty, Is.True);
        Assert.That(AppRoleCompiler.ComputeDiff(NotesSlug, [], stored).ToDelete, Is.EquivalentTo(compiled));
    }

    [Test]
    public void ComputeDiff_deletes_a_stored_owned_rule_under_the_same_id_on_a_different_tree()
    {
        var compiled = Compile(Manifest(Role("writer", ReadWrite, TreeScope("docs"))), Bind("writer", "g-a")).Rules;
        var misplaced = compiled[0] with { Scope = LatticeScope.Tree("a/notes/audit") };

        var diff = AppRoleCompiler.ComputeDiff(NotesSlug, compiled, [misplaced]);

        Assert.That(diff.ToUpsert, Is.EqualTo(compiled));
        Assert.That(diff.ToDelete, Is.EqualTo(new[] { misplaced }));
    }

    [Test]
    public void ComputeDiff_orders_both_lists_by_rule_id()
    {
        var compiled = TwoRoleSet();
        var diff = AppRoleCompiler.ComputeDiff(NotesSlug, [], compiled.Reverse());
        var ids = diff.ToDelete.Select(r => r.RuleId).ToArray();

        Assert.That(ids, Is.EqualTo(ids.OrderBy(i => i, StringComparer.Ordinal).ToArray()));
    }

    [Test]
    public void ComputeDiff_rejects_invalid_arguments()
    {
        var compiled = TwoRoleSet();

        Assert.That(() => AppRoleCompiler.ComputeDiff(NotesSlug, null!, []), Throws.ArgumentNullException);
        Assert.That(() => AppRoleCompiler.ComputeDiff(NotesSlug, compiled, null!), Throws.ArgumentNullException);
        Assert.That(() => AppRoleCompiler.ComputeDiff(default, compiled, []), Throws.ArgumentException);
        Assert.That(() => AppRoleCompiler.ComputeDiff(NotesSlug, [null!], []), Throws.ArgumentException);
        Assert.That(() => AppRoleCompiler.ComputeDiff(NotesSlug, compiled, [null!]), Throws.ArgumentException);
        Assert.That(() => AppRoleCompiler.ComputeDiff(NotesSlug, [Foreign("ops-readers")], []), Throws.ArgumentException);
        Assert.That(() => AppRoleCompiler.ComputeDiff(NotesSlug, [compiled[0], compiled[0]], []), Throws.ArgumentException);
        Assert.That(
            () => AppRoleCompiler.ComputeDiff(AppSlug.Parse("other"), compiled, []),
            Throws.ArgumentException);
    }
}
