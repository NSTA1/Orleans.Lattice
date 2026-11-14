using static Orleans.Lattice.Auth.Tests.TenantLayerTestRules;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Decision tests for subject-relative (templated) scopes (issue #6061), exercised
/// through <see cref="CompiledPolicy"/> and <see cref="PolicyEvaluator"/> without a
/// cluster: each caller is granted exactly their own prefix, the segment boundary and
/// the anonymous caller are denied, a templated prefix ranks by its resolved length
/// and competes with a literal rule on the same resolved prefix, group placeholders
/// follow the caller's current group set, range requests stay uniform only inside the
/// resolved prefix, and the existence probe sees a templated grant.
/// </summary>
[TestFixture]
public sealed class TemplatedScopeEvaluationTests
{
    private const string Images = "images";
    private const string Audience = "aud";

    private static readonly LatticeSubjectSelector Members = LatticeSubjectSelector.Group("members");

    private static readonly LatticeAuthorizationRule OwnerWrite = new(
        "owner-write",
        Members,
        LatticeScope.Prefix(Images, LatticeScope.SubjectPlaceholder + "/"),
        LatticeOperation.Write | LatticeOperation.Read,
        LatticeEffect.Allow);

    private static readonly LatticeAuthorizationRule GroupRead = new(
        "group-read",
        Members,
        LatticeScope.Prefix(Audience, "g/" + LatticeScope.GroupPlaceholder("grp:") + "/"),
        LatticeOperation.Read | LatticeOperation.RangeRead,
        LatticeEffect.Allow);

    private static LatticeAccessDecision Eval(
        IEnumerable<LatticeAuthorizationRule> rules,
        LatticeSubject subject,
        string tree,
        LatticeOperation operation,
        string? key,
        string? rangeStart = null,
        string? rangeEnd = null) =>
        PolicyEvaluator.Evaluate(
            CompiledPolicy.Compile(rules), new LatticeAuthOptions(), subject, tree, operation, key, rangeStart, rangeEnd);

    private static bool Allowed(IEnumerable<LatticeAuthorizationRule> rules, LatticeSubject subject, string key, LatticeOperation op = LatticeOperation.Write) =>
        Eval(rules, subject, Images, op, key).Allowed;

    // ---- {subject} ---------------------------------------------------------

    [Test]
    public void Subject_prefix_grants_each_caller_their_own_prefix_and_nobody_elses()
    {
        LatticeAuthorizationRule[] rules = [OwnerWrite];

        Assert.Multiple(() =>
        {
            Assert.That(Allowed(rules, Subject("u1", "members"), "u1/cat.jpg"), Is.True);
            Assert.That(Allowed(rules, Subject("u2", "members"), "u2/a/b.jpg"), Is.True);
            Assert.That(Allowed(rules, Subject("u1", "members"), "u2/cat.jpg"), Is.False, "another caller's prefix");
            Assert.That(Allowed(rules, Subject("u1"), "u1/cat.jpg"), Is.False, "the rule's own subject selector still applies");
        });
    }

    [Test]
    public void Subject_prefix_is_segment_bounded()
    {
        LatticeAuthorizationRule[] rules = [OwnerWrite];
        var u1 = Subject("u1", "members");

        Assert.Multiple(() =>
        {
            Assert.That(Allowed(rules, u1, "u10/cat.jpg"), Is.False, "u1 must not match u10");
            Assert.That(Allowed(rules, Subject("u10", "members"), "u1/cat.jpg"), Is.False, "u10 must not match u1");
            Assert.That(Allowed(rules, u1, "u1"), Is.False, "no delimiter, no match");
            Assert.That(Allowed(rules, u1, "u1/"), Is.True, "the resolved prefix itself is under the prefix");
        });
    }

    [Test]
    public void Subject_id_containing_the_delimiter_or_unit_separator_never_matches()
    {
        LatticeAuthorizationRule[] rules = [OwnerWrite];

        Assert.Multiple(() =>
        {
            Assert.That(Allowed(rules, Subject("a/b", "members"), "a/b/x"), Is.False);
            Assert.That(Allowed(rules, Subject("a", "members"), "a/b/x"), Is.True, "control: 'a' owns a/");
            Assert.That(Allowed(rules, Subject("a\u001fb", "members"), "a\u001fb/x"), Is.False);
        });
    }

    [Test]
    public void Anonymous_caller_never_matches_a_templated_scope()
    {
        var rule = new LatticeAuthorizationRule(
            "anyone",
            LatticeSubjectSelector.User(LatticeSubject.AnonymousSubjectId),
            LatticeScope.Prefix(Images, "{subject}/"),
            LatticeOperation.Write,
            LatticeEffect.Allow);

        Assert.That(Allowed([rule], LatticeSubject.Anonymous, "anonymous/x"), Is.False);
    }

    [Test]
    public void Templated_key_matches_only_the_callers_resolved_key()
    {
        var rule = new LatticeAuthorizationRule(
            "profile", Members, LatticeScope.Key(Images, "{subject}/profile"), LatticeOperation.Write, LatticeEffect.Allow);
        var u1 = Subject("u1", "members");

        Assert.Multiple(() =>
        {
            Assert.That(Allowed([rule], u1, "u1/profile"), Is.True);
            Assert.That(Allowed([rule], u1, "u1/profile2"), Is.False, "an exact key, not a prefix");
            Assert.That(Allowed([rule], u1, "u2/profile"), Is.False);
        });
    }

    // ---- Precedence ---------------------------------------------------------

    [Test]
    public void Fixed_user_deny_on_the_same_resolved_prefix_overrides_the_templated_allow()
    {
        var deny = new LatticeAuthorizationRule(
            "ban-u1", LatticeSubjectSelector.User("u1"), LatticeScope.Prefix(Images, "u1/"), LatticeOperation.Write, LatticeEffect.Deny);
        LatticeAuthorizationRule[] rules = [OwnerWrite, deny];

        var decision = PolicyEvaluator.Evaluate(
            CompiledPolicy.Compile(rules), new LatticeAuthOptions(), Subject("u1", "members"), Images, LatticeOperation.Write,
            "u1/x", null, null, out var match);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(match.RuleId, Is.EqualTo("ban-u1"));
            Assert.That(Allowed(rules, Subject("u2", "members"), "u2/x"), Is.True, "other callers are unaffected");
        });
    }

    [Test]
    public void Templated_deny_for_a_group_overrides_a_templated_allow_at_the_same_prefix()
    {
        var suspended = new LatticeAuthorizationRule(
            "suspended", LatticeSubjectSelector.Group("suspended"), LatticeScope.Prefix(Images, "{subject}/"),
            LatticeOperation.Write, LatticeEffect.Deny);
        LatticeAuthorizationRule[] rules = [OwnerWrite, suspended];

        Assert.Multiple(() =>
        {
            Assert.That(Allowed(rules, Subject("u1", "members", "suspended"), "u1/x"), Is.False);
            Assert.That(Allowed(rules, Subject("u2", "members"), "u2/x"), Is.True);
        });
    }

    [Test]
    public void Templated_prefix_ranks_by_its_resolved_length()
    {
        // "u1/" (3) is longer than the literal "u" (1) and shorter than "u1/private/" (11).
        var shortDeny = new LatticeAuthorizationRule(
            "short-deny", Members, LatticeScope.Prefix(Images, "u"), LatticeOperation.Write, LatticeEffect.Deny);
        var longDeny = new LatticeAuthorizationRule(
            "long-deny", Members, LatticeScope.Prefix(Images, "u1/private/"), LatticeOperation.Write, LatticeEffect.Deny);
        LatticeAuthorizationRule[] rules = [OwnerWrite, shortDeny, longDeny];
        var u1 = Subject("u1", "members");

        Assert.Multiple(() =>
        {
            Assert.That(Allowed(rules, u1, "u1/public/x"), Is.True, "the longer resolved templated prefix beats a shorter literal deny");
            Assert.That(Allowed(rules, u1, "u1/private/x"), Is.False, "a longer literal prefix beats the templated allow");
        });
    }

    [Test]
    public void Decision_reason_names_the_template_as_written()
    {
        PolicyEvaluator.Evaluate(
            CompiledPolicy.Compile([OwnerWrite]), new LatticeAuthOptions(), Subject("u1", "members"), Images,
            LatticeOperation.Write, "u1/x", null, null, out var match);

        Assert.Multiple(() =>
        {
            Assert.That(match.RuleId, Is.EqualTo("owner-write"));
            Assert.That(match.ScopeKind, Is.EqualTo(LatticeScopeKind.Prefix));
            Assert.That(match.ScopeValue, Is.EqualTo("{subject}/"));
        });
    }

    // ---- {group:...} ---------------------------------------------------------

    [Test]
    public void Group_placeholder_grants_the_groups_the_caller_is_in_with_the_prefix_stripped()
    {
        LatticeAuthorizationRule[] rules = [GroupRead];

        Assert.Multiple(() =>
        {
            Assert.That(Eval(rules, Subject("u1", "members", "grp:hikers"), Audience, LatticeOperation.Read, "g/hikers/1").Allowed, Is.True);
            Assert.That(Eval(rules, Subject("u1", "members", "grp:hikers"), Audience, LatticeOperation.Read, "g/bikers/1").Allowed, Is.False);
            Assert.That(Eval(rules, Subject("u1", "members", "hikers"), Audience, LatticeOperation.Read, "g/hikers/1").Allowed, Is.False, "the group must carry the prefix");
        });
    }

    [Test]
    public void Removing_the_caller_from_the_group_revokes_access()
    {
        LatticeAuthorizationRule[] rules = [GroupRead];
        var policy = CompiledPolicy.Compile(rules);
        var options = new LatticeAuthOptions();

        var before = PolicyEvaluator.Evaluate(policy, options, Subject("u1", "members", "grp:hikers"), Audience, LatticeOperation.Read, "g/hikers/1", null, null);
        var after = PolicyEvaluator.Evaluate(policy, options, Subject("u1", "members"), Audience, LatticeOperation.Read, "g/hikers/1", null, null);

        Assert.Multiple(() =>
        {
            Assert.That(before.Allowed, Is.True);
            Assert.That(after.Allowed, Is.False, "the same compiled snapshot, the caller's new group set");
        });
    }

    [TestCaseSource(nameof(GroupClosures))]
    public void Group_placeholder_matches_every_closure_shape(IReadOnlyCollection<string> groups)
    {
        var subject = new LatticeSubject("u1", groups);

        Assert.Multiple(() =>
        {
            Assert.That(Eval([GroupRead], subject, Audience, LatticeOperation.Read, "g/hikers/1").Allowed, Is.True);
            Assert.That(Eval([GroupRead], subject, Audience, LatticeOperation.Read, "g/bikers/1").Allowed, Is.False);
        });
    }

    private static IEnumerable<IReadOnlyCollection<string>> GroupClosures()
    {
        string[] ids = ["members", "grp:hikers"];
        yield return ids;
        yield return new List<string>(ids);
        yield return new HashSet<string>(ids, StringComparer.Ordinal);
        yield return System.Collections.Frozen.FrozenSet.ToFrozenSet(ids, StringComparer.Ordinal);
        yield return new System.Collections.ObjectModel.ReadOnlyCollection<string>(ids);
    }

    // ---- Ranges ---------------------------------------------------------------

    [Test]
    public void Range_inside_the_callers_resolved_prefix_is_a_plain_allow()
    {
        var decision = Eval([OwnerWrite], Subject("u1", "members"), Images, LatticeOperation.Read, null, "u1/", "u10");

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True);
            Assert.That(decision.KeyFilter, Is.Null, "the whole range is inside u1/");
        });
    }

    [Test]
    public void Range_reaching_beyond_the_callers_resolved_prefix_is_filtered_per_key()
    {
        var decision = Eval([OwnerWrite], Subject("u1", "members"), Images, LatticeOperation.Read, null, "u", "v");

        Assert.Multiple(() =>
        {
            Assert.That(decision.KeyFilter, Is.Not.Null);
            Assert.That(decision.KeyFilter!("u1/x"), Is.True);
            Assert.That(decision.KeyFilter!("u10/x"), Is.False);
            Assert.That(decision.KeyFilter!("u2/x"), Is.False);
        });
    }

    [Test]
    public void Range_over_another_callers_prefix_admits_nothing()
    {
        var decision = Eval([OwnerWrite], Subject("u1", "members"), Images, LatticeOperation.Read, null, "u10/", "u100");

        Assert.That(decision.Allowed && decision.KeyFilter is null, Is.False);
        Assert.That(decision.KeyFilter?.Invoke("u10/x") ?? decision.Allowed, Is.False);
    }

    [Test]
    public void Range_inside_a_resolved_prefix_with_a_templated_deny_on_a_narrower_prefix_is_filtered()
    {
        var narrowDeny = new LatticeAuthorizationRule(
            "private", Members, LatticeScope.Prefix(Images, "{subject}/private/"), LatticeOperation.Read, LatticeEffect.Deny);

        var decision = Eval([OwnerWrite, narrowDeny], Subject("u1", "members"), Images, LatticeOperation.Read, null, "u1/", "u10");

        Assert.Multiple(() =>
        {
            Assert.That(decision.KeyFilter, Is.Not.Null);
            Assert.That(decision.KeyFilter!("u1/public/a"), Is.True);
            Assert.That(decision.KeyFilter!("u1/private/a"), Is.False);
        });
    }

    // ---- Existence probe ---------------------------------------------------------

    [Test]
    public void HasAnyGrant_sees_a_templated_grant_for_a_matching_caller_only()
    {
        var policy = CompiledPolicy.Compile([OwnerWrite]);
        var options = new LatticeAuthOptions();

        Assert.Multiple(() =>
        {
            Assert.That(PolicyEvaluator.HasAnyGrant(policy, options, Subject("u1", "members"), Images, LatticeOperation.Read), Is.True);
            Assert.That(PolicyEvaluator.HasAnyGrant(policy, options, Subject("u1"), Images, LatticeOperation.Read), Is.False);
            Assert.That(PolicyEvaluator.HasAnyGrant(policy, options, LatticeSubject.Anonymous, Images, LatticeOperation.Read), Is.False);
        });
    }

    [Test]
    public void HasAnyGrant_in_the_tenant_layer_sees_a_templated_tenant_grant()
    {
        var rule = Tenant("own", Members, LatticeScope.Prefix(TenantTree, "{subject}/"), LatticeOperation.Read, LatticeEffect.Allow);
        var policy = CompiledPolicy.Compile([rule], includeTenantLayer: true);
        var options = new LatticeAuthOptions();

        Assert.Multiple(() =>
        {
            Assert.That(PolicyEvaluator.HasAnyGrant(policy, options, Subject("u1", "members"), TenantTree, LatticeOperation.Read, tenantLayerActive: true), Is.True);
            Assert.That(PolicyEvaluator.HasAnyGrant(policy, options, Subject("u1"), TenantTree, LatticeOperation.Read, tenantLayerActive: true), Is.False);
            Assert.That(Evaluate([rule], Subject("u1", "members"), TenantTree, LatticeOperation.Read, "u1/x").Allowed, Is.True);
            Assert.That(Evaluate([rule], Subject("u1", "members"), TenantTree, LatticeOperation.Read, "u2/x").Allowed, Is.False);
        });
    }

    // ---- Literal fallbacks ---------------------------------------------------------

    [Test]
    public void Invalid_template_that_reached_the_policy_compiles_as_a_literal()
    {
        // No delimiter after the placeholder: the store refuses it, but a replicated or
        // restored copy keeps the literal meaning it had before templates existed.
        var rule = new LatticeAuthorizationRule(
            "legacy", Members, LatticeScope.Prefix(Images, "{subject}"), LatticeOperation.Write, LatticeEffect.Allow);

        Assert.Multiple(() =>
        {
            Assert.That(Allowed([rule], Subject("u1", "members"), "u1"), Is.False);
            Assert.That(Allowed([rule], Subject("u1", "members"), "{subject}x"), Is.True);
        });
    }

    [Test]
    public void App_owned_rule_always_matches_literally()
    {
        var rule = new LatticeAuthorizationRule(
            LatticeAppRuleIds.Prefix + "photos:viewer", Members, LatticeScope.Prefix(Images, "{subject}/"),
            LatticeOperation.Write, LatticeEffect.Allow);

        Assert.Multiple(() =>
        {
            Assert.That(Allowed([rule], Subject("u1", "members"), "u1/x"), Is.False);
            Assert.That(Allowed([rule], Subject("u1", "members"), "{subject}/x"), Is.True);
        });
    }

    [Test]
    public void Template_on_a_control_plane_tree_compiles_as_a_literal()
    {
        var rule = new LatticeAuthorizationRule(
            "cp", Members, LatticeScope.Prefix("sys-membership-edges", "{subject}/"), LatticeOperation.Read, LatticeEffect.Allow);

        var decision = Eval([rule], Subject("u1", "members"), "sys-membership-edges", LatticeOperation.Read, "u1/x");

        Assert.That(decision.Allowed, Is.False);
    }
}
