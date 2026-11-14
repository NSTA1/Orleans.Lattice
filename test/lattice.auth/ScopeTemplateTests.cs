namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for <see cref="ScopeTemplate"/>, the parsed form of a subject-relative
/// key or prefix (issue #6061): parsing, the compile-time literal fallbacks,
/// matching, and the bounded enumeration of a template's instantiations.
/// </summary>
[TestFixture]
public sealed class ScopeTemplateTests
{
    private static readonly LatticeSubject U1 = new("u1", new HashSet<string>(["grp:a", "grp:b", "other"], StringComparer.Ordinal));

    private static ScopeTemplate Parse(string text)
    {
        Assert.That(ScopeTemplate.TryParse(text, out var template, out var error), Is.True, error);
        return template!;
    }

    private static LatticeAuthorizationRule Rule(LatticeScope scope, string id = "r") =>
        new(id, LatticeSubjectSelector.Group("members"), scope, LatticeOperation.Read, LatticeEffect.Allow);

    [TestCase(null, ExpectedResult = false)]
    [TestCase("plain/", ExpectedResult = false)]
    [TestCase("{subject}/", ExpectedResult = true)]
    [TestCase("x{group:", ExpectedResult = true)]
    public bool ContainsPlaceholder_detects_a_placeholder_token(string? text) => ScopeTemplate.ContainsPlaceholder(text);

    [Test]
    public void TryParse_of_a_literal_reports_no_error()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ScopeTemplate.TryParse("{other}/", out var template, out var error), Is.False);
            Assert.That(template, Is.Null);
            Assert.That(error, Is.Null, "a literal is not an invalid template");
        });
    }

    [Test]
    public void TryParse_keeps_the_text_as_written()
    {
        Assert.That(Parse("a/{subject}/b/{group:g}:").Text, Is.EqualTo("a/{subject}/b/{group:g}:"));
    }

    [TestCase("sys-x", ExpectedResult = true)]
    [TestCase("_lattice_x", ExpectedResult = true)]
    [TestCase("tag-x", ExpectedResult = true)]
    [TestCase("orders", ExpectedResult = false)]
    public bool IsForbiddenTree_names_control_plane_trees(string tree) => ScopeTemplate.IsForbiddenTree(tree);

    [Test]
    public void ForCompile_returns_null_for_literal_and_refused_scopes()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ScopeTemplate.ForCompile(Rule(LatticeScope.Tree("t"))), Is.Null);
            Assert.That(ScopeTemplate.ForCompile(Rule(LatticeScope.Prefix("t", "plain/"))), Is.Null);
            Assert.That(ScopeTemplate.ForCompile(Rule(LatticeScope.Prefix("t", "{subject}"))), Is.Null, "invalid");
            Assert.That(ScopeTemplate.ForCompile(Rule(LatticeScope.Prefix("sys-x", "{subject}/"))), Is.Null, "control plane");
            Assert.That(ScopeTemplate.ForCompile(Rule(LatticeScope.Prefix("t", "{group:t/}/"))), Is.Null, "unconfined tenant groups");
            Assert.That(ScopeTemplate.ForCompile(Rule(LatticeScope.Prefix("t", "{subject}/"), LatticeAppRuleIds.Prefix + "x:y")), Is.Null, "app-owned");
            Assert.That(ScopeTemplate.ForCompile(Rule(LatticeScope.Prefix("t", "{subject}/"))), Is.Not.Null);
        });
    }

    [Test]
    public void Validate_returns_null_for_an_app_owned_rule()
    {
        Assert.That(ScopeTemplate.Validate(Rule(LatticeScope.Prefix("t", "{subject}"), LatticeAppRuleIds.Prefix + "x:y")), Is.Null);
    }

    [Test]
    public void TryMatchPrefix_reports_the_resolved_length()
    {
        var template = Parse("u/{subject}/");

        Assert.Multiple(() =>
        {
            Assert.That(template.TryMatchPrefix("u/u1/x", U1, out var length), Is.True);
            Assert.That(length, Is.EqualTo("u/u1/".Length));
            Assert.That(template.TryMatchPrefix("v/u1/x", U1, out _), Is.False);
            Assert.That(template.TryMatchPrefix("u//x", U1, out _), Is.False, "an empty segment never matches");
        });
    }

    [Test]
    public void Group_placeholder_matches_a_group_longer_than_the_stack_buffer()
    {
        var longName = new string('x', 300);
        var subject = new LatticeSubject("u1", new HashSet<string>(["grp:" + longName], StringComparer.Ordinal));

        Assert.That(Parse("{group:grp:}/").MatchesKey(longName + "/", subject), Is.True);
    }

    [Test]
    public void AppendInstantiations_produces_the_cross_product_of_placeholder_choices()
    {
        var values = new List<string>();

        var complete = Parse("{subject}/{group:grp:}/").AppendInstantiations(U1, values);

        Assert.Multiple(() =>
        {
            Assert.That(complete, Is.True);
            Assert.That(values, Is.EquivalentTo(new[] { "u1/a/", "u1/b/" }));
        });
    }

    [Test]
    public void AppendInstantiations_skips_unsubstitutable_ids_and_the_anonymous_subject()
    {
        var values = new List<string>();
        var template = Parse("{group:g}/");
        var subject = new LatticeSubject("u1", ["g", "gx/y", "gz\u001f", "gok"]);

        template.AppendInstantiations(subject, values);
        template.AppendInstantiations(LatticeSubject.Anonymous, values);

        Assert.That(values, Is.EqualTo(new[] { "ok/" }));
    }

    [Test]
    public void AppendInstantiations_stops_at_the_cap_and_reports_it()
    {
        var groups = Enumerable.Range(0, 100).Select(i => "g" + i).ToArray();
        var subject = new LatticeSubject("u1", groups);
        var values = new List<string>();

        var complete = Parse("{group:g}/{group:g}/").AppendInstantiations(subject, values);

        Assert.Multiple(() =>
        {
            Assert.That(complete, Is.False);
            Assert.That(values, Has.Count.EqualTo(ScopeTemplate.MaxInstantiations));
        });
    }

    [Test]
    public void A_placeholder_no_group_can_fill_ends_the_walk_before_expanding_the_others()
    {
        // Seven placeholders each fill from 40 groups and the eighth from none: without
        // the up-front check the walk would expand 40^7 branches that never complete.
        var groups = Enumerable.Range(0, 40).Select(i => "t/T/g" + i).ToArray();
        var subject = new LatticeSubject("u1", groups);
        var template = Parse(string.Concat(Enumerable.Repeat("{group:t/T/}/", 7)) + "{group:t/T/zz-none}/");
        var values = new List<string>();

        var complete = template.AppendInstantiations(subject, values);

        Assert.Multiple(() =>
        {
            Assert.That(complete, Is.True);
            Assert.That(values, Is.Empty);
        });
    }

    [Test]
    public void The_step_budget_ends_a_walk_that_completes_too_few_instantiations_to_reach_the_cap()
    {
        // One placeholder over a group set larger than the step budget: one match, but
        // scanning for it exceeds MaxWalkSteps, so the walk reports itself incomplete.
        var groups = Enumerable.Range(0, ScopeTemplate.MaxWalkSteps + 10).Select(i => "x" + i).Append("g:a").ToArray();
        var subject = new LatticeSubject("u1", groups);
        var values = new List<string>();

        var complete = Parse("{group:g:}/").AppendInstantiations(subject, values);

        Assert.That(complete, Is.False);
    }

    [Test]
    public void Truncated_enumeration_keeps_a_range_filtered_per_key()
    {
        var groups = Enumerable.Range(0, ScopeTemplate.MaxWalkSteps + 10).Select(i => "x" + i).Append("members").Append("g:a").ToArray();
        var subject = new LatticeSubject("u1", groups);
        var policy = CompiledPolicy.Compile([Rule(LatticeScope.Prefix("t", "{group:g:}/"))]);

        var decision = PolicyEvaluator.Evaluate(policy, new LatticeAuthOptions(), subject, "t", LatticeOperation.Read, null, "a/", "a0");

        Assert.That(decision.KeyFilter, Is.Not.Null, "an incomplete walk cannot prove the range uniform");
    }

    [Test]
    public void Existence_probe_work_is_bounded_across_all_templated_rules_of_a_tree()
    {
        // 300 templated denies, each resolving to 64 x 64 = 4096 keys for the caller.
        // Unbounded, the probe would resolve 1.2M positions against all 300 rules each.
        var groups = Enumerable.Range(0, 64).Select(i => "t/acme/g" + i).Append("members").ToArray();
        var subject = new LatticeSubject("u1", groups);
        var rules = Enumerable.Range(0, 300)
            .Select(i => new LatticeAuthorizationRule(
                "deny" + i, LatticeSubjectSelector.Group("members"),
                LatticeScope.Key("t/acme/data", "{group:t/acme/}/{group:t/acme/}/k" + i + "/"),
                LatticeOperation.Read, LatticeEffect.Deny))
            .ToArray();
        var policy = CompiledPolicy.Compile(rules);

        var granted = PolicyEvaluator.HasAnyGrant(policy, new LatticeAuthOptions(), subject, "t/acme/data", LatticeOperation.Read);

        Assert.That(granted, Is.False);
    }

    [Test]
    public void Range_uniformity_shares_one_budget_across_templated_rules_and_fails_closed()
    {
        // Each rule alone fits the per-template caps; together they exceed them, so the
        // range cannot be proven uniform and is filtered per key.
        var groups = Enumerable.Range(0, 64).Select(i => "g:" + i).Append("members").ToArray();
        var subject = new LatticeSubject("u1", groups);
        var rules = Enumerable.Range(0, 4)
            .Select(i => new LatticeAuthorizationRule(
                "allow" + i, LatticeSubjectSelector.Group("members"),
                LatticeScope.Prefix("t", "z" + i + "/{group:g:}/{group:g:}/"),
                LatticeOperation.Read, LatticeEffect.Allow))
            .Append(new LatticeAuthorizationRule(
                "base", LatticeSubjectSelector.Group("members"), LatticeScope.Prefix("t", "a/"), LatticeOperation.Read, LatticeEffect.Allow))
            .ToArray();
        var policy = CompiledPolicy.Compile(rules);

        var decision = PolicyEvaluator.Evaluate(policy, new LatticeAuthOptions(), subject, "t", LatticeOperation.Read, null, "a/", "a0");

        Assert.Multiple(() =>
        {
            Assert.That(decision.KeyFilter, Is.Not.Null, "the shared budget ran out before the range was proven uniform");
            Assert.That(decision.KeyFilter!("a/x"), Is.True, "per-key filtering still admits the granted keys");
        });
    }

    [Test]
    public void ResolveFor_cites_at_most_the_explain_cap_per_rule()
    {
        var groups = Enumerable.Range(0, 20).Select(i => "g:" + i).ToArray();
        var subject = new LatticeSubject("u1", groups);

        var scopes = ScopeTemplate.ResolveFor(Rule(LatticeScope.Prefix("t", "{group:g:}/{group:g:}/")), subject);

        Assert.That(scopes, Has.Count.EqualTo(ScopeTemplate.MaxExplainInstantiations));
    }

    [Test]
    public void VisitInstantiations_stops_when_the_visitor_declines()
    {
        var visitor = new CountingVisitor(stopAfter: 1);

        var complete = Parse("{group:grp:}/").VisitInstantiations(U1, ref visitor);

        Assert.Multiple(() =>
        {
            Assert.That(complete, Is.True, "a visitor stop is not truncation");
            Assert.That(visitor.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public void ResolveFor_returns_the_literal_scope_or_the_callers_instantiations()
    {
        var literal = LatticeScope.Prefix("t", "plain/");

        Assert.Multiple(() =>
        {
            Assert.That(ScopeTemplate.ResolveFor(Rule(literal), U1), Is.EqualTo(new[] { literal }));
            Assert.That(
                ScopeTemplate.ResolveFor(Rule(LatticeScope.Key("t", "{subject}/me")), U1),
                Is.EqualTo(new[] { LatticeScope.Key("t", "u1/me") }));
            Assert.That(ScopeTemplate.ResolveFor(Rule(LatticeScope.Prefix("t", "{subject}/")), LatticeSubject.Anonymous), Is.Empty);
        });
    }

    [Test]
    public void Tree_with_more_templated_prefixes_than_the_stack_budget_resolves_the_longest()
    {
        // 200 templated prefix rules exceed the stack-allocated length table.
        var rules = Enumerable.Range(0, 200)
            .Select(i => Rule(LatticeScope.Prefix("t", "p" + i + "/{subject}/"), "r" + i))
            .Append(new LatticeAuthorizationRule(
                "deep-deny", LatticeSubjectSelector.Group("members"), LatticeScope.Prefix("t", "p7/{subject}/x/"),
                LatticeOperation.Read, LatticeEffect.Deny))
            .ToArray();
        var policy = CompiledPolicy.Compile(rules);
        var subject = new LatticeSubject("u1", ["members"]);
        var options = new LatticeAuthOptions();

        Assert.Multiple(() =>
        {
            Assert.That(PolicyEvaluator.Evaluate(policy, options, subject, "t", LatticeOperation.Read, "p7/u1/a", null, null).Allowed, Is.True);
            Assert.That(PolicyEvaluator.Evaluate(policy, options, subject, "t", LatticeOperation.Read, "p7/u1/x/a", null, null).Allowed, Is.False);
        });
    }

    private struct CountingVisitor(int stopAfter) : ScopeTemplate.IInstantiationVisitor
    {
        public int Count { get; private set; }

        public bool Visit(ReadOnlySpan<char> value)
        {
            Count++;
            return Count < stopAfter;
        }
    }
}
