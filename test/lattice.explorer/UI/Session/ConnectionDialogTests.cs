using Bunit;
using Microsoft.JSInterop;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Session;

/// <summary>
/// The connection settings: Core's transport validation before any test or save,
/// the connection test, save-and-connect, and the mandatory first-run form.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ConnectionDialogTests : SessionTestContext
{
    [Test]
    public void A_refused_first_focus_leaves_the_dialog_answering()
    {
        JSInterop.SetupVoid("Blazor._internal.domWrapper.focus", _ => true)
            .SetException(new JSException("Unable to focus an invalid element."));

        var cut = Render<ConnectionDialog>();
        cut.Find("input.lt-input").Input("not a url");
        cut.Find("form").Submit();

        Assert.That(cut.Find("input.lt-input").GetAttribute("aria-invalid"), Is.EqualTo("true"));
    }

    [Test]
    public void An_empty_endpoint_is_refused_with_an_announced_error_and_nothing_is_applied()
    {
        var cut = Render<ConnectionDialog>();

        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(Explorer.Applied, Is.Empty);
            Assert.That(cut.Find("input.lt-input").GetAttribute("aria-invalid"), Is.EqualTo("true"));
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Enter the state-API endpoint"));
        });
    }

    [Test]
    public void A_malformed_endpoint_is_refused()
    {
        var cut = Render<ConnectionDialog>();

        cut.Find("input.lt-input").Input("not a url");
        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(Explorer.Applied, Is.Empty);
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("absolute http:// or https:// URL"));
        });
    }

    [Test]
    public void A_plaintext_remote_endpoint_is_refused_in_secure_mode()
    {
        var cut = Render<ConnectionDialog>();

        cut.Find("input.lt-input").Input("http://cluster.example:80");
        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(Explorer.Applied, Is.Empty);
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("requires TLS"));
        });
    }

    [Test]
    public void Insecure_loopback_mode_is_refused_for_a_remote_endpoint()
    {
        var cut = Render<ConnectionDialog>();

        cut.Find("input.lt-input").Input("http://cluster.example:80");
        CheckboxLabelled(cut, "Insecure loopback development mode").Change(true);
        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(Explorer.Applied, Is.Empty);
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("only allowed for a loopback endpoint"));
        });
    }

    [Test]
    public void Editing_the_endpoint_clears_the_error()
    {
        var cut = Render<ConnectionDialog>();
        cut.Find("form").Submit();

        cut.Find("input.lt-input").Input("https://cluster.example:443");

        Assert.That(cut.FindAll(".lt-field__error"), Is.Empty);
    }

    [Test]
    public void A_valid_endpoint_is_applied_with_its_transport_posture_and_raises_saved()
    {
        var saved = 0;
        var cut = Render<ConnectionDialog>(parameters => parameters.Add(p => p.OnSaved, () => saved++));

        cut.Find("input.lt-input").Input("  http://localhost:5199  ");
        CheckboxLabelled(cut, "Insecure loopback development mode").Change(true);
        CheckboxLabelled(cut, "Allow unencrypted HTTP/2 (h2c)").Change(true);
        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(Explorer.Applied, Has.Count.EqualTo(1));
            Assert.That(Explorer.Applied[0].Endpoint, Is.EqualTo("http://localhost:5199"));
            Assert.That(Explorer.Applied[0].TransportMode, Is.EqualTo(ExplorerTransportMode.InsecureLoopbackDev));
            Assert.That(Explorer.Applied[0].AllowUnencryptedHttp2, Is.True);
            Assert.That(saved, Is.EqualTo(1));
        });
    }

    [Test]
    public void An_apply_failure_is_shown_and_saved_is_not_raised()
    {
        Explorer.ApplyFailure = new InvalidOperationException("The store is read-only.");
        var saved = 0;
        var cut = Render<ConnectionDialog>(parameters => parameters.Add(p => p.OnSaved, () => saved++));

        cut.Find("input.lt-input").Input("https://cluster.example:443");
        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(saved, Is.Zero);
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("The store is read-only."));
        });
    }

    [Test]
    public void Initial_pre_fills_the_form_and_keeps_its_non_secret_headers()
    {
        var initial = new ExplorerConfiguration
        {
            Endpoint = "http://localhost:5199",
            TransportMode = ExplorerTransportMode.InsecureLoopbackDev,
            AllowUnencryptedHttp2 = true,
            Headers = new Dictionary<string, string> { ["x-meta"] = "1" },
            TransportHeaders = new Dictionary<string, string> { ["X-Azure-FDID"] = "abc" },
        };
        var cut = Render<ConnectionDialog>(parameters => parameters.Add(p => p.Initial, initial));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("input.lt-input").GetAttribute("value"), Is.EqualTo("http://localhost:5199"));
            Assert.That(cut.FindAll("input[type=checkbox]").All(box => box.HasAttribute("checked")), Is.True);
        });

        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(Explorer.Applied[0].Headers, Is.SameAs(initial.Headers));
            Assert.That(Explorer.Applied[0].TransportHeaders, Is.SameAs(initial.TransportHeaders));
        });
    }

    [Test]
    public void The_mandatory_form_has_no_cancel_or_close_and_escape_does_not_dismiss_it()
    {
        var cancelled = 0;
        var cut = Render<ConnectionDialog>(parameters => parameters
            .Add(p => p.AllowCancel, false)
            .Add(p => p.OnCancelled, () => cancelled++));

        cut.Find("[role=dialog]").KeyDown("Escape");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Has.No.Member("Cancel").And.No.Member("Close"));
            Assert.That(cancelled, Is.Zero);
        });
    }

    [TestCase("Cancel")]
    [TestCase("Close")]
    public void A_cancellable_form_raises_cancelled(string control)
    {
        var cancelled = 0;
        var cut = Render<ConnectionDialog>(parameters => parameters
            .Add(p => p.AllowCancel, true)
            .Add(p => p.OnCancelled, () => cancelled++));

        cut.FindAll("button").Single(button => button.TextContent.Trim() == control).Click();

        Assert.That(cancelled, Is.EqualTo(1));
    }

    [Test]
    public void Escape_dismisses_a_cancellable_form()
    {
        var cancelled = 0;
        var cut = Render<ConnectionDialog>(parameters => parameters
            .Add(p => p.AllowCancel, true)
            .Add(p => p.OnCancelled, () => cancelled++));

        cut.Find("[role=dialog]").KeyDown("Escape");

        Assert.That(cancelled, Is.EqualTo(1));
    }

    [TestCase(0, LtStateRole.Healthy, "Reachable")]
    [TestCase(1, LtStateRole.Stalled, "Reachable - sign-in required")]
    [TestCase(2, LtStateRole.Failed, "Unreachable")]
    public void The_connection_test_reports_its_outcome_in_words_and_role_without_applying(
        int outcomeValue, LtStateRole role, string text)
    {
        // The outcome enum is internal, so the case carries its value.
        var outcome = (ConnectionTestOutcome)outcomeValue;
        Tester.Result = new ConnectionTestResult(outcome);
        var cut = Render<ConnectionDialog>();

        cut.Find("input.lt-input").Input("https://cluster.example:443");
        TestButton(cut).Click();

        Assert.Multiple(() =>
        {
            Assert.That(Tester.Tested.Single().Endpoint, Is.EqualTo("https://cluster.example:443"));
            Assert.That(Explorer.Applied, Is.Empty, "a test must not persist or reconnect anything");
            Assert.That(cut.Find("[role=status] .lt-pill").GetAttribute("data-lt-state"), Is.EqualTo(role.ToString().ToLowerInvariant()));
            Assert.That(cut.Find("[role=status] .lt-pill__text").TextContent, Is.EqualTo(text));
        });
    }

    [Test]
    public void The_connection_test_never_shows_the_endpoints_own_status_text()
    {
        // What answered at an address the visitor chose is described in fixed words
        // only, so the test cannot be read as a host and port scanner.
        Tester.Result = LatticeConnectionTester.Classify(new Orleans.Lattice.Explorer.Core.Connection.LatticeConnectionStatus(
            Orleans.Lattice.Explorer.Core.Connection.LatticeConnectionState.Faulted,
            "https://10.0.0.5:8443",
            "Status(StatusCode=Unavailable, Detail=\"connect to 10.0.0.5:8443 refused\")"));
        var cut = Render<ConnectionDialog>();

        cut.Find("input.lt-input").Input("https://10.0.0.5:8443");
        TestButton(cut).Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[role=status]").TextContent, Does.Not.Contain("StatusCode").And.Not.Contain("refused"));
            Assert.That(
                cut.Find("[role=status] .lt-field__hint").TextContent,
                Is.EqualTo("No Lattice API answered at this address. Check the endpoint and its transport settings."));
        });
    }

    [Test]
    public void A_throwing_connection_test_never_shows_the_exceptions_text()
    {
        Tester.Failure = new InvalidOperationException("No such host is known. (internal-db.corp:5432)");
        var cut = Render<ConnectionDialog>();

        cut.Find("input.lt-input").Input("https://cluster.example:443");
        TestButton(cut).Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Markup, Does.Not.Contain("internal-db.corp").And.Not.Contain("No such host"));
            Assert.That(cut.Find("[role=status] .lt-pill__text").TextContent, Is.EqualTo("Unreachable"));
        });
    }

    [Test]
    public void A_read_only_head_shows_the_configured_endpoint_and_offers_no_form_test_or_save()
    {
        UseReadOnlyEndpoint();
        Explorer.Configured(RemoteConfiguration("https://cluster.example:8443"));

        var cut = Render<ConnectionDialog>(parameters => parameters
            .Add(p => p.AllowCancel, true)
            .Add(p => p.Initial, Explorer.Current));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("input"), Is.Empty, "a read-only head offers nothing to type an endpoint into");
            Assert.That(cut.FindAll("form"), Is.Empty);
            Assert.That(
                cut.FindAll("button").Select(button => button.TextContent.Trim()),
                Has.No.Member("Test connection").And.No.Member("Save and connect"));
            Assert.That(cut.Find(".lt-dl__value--mono").TextContent, Is.EqualTo("https://cluster.example:8443"));
            Assert.That(Tester.Tested, Is.Empty);
            Assert.That(Explorer.Applied, Is.Empty);
        });
    }

    [Test]
    public void A_read_only_first_run_says_where_the_endpoint_comes_from_and_cannot_be_dismissed()
    {
        UseReadOnlyEndpoint();
        var cancelled = 0;

        var cut = Render<ConnectionDialog>(parameters => parameters
            .Add(p => p.AllowCancel, false)
            .Add(p => p.OnCancelled, () => cancelled++));
        cut.Find("[role=dialog]").KeyDown("Escape");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("input"), Is.Empty);
            Assert.That(cut.FindAll("button"), Is.Empty);
            Assert.That(cut.Find("[role=dialog]").TextContent, Does.Contain("LATTICE_EXPLORER_ENDPOINT"));
            Assert.That(cancelled, Is.Zero);
        });
    }

    [Test]
    public void A_throwing_connection_test_reads_as_unreachable()
    {
        Tester.Failure = new InvalidOperationException("Boom.");
        var cut = Render<ConnectionDialog>();

        cut.Find("input.lt-input").Input("https://cluster.example:443");
        TestButton(cut).Click();

        Assert.That(cut.Find("[role=status] .lt-pill__text").TextContent, Is.EqualTo("Unreachable"));
    }

    [Test]
    public void The_connection_test_validates_first_and_never_probes_a_refused_endpoint()
    {
        var cut = Render<ConnectionDialog>();

        cut.Find("input.lt-input").Input("http://cluster.example:80");
        TestButton(cut).Click();

        Assert.Multiple(() =>
        {
            Assert.That(Tester.Tested, Is.Empty);
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("requires TLS"));
        });
    }

    [Test]
    public void Editing_the_endpoint_discards_a_stale_test_result()
    {
        var cut = Render<ConnectionDialog>();
        cut.Find("input.lt-input").Input("https://cluster.example:443");
        TestButton(cut).Click();

        cut.Find("input.lt-input").Input("https://other.example:443");

        Assert.That(cut.FindAll("[role=status] .lt-pill"), Is.Empty);
    }

    [Test]
    public void The_endpoint_is_labelled_mono_and_says_it_serves_every_facade()
    {
        var cut = Render<ConnectionDialog>();

        var input = cut.Find("input.lt-input");
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find($"label[for='{input.Id}']").TextContent, Is.EqualTo("Endpoint"));
            Assert.That(input.ClassList, Does.Contain("lt-input--mono"));
            Assert.That(cut.Find(".lt-field__hint").TextContent, Does.Contain("Every Lattice API"));
        });
    }

    private static AngleSharp.Dom.IElement TestButton(IRenderedComponent<ConnectionDialog> cut) =>
        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Test connection");

    private static AngleSharp.Dom.IElement CheckboxLabelled(IRenderedComponent<ConnectionDialog> cut, string label)
    {
        var id = cut.FindAll("label.lt-check__label").Single(node => node.TextContent == label).GetAttribute("for");
        return cut.Find($"#{id}");
    }
}
