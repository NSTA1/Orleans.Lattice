using System.Text;
using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// View administration from the Data area (#5369): New view (command
/// <c>data.create-view</c>, address <c>?tab=views&amp;new=view</c>) lists the
/// cluster's registered providers with their descriptions, takes a name and a
/// payload - a form for a described provider, JSON otherwise - and creates the
/// view; Drop view removes one behind the destructive confirmation. Without
/// administrative authority over the source none of it is drawn and nothing is
/// called.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataViewAdministrationTests : DataTestContext
{
    private static readonly TreeViewProviderInfo Described = new()
    {
        ProviderKey = "by-status.v1",
        Description = "Orders grouped by status",
        PayloadFields =
        [
            new TreeViewProviderField { Name = "status", Label = "Status", Required = true },
            new TreeViewProviderField { Name = "limit", Label = "Limit", Kind = TreeViewProviderFieldKind.Number },
            new TreeViewProviderField { Name = "open", Label = "Open only", Kind = TreeViewProviderFieldKind.Boolean },
        ],
    };

    private static readonly TreeViewProviderInfo Raw = new() { ProviderKey = "raw.v1" };

    private readonly List<(string Name, string Source, string Provider, string Payload)> _created = [];

    [Test]
    public void Without_authority_nothing_is_offered_and_nothing_is_called()
    {
        SeedViews();

        var cut = RenderAt("data/orders?tab=views&new=view");

        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Has.None.EqualTo("New view...").And.None.EqualTo("Drop view..."));
            Assert.That(cut.FindAll(".lt-dialog"), Is.Empty, "the deep link opens nothing for a caller who cannot create");
            Assert.That(Area.Commands.Select(command => command.Id), Is.EqualTo(new[] { DataArea.RefreshCommandId }));
        });
        Admin.DidNotReceiveWithAnyArgs().ListViewProvidersAsync(default!, default);
        Admin.DidNotReceiveWithAnyArgs().CreateViewAsync(default!, default!, default!, default!, default);
    }

    [Test]
    public void New_view_lists_described_providers_and_creates_from_the_form()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        Providers(Described, Raw);
        var cut = RenderAt("data/orders?tab=views");
        cut.WaitUntil(() => Assert.That(cut.FindAll($"[data-lt-command='{DataArea.CreateViewCommandId}']"), Has.Count.EqualTo(1)));

        cut.Find($"[data-lt-command='{DataArea.CreateViewCommandId}']").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(CurrentRelative, Is.EqualTo("/data/orders?tab=views&new=view"));
            Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("New view"));
            Assert.That(cut.FindAll("select option").Select(option => option.TextContent), Is.EqualTo(new[] { "by-status.v1", "raw.v1" }));
            Assert.That(cut.Find(".lt-dialog").TextContent, Does.Contain("Orders grouped by status"));
            Assert.That(cut.FindAll("[data-lt-view-payload='form']"), Has.Count.EqualTo(1));
        });

        Control(cut, "View name").Input("open-orders");
        Control(cut, "Status").Input("open");
        Control(cut, "Limit (optional)").Input("25");
        cut.Find("input[data-lt-view-field='open']").Change(true);
        cut.Find("[data-lt-view-create]").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(_created, Is.EqualTo(new[] { ("open-orders", "orders", "by-status.v1", "{\"status\":\"open\",\"limit\":25,\"open\":true}") }));
            Assert.That(Toasts, Is.EqualTo(new[] { "Created view open-orders." }));
            Assert.That(cut.FindAll(".lt-dialog"), Is.Empty);
            Assert.That(CurrentRelative, Is.EqualTo("/data/orders?tab=views"));
            Assert.That(Rows(cut).Select(row => row.QuerySelector("th")!.TextContent.Trim()), Is.EqualTo(new[] { "by-status", "open-orders" }));
        });
        Assert.That(cut.Find("tbody th a[href='data/view-open-orders']"), Is.Not.Null, "the projection is browsable in Data");
    }

    [Test]
    public void A_required_field_and_a_malformed_number_are_reported_before_any_call()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        Providers(Described);
        var cut = OpenDialog();

        Control(cut, "View name").Input("open-orders");
        Control(cut, "Limit (optional)").Input("lots");
        cut.Find("[data-lt-view-create]").Submit();

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-dialog .lt-field__error").Select(error => error.TextContent.Trim()), Is.EqualTo(new[]
        {
            "! Enter a value.",
            "! Enter a number, such as 12 or 0.5.",
        })));
        Assert.That(_created, Is.Empty);
    }

    [TestCase("", "Name the view.")]
    [TestCase("a/b", "A view name cannot contain /, \\, #, ? or ~.")]
    [TestCase("by-status", "A view with this name already exists.")]
    public void A_name_the_cluster_would_refuse_is_reported_before_any_call(string name, string error)
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        Providers(Raw);
        var cut = OpenDialog();

        Control(cut, "View name").Input(name);
        cut.Find("[data-lt-view-create]").Submit();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog .lt-field__error").TextContent, Does.Contain(error)));
        Assert.That(_created, Is.Empty);
    }

    [Test]
    public void A_json_provider_is_validated_locally_and_its_refusal_is_a_fixed_sentence()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        Providers(Raw);
        Admin.CreateViewAsync("raw", Arg.Any<string>(), Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new ArgumentException("view-gen on t/acme/orders rejected"));
        var cut = OpenDialog();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-view-payload='json']"), Has.Count.EqualTo(1)));

        Control(cut, "View name").Input("raw");
        Control(cut, "Payload (JSON)").Input("{ not json");
        cut.Find("[data-lt-view-create]").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog .lt-field__error").TextContent, Does.Contain("This is not valid JSON.")));
        Admin.DidNotReceiveWithAnyArgs().CreateViewAsync(default!, default!, default!, default!, default);

        Control(cut, "Payload (JSON)").Input("{\"x\":1}");
        cut.Find("[data-lt-view-create]").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-view-create-error]").TextContent, Does.StartWith("The cluster refused this view"));
            Assert.That(cut.Markup, Does.Not.Contain("t/acme"));
        });
        Admin.Received(1).CreateViewAsync(
            "raw", "orders", "raw.v1", Arg.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == "{\"x\":1}"), Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_cluster_without_providers_says_so()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        Providers();

        var cut = OpenDialog();

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-view-providers='none']").TextContent, Does.Contain("registers no view providers")));
        Assert.That(cut.FindAll("[data-lt-view-create]"), Is.Empty);
    }

    [Test]
    public void A_refused_provider_listing_is_reported_in_a_fixed_sentence()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        Admin.ListViewProvidersAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new Grpc.Core.RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.PermissionDenied, "t/acme/orders")));

        var cut = OpenDialog();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog [role=alert]").TextContent, Is.EqualTo("You do not have permission to list the view providers.")));
    }

    [Test]
    public void Cancel_closes_the_dialog_and_forgets_the_address()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        Providers(Raw);
        var cut = OpenDialog();

        Button(cut, "Cancel").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-dialog"), Is.Empty);
            Assert.That(CurrentRelative, Is.EqualTo("/data/orders?tab=views"));
        });
    }

    [Test]
    public void The_palette_command_is_offered_over_an_administered_tree_and_has_its_control()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        Providers(Raw);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(Area.Commands.Select(command => command.Id), Does.Contain(DataArea.CreateViewCommandId)));
        var command = Area.Commands.Single(candidate => candidate.Id == DataArea.CreateViewCommandId);

        Assert.That(command.Target, Is.EqualTo(ExplorerAddress.ForTree("data", "orders").WithQuery("tab", "views").WithQuery("new", "view")));
        Navigation.NavigateTo(command.Target!.ToHref());
        cut.WaitUntil(() => ExplorerCommandControls.AssertVisibleControl(cut, command));

        Navigation.NavigateTo("data");
        cut.WaitUntil(() => Assert.That(Area.Commands.Select(candidate => candidate.Id), Is.EqualTo(new[] { DataArea.RefreshCommandId })));
    }

    [Test]
    public void Drop_view_runs_only_after_the_destructive_confirmation_and_removes_the_row()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        Admin.DropViewAsync("by-status", Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                Client.Views.RemoveAll(view => view.ViewName == "by-status");
                return Task.CompletedTask;
            });
        var cut = RenderAt("data/orders?tab=views");
        cut.WaitUntil(() => Assert.That(Button(cut, "Drop view...").HasAttribute("disabled"), Is.False));

        Button(cut, "Drop view...").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("Drop this view?")));
        Admin.DidNotReceiveWithAnyArgs().DropViewAsync(default!, default);

        Confirm(cut, "by-status");

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Is.EqualTo(new[] { "Dropped view by-status." }));
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("No views"));
        });
        Admin.Received(1).DropViewAsync("by-status", Arg.Any<CancellationToken>());
    }

    [Test]
    public void Dropping_a_view_from_its_own_workspace_returns_to_the_source()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        Admin.DropViewAsync("by-status", Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                Client.Views.Clear();
                return Task.CompletedTask;
            });
        var cut = RenderAt("data/view-by-status?tab=views");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Drop view..."), Is.EqualTo(1)));
        Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Has.None.EqualTo("New view..."), "a view is not a source");

        Button(cut, "Drop view...").Click();
        Confirm(cut, "by-status");

        cut.WaitUntil(() => Assert.That(CurrentRelative, Is.EqualTo("/data/orders?tab=views")));
    }

    [Test]
    public void A_startup_view_the_cluster_will_not_drop_is_said_plainly()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        Admin.DropViewAsync("by-status", Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("declared at startup on t/acme"));
        var cut = RenderAt("data/orders?tab=views");
        cut.WaitUntil(() => Assert.That(Button(cut, "Drop view...").HasAttribute("disabled"), Is.False));

        Button(cut, "Drop view...").Click();
        Confirm(cut, "by-status");

        cut.WaitUntil(() => Assert.That(Toasts, Is.EqualTo(new[] { "The cluster refused to drop this view. A view the host declares at startup cannot be dropped here." })));
        Assert.That(Rows(cut), Has.Count.EqualTo(1));
    }

    private IReadOnlyList<string> Toasts => Services.GetRequiredService<LtToastService>().Toasts.Select(toast => toast.Message).ToArray();

    private IRenderedComponent<DataPageHost> OpenDialog()
    {
        var cut = RenderAt("data/orders?tab=views&new=view");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("New view")));
        return cut;
    }

    private void Providers(params TreeViewProviderInfo[] providers) =>
        Admin.ListViewProvidersAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new TreeViewProviderCatalog { Providers = [.. providers] });

    private void SeedViews()
    {
        Client.WithTree("orders");
        Client.Views.Add(new ViewStateSummary { ViewName = "by-status", SourceTreeId = "orders" });
        Admin.GetViewStatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => new TreeViewStatus { ViewName = call.Arg<string>(), SourceTreeId = "orders", ApplyLag = 0 });
        Admin.CreateViewAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var name = call.ArgAt<string>(0);
                _created.Add((name, call.ArgAt<string>(1), call.ArgAt<string>(2), Encoding.UTF8.GetString(call.ArgAt<byte[]>(3))));
                Client.Views.Add(new ViewStateSummary { ViewName = name, SourceTreeId = call.ArgAt<string>(1) });
                return new TreeViewStatus { ViewName = name, SourceTreeId = call.ArgAt<string>(1), ApplyLag = 0 };
            });
    }
}
