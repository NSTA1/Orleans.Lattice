using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U2;

/// <summary>The Remediation tab: the step editor, the review, and the operation's status.</summary>
internal sealed partial class SchemaPage
{
    /// <summary>The status headline of a finished operation.</summary>
    public const string OperationFinishedText = "The last operation finished, and the tree serves its result.";

    /// <summary>The step editor.</summary>
    public ILocator StepEditor => Content.Locator("fieldset.lt-schema-builder");

    /// <summary>The background operation's status section.</summary>
    public ILocator OperationStatus => Content.Locator("section.lt-schema-operation");

    /// <summary>The steps written so far, as the editor reads them back.</summary>
    public ILocator TransformSteps => Content.GetByRole(AriaRole.List, new() { Name = "Transform steps", Exact = true }).Locator(".lt-schema-rules__text");

    /// <summary>
    /// Writes a conditional set step (#5476): <paramref name="member"/> is set to the number
    /// <paramref name="value"/> only where it is less than the number <paramref name="lessThan"/>.
    /// Leaves the step written but not yet added, so the editor can be photographed.
    /// </summary>
    /// <param name="member">The member.</param>
    /// <param name="value">The number written.</param>
    /// <param name="lessThan">The number the member must be less than.</param>
    public async Task WriteConditionalSetAsync(string member, string value, string lessThan)
    {
        await Expect(StepEditor.GetByLabel("Step", new() { Exact = true })).ToHaveValueAsync("Set");
        var path = StepEditor.GetByRole(AriaRole.Combobox, new() { Name = "Member", Exact = true });
        await path.FillAsync(member);
        await path.PressAsync("Escape");
        await StepEditor.GetByLabel("Value kind", new() { Exact = true }).SelectOptionAsync("Number");
        await StepEditor.GetByLabel("Value", new() { Exact = true }).FillAsync(value);

        var condition = StepEditor.GetByRole(AriaRole.Switch, new() { Name = "Only where the member meets a condition" });
        await condition.ClickAsync();
        await Expect(condition).ToHaveAttributeAsync("aria-checked", "true");
        await Expect(StepEditor.GetByLabel("Where the member is", new() { Exact = true })).ToHaveValueAsync("LessThan");
        await Expect(StepEditor.GetByLabel("Compared value kind", new() { Exact = true })).ToHaveValueAsync("Number");
        await StepEditor.GetByLabel("Compared value", new() { Exact = true }).FillAsync(lessThan);
    }

    /// <summary>Adds the step written, and asserts the editor reads it back as <paramref name="sentence"/>.</summary>
    /// <param name="sentence">The step's sentence.</param>
    public async Task AddStepAsync(string sentence)
    {
        await StepEditor.GetByRole(AriaRole.Button, new() { Name = "Add step", Exact = true }).ClickAsync();
        await Expect(TransformSteps).ToHaveTextAsync([sentence]);
        await Expect(StepEditor.GetByRole(AriaRole.Switch, new() { Name = "Only where the member meets a condition" })).ToHaveAttributeAsync("aria-checked", "false");
    }

    /// <summary>Opens the review of the remediation: the destructive confirmation naming the tree.</summary>
    /// <param name="steps">How many steps it says the values are rewritten through.</param>
    public async Task ReviewRemediationAsync(int steps)
    {
        await Button("Review and start...").ClickAsync();
        await Expect(AlertDialog.Filter(new() { HasText = "Remediate this tree" }))
            .ToContainTextAsync($"Every value is rewritten through {steps} step{(steps == 1 ? string.Empty : "s")} and checked against the policy.");
    }

    /// <summary>Waits until the tree's background operation has finished and the tree serves its result.</summary>
    public async Task ExpectOperationFinishedAsync()
    {
        await Expect(OperationStatus.Locator("p.lt-schema-status")).ToHaveTextAsync(
            OperationFinishedText, new() { Timeout = (float)OperationPatience.TotalMilliseconds });
        await Expect(OperationStatus.Locator(".lt-schema-abort")).ToHaveCountAsync(0);
    }

    /// <summary><b>Refresh status</b> re-reads the cluster's status: still finished.</summary>
    public async Task RefreshStatusAsync()
    {
        await OperationStatus.GetByRole(AriaRole.Button, new() { Name = "Refresh status", Exact = true }).ClickAsync();
        await ExpectOperationFinishedAsync();
    }

    /// <summary><b>Clear this result</b> dismisses the finished operation this session started.</summary>
    public async Task ClearResultAsync()
    {
        var clear = OperationStatus.GetByRole(AriaRole.Button, new() { Name = "Clear this result", Exact = true });
        await clear.ClickAsync();
        await Expect(clear).ToHaveCountAsync(0);
    }
}
