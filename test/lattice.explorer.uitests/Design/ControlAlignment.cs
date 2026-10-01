using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Design;

/// <summary>
/// Issue #4120: the measurements behind the control-alignment gate. A page is visited,
/// and every toolbar and control row in its content, every field primitive and every
/// button is measured in the browser against the design system's one control rule.
/// </summary>
/// <remarks>
/// What is measured, and why each one is a fault:
/// <list type="bullet">
/// <item>Controls that share a toolbar row (their control boxes overlap vertically) start
/// at the same top, within 1 px - a field with a label row and a button or a search box
/// without one must not sit at different heights.</item>
/// <item>Every field primitive draws a visible label row over a control box, and every
/// control box and every button is exactly one control height
/// (<c>--lt-op-control-height</c>) - a picker, a select and a text box are one height.</item>
/// <item>A toolbar's label rows are one line, so they do not push a control down.</item>
/// <item>A button's text and a field's label fit their box: nothing is cut off.</item>
/// <item>In engines that resolve <c>::placeholder</c> styles, a placeholder is in the UI
/// face even in a field whose value is in mono.</item>
/// </list>
/// The checkbox, whose box is smaller than a control, is held to the centre of its row.
/// </remarks>
internal static class ControlAlignment
{
    /// <summary>The pages of the shared test world whose toolbars are measured.</summary>
    public static IReadOnlyList<string> WorldPages { get; } =
    [
        "/backups",
        "/backups/schedules",
        "/data",
        $"/data/{ExplorerWorld.DemoTree}",
        $"/data/{ExplorerWorld.DemoTree}?tab=history",
        "/access",
        "/access/groups",
        "/cluster/trees",
        "/cluster/wal",
        "/cluster/orphans",
        "/apps/catalogue",
        "/schema",
        "/replication",
    ];

    /// <summary>The pages of the tenancy world whose toolbars are measured.</summary>
    public static IReadOnlyList<string> TenancyPages { get; } =
    [
        "/tenancy",
        "/tenancy/acme/members",
    ];

    private const string Measure =
        """
        (placeholders) => {
          const faults = [];
          const sizeOf = (css) => {
            const probe = document.createElement('div');
            probe.style.cssText = 'position:absolute;visibility:hidden;width:1px;height:' + css;
            document.body.appendChild(probe);
            const size = probe.getBoundingClientRect().height;
            probe.remove();
            return size;
          };
          const control = sizeOf('var(--lt-op-control-height)');
          const labelLine = sizeOf('var(--lt-op-label-line-height)');
          const compact = !!document.querySelector('.lt-shell--compact');
          const shown = (e) => {
            const box = e.getBoundingClientRect();
            return box.width > 0 && box.height > 0 && !e.closest('[hidden]') && getComputedStyle(e).visibility !== 'hidden';
          };
          const say = (e) => {
            const text = (e.innerText || e.getAttribute('aria-label') || e.getAttribute('placeholder') || e.className || e.tagName).toString();
            return '"' + text.trim().replace(/\s+/g, ' ').slice(0, 40) + '"';
          };
          const boxOf = (field) => {
            const label = field.querySelector(':scope > .lt-field__label');
            return label ? label.nextElementSibling : null;
          };
          const main = document.querySelector('main');
          if (!main) { return { control, toolbars: 0, fields: 0, faults: ['the page has no main landmark'] }; }

          let fields = 0;
          for (const field of [...main.querySelectorAll('.lt-field')].filter(shown)) {
            fields++;
            const label = field.querySelector(':scope > .lt-field__label');
            const box = boxOf(field);
            if (!label || !box) { faults.push('a field ' + say(field) + ' has no label row over its control box'); continue; }
            if (label.classList.contains('lt-visually-hidden') || !shown(label)) {
              faults.push('the field ' + say(label) + ' hides its label row');
            }
            const height = box.getBoundingClientRect().height;
            if (Math.abs(height - control) > 0.5 && !box.querySelector('.lt-combobox__chip')) {
              faults.push('the control box of ' + say(label) + ' is ' + height.toFixed(1) + 'px tall, not the ' + control.toFixed(1) + 'px control height');
            }
            if (label.scrollWidth > label.clientWidth + 1) {
              faults.push('the label ' + say(label) + ' is cut off');
            }
          }

          for (const button of [...main.querySelectorAll('.lt-btn')].filter(shown)) {
            const height = button.getBoundingClientRect().height;
            if (Math.abs(height - control) > 0.5) {
              faults.push('the button ' + say(button) + ' is ' + height.toFixed(1) + 'px tall, not the ' + control.toFixed(1) + 'px control height');
            }
            if (button.scrollWidth > button.clientWidth + 1) {
              faults.push('the text of the button ' + say(button) + ' overflows it');
            }
          }

          const rows = [...main.querySelectorAll('.lt-toolbar, .lt-control-row')].filter(shown);
          for (const row of rows) {
            if (!compact) {
              for (const label of row.querySelectorAll('.lt-field__label')) {
                if (shown(label) && label.getBoundingClientRect().height > labelLine + 0.5) {
                  faults.push('the toolbar label ' + say(label) + ' wraps onto more than one line');
                }
              }
            }
            const boxes = [];
            for (const field of row.querySelectorAll('.lt-field')) {
              const box = boxOf(field);
              if (box && shown(box)) { boxes.push({ element: box, centred: false, name: say(field.querySelector('.lt-field__label')) }); }
            }
            for (const element of row.querySelectorAll('.lt-btn, .lt-switch, input.lt-input, select.lt-select')) {
              if (element.matches('.lt-input, .lt-select') && element.closest('.lt-field')) { continue; }
              if (element.matches('.lt-input, .lt-select') && shown(element)) {
                faults.push('the toolbar control ' + say(element) + ' is not drawn by a field primitive, so it has no label row');
              }
              if (shown(element)) { boxes.push({ element, centred: false, name: say(element) }); }
            }
            for (const element of row.querySelectorAll('.lt-check__box')) {
              if (shown(element)) { boxes.push({ element, centred: true, name: say(element.parentElement) }); }
            }
            for (const entry of boxes) {
              const box = entry.element.getBoundingClientRect();
              entry.top = box.top;
              entry.bottom = box.bottom;
              entry.centre = (box.top + box.bottom) / 2;
            }
            boxes.sort((a, b) => a.top - b.top);
            const lines = [];
            for (const entry of boxes) {
              const line = lines.find(l => entry.top < l.bottom - 1 && entry.bottom > l.top + 1);
              if (line) { line.items.push(entry); line.bottom = Math.max(line.bottom, entry.bottom); line.top = Math.min(line.top, entry.top); }
              else { lines.push({ top: entry.top, bottom: entry.bottom, items: [entry] }); }
            }
            for (const line of lines) {
              const full = line.items.filter(i => !i.centred);
              if (full.length === 0) { continue; }
              const tops = full.map(i => i.top);
              const top = Math.min(...tops);
              if (Math.max(...tops) - top > 1) {
                faults.push('a toolbar row does not line up on its control boxes: '
                  + full.map(i => i.name + ' at ' + i.top.toFixed(1)).join(', '));
              }
              for (const item of line.items.filter(i => i.centred)) {
                if (Math.abs(item.centre - (top + control / 2)) > 1) {
                  faults.push('the checkbox ' + item.name + ' is not centred on its toolbar row');
                }
              }
            }
          }

          if (placeholders) {
            const sans = getComputedStyle(document.documentElement).getPropertyValue('--lt-font-sans').split(',')[0].trim().replace(/["']/g, '');
            for (const input of [...main.querySelectorAll('input[placeholder]')].filter(shown)) {
              if (!input.placeholder) { continue; }
              const face = getComputedStyle(input, '::placeholder').fontFamily.split(',')[0].trim().replace(/["']/g, '');
              if (face !== sans) {
                faults.push('the placeholder ' + say(input) + ' is in ' + face + ', not the UI face ' + sans);
              }
            }
          }

          return { control, toolbars: rows.length, fields, faults };
        }
        """;

    /// <summary>
    /// Visits every page in <paramref name="paths"/> on <paramref name="page"/> and returns
    /// every fault the measurements find, each prefixed with its page.
    /// </summary>
    /// <param name="page">A page already open on <paramref name="world"/>'s head, in the appearance to measure.</param>
    /// <param name="world">The world the pages live in.</param>
    /// <param name="paths">The head-relative paths to visit.</param>
    /// <param name="placeholders">Whether to measure placeholder faces: true in an engine that resolves <c>::placeholder</c> styles.</param>
    public static async Task<IReadOnlyList<string>> MeasureAsync(IPage page, ExplorerWorld world, IEnumerable<string> paths, bool placeholders)
    {
        var faults = new List<string>();
        foreach (var path in paths)
        {
            await Shell.GotoAsync(page, world.Head, path);
            await Expect(Shell.Heading(page)).ToBeVisibleAsync();
            await Expect(Shell.Content(page).Locator(".lt-toolbar").First).ToBeVisibleAsync();
            await Shell.WaitForMotionToSettleAsync(page);

            var result = await page.EvaluateAsync<Measurement>(Measure, placeholders);
            Assert.That(result.Toolbars, Is.GreaterThan(0), $"{path}: the scan found no toolbar, so it measured nothing.");
            faults.AddRange(result.Faults.Select(fault => $"{path}: {fault}"));
        }

        return faults;
    }

    /// <summary>What one page's scan returned.</summary>
    internal sealed class Measurement
    {
        /// <summary>The control height the page resolved, in CSS pixels.</summary>
        public double Control { get; set; }

        /// <summary>How many toolbars and control rows were measured.</summary>
        public int Toolbars { get; set; }

        /// <summary>How many field primitives were measured.</summary>
        public int Fields { get; set; }

        /// <summary>Every fault found.</summary>
        public string[] Faults { get; set; } = [];
    }
}
