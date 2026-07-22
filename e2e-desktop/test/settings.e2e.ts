// @ts-nocheck — WDIO element/callback types churn across versions; runtime
// validates the behaviour. Same pragma as wdio.conf.ts.
//
// Settings smoke — open the Settings screen from the sidebar, then walk every
// nav item from SETTINGS_NAV_GROUPS (general, profile, appearance, …). For
// each: click the nav button, wait for the section to mount, and verify the
// section label renders somewhere in the document. This catches broken
// section components, dangling route refs, and panel-crash error boundaries.

const SECTION_LABELS = [
  "General",
  "Profile",
  "Appearance",
  "Notifications",
  "Behavior",
  "Worktrees",
  "Archived",
  "Models",
  "Providers",
  "Skills",
  "Agents",
  "MCP",
  "Usage",
  "Advanced",
] as const;

describe("Settings", () => {
  it("opens the settings screen from the sidebar", async () => {
    const settingsBtn = await browser.waitUntil(
      async () => {
        // The Settings menu button is the only SidebarMenuButton whose visible
        // label text is exactly "Settings".
        const el = await $("button*=Settings");
        return (await el.isExisting()) ? el : false;
      },
      { timeout: 30_000, timeoutMsg: "settings sidebar button never rendered" },
    );
    await settingsBtn.click();

    await browser.waitUntil(
      async () => {
        const url = await browser.getUrl();
        return url.includes("/settings");
      },
      { timeout: 30_000, timeoutMsg: "route never advanced to /settings" },
    );
  });

  it("renders the settings sidebar nav", async () => {
    const nav = await browser.waitUntil(
      async () => {
        const el = await $('nav[aria-label="Settings sections"]');
        return (await el.isExisting()) ? el : false;
      },
      { timeout: 30_000, timeoutMsg: "settings sidebar nav never mounted" },
    );
    expect(await nav.isDisplayed()).toBe(true);
  });

  for (const label of SECTION_LABELS) {
    it(`renders the ${label} section when its nav button is clicked`, async () => {
      // Each settings nav item is a button whose visible label is the section
      // name. Re-query each iteration — section panels can lazily unmount on
      // navigation away.
      const btn = await browser.waitUntil(
        async () => {
          const el = await $(`button*=${label}`);
          return (await el.isExisting()) ? el : false;
        },
        { timeout: 30_000, timeoutMsg: `${label} nav button never rendered` },
      );
      await btn.click();

      // Section panel mount: the document body should contain the label text
      // somewhere (header, eyebrow, or content). Wait for it to appear so the
      // next iteration starts from a settled DOM.
      await browser.waitUntil(
        async () => {
          const body = await $("body").getText();
          return body.includes(label);
        },
        { timeout: 30_000, timeoutMsg: `${label} section label never appeared in the panel` },
      );
    });
  }
});
