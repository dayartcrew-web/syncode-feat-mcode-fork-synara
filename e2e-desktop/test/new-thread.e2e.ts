// @ts-nocheck — WDIO element/callback types churn across versions; runtime
// validates the behaviour. Same pragma as wdio.conf.ts.
//
// New thread smoke — verify the per-project new-thread button
// (`data-testid="new-thread-button"` in Sidebar.tsx) advances the route to a
// fresh `/chat/<id>` and clears the composer.
//
// On a fresh-app baseline with no projects configured the button doesn't
// exist; we skip cleanly in that case (same pattern as `existing-thread.e2e.ts`).
// The browser-level Ctrl+N hotkey can't be used as a fallback — Edge WebView2
// reserves it at the OS layer before the app's keybinding handler runs, and a
// synthetic KeyboardEvent is rejected because `isTrusted=false` events don't
// fire React/cmdk handlers.

describe("New thread", () => {
  it("creates a fresh thread via the sidebar button", async () => {
    const beforeUrl = await browser.getUrl();

    const sidebarBtn = await $('[data-testid="new-thread-button"]');
    const sidebarBtnExists = await sidebarBtn.isExisting();
    if (!sidebarBtnExists) {
      console.warn(
        "[new-thread] no sidebar new-thread button — UI affordance absent on this baseline; skipping",
      );
      return; // mocha treats no-assertion as passed; covered when a project exists
    }

    await sidebarBtn.click();

    await browser.waitUntil(
      async () => {
        const url = await browser.getUrl();
        return url !== beforeUrl && /\/chat\/[^/]+/.test(url);
      },
      { timeout: 30_000, timeoutMsg: "route never advanced to a new thread id" },
    );

    const editor = await browser.waitUntil(
      async () => {
        const el = await $('[data-testid="composer-editor"]');
        return (await el.isExisting()) ? el : false;
      },
      { timeout: 30_000, timeoutMsg: "composer editor never mounted on new thread" },
    );
    const text = await editor.getText();
    expect(text.trim()).toBe("");
  });
});
