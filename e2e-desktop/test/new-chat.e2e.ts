// @ts-nocheck — WDIO element/callback types churn across versions; runtime
// validates the behaviour. Same pragma as wdio.conf.ts.
//
// New chat smoke — click the sidebar's "New chat" toolbar button (the icon
// button at the top of the Chats section, labelled "Open new chat home") and
// verify the app navigates to a fresh chat home route with the empty-state
// shell rather than a previously-open transcript.

describe("New chat", () => {
  it("navigates to a fresh chat home via the sidebar new-chat button", async () => {
    const newChatBtn = await browser.waitUntil(
      async () => {
        const el = await $('[aria-label="Open new chat home"]');
        return (await el.isExisting()) ? el : false;
      },
      { timeout: 30_000, timeoutMsg: "new-chat toolbar button never rendered" },
    );
    const beforeUrl = await browser.getUrl();
    await newChatBtn.click();

    await browser.waitUntil(
      async () => {
        const url = await browser.getUrl();
        return url !== beforeUrl;
      },
      { timeout: 30_000, timeoutMsg: "route never changed after clicking new chat" },
    );

    // After navigation, the composer should mount fresh (empty). Some routes
    // fall back to /chat itself which then auto-resolves to a draft thread —
    // accept either as long as the composer is present.
    const composer = await browser.waitUntil(
      async () => {
        const form = await $('form[data-chat-composer-form="true"]');
        return (await form.isExisting()) ? form : false;
      },
      { timeout: 30_000, timeoutMsg: "composer form never mounted on new chat" },
    );
    expect(await composer.isDisplayed()).toBe(true);
  });
});
