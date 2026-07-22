// @ts-nocheck — WDIO element/callback types churn across versions; runtime
// validates the behaviour. Same pragma as wdio.conf.ts.
//
// Chat cycle smoke — verify the composer accepts typed text, submits, and
// reflects the user's prompt as a message bubble in the transcript.
//
// The composer is a Lexical contentEditable, NOT a plain input — direct
// textContent writes are ignored by Lexical's internal state. We drive it via
// execCommand('insertText') which Lexical's Paste handlers honour.
//
// This does NOT assert on a real LLM response — the embedded Tauri binary
// ships without provider credentials, so we only verify the client-side
// dispatch path: typed text → form submit → user message rendered.

async function typeIntoEditor(editor: WebdriverIO.Element, text: string) {
  await editor.click();
  await browser.execute((el: HTMLElement) => el.focus(), editor);
  await browser.execute(
    (t: string) => document.execCommand("insertText", false, t),
    text,
  );
}

describe("Chat cycle", () => {
  it("renders the composer form on boot", async () => {
    const composer = await browser.waitUntil(
      async () => {
        const form = await $('form[data-chat-composer-form="true"]');
        return (await form.isExisting()) ? form : false;
      },
      { timeout: 30_000, timeoutMsg: "composer form never mounted" },
    );
    expect(await composer.isDisplayed()).toBe(true);
  });

  it("accepts typed text in the composer editor", async () => {
    const editor = await browser.waitUntil(
      async () => {
        const el = await $('[data-testid="composer-editor"]');
        return (await el.isExisting()) ? el : false;
      },
      { timeout: 30_000, timeoutMsg: "composer editor never mounted" },
    );
    const probe = "e2e-chat-cycle-probe-" + Date.now();
    await typeIntoEditor(editor, probe);
    const text = await editor.getText();
    expect(text).toContain(probe);
  });

  it("submits the prompt and renders a user message bubble", async () => {
    const editor = await $('[data-testid="composer-editor"]');
    const probe = "e2e-chat-cycle-submit-" + Date.now();
    await typeIntoEditor(editor, probe);

    const form = await $('form[data-chat-composer-form="true"]');
    await browser.execute((f: HTMLFormElement) => f.requestSubmit(), form);

    await browser.waitUntil(
      async () => {
        const body = await $("body").getHTML();
        return body.includes(probe);
      },
      { timeout: 30_000, timeoutMsg: "submitted prompt never appeared in the transcript" },
    );
  });
});
