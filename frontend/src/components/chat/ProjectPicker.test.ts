import { describe, expect, it } from "vitest";
import { basenameOfPath, isAbsolutePath } from "./ProjectPicker";

describe("basenameOfPath", () => {
  it("extracts the final segment from a posix path", () => {
    expect(basenameOfPath("/home/me/project")).toBe("project");
  });
  it("extracts the final segment from a windows path", () => {
    expect(basenameOfPath("C:\\Users\\me\\project")).toBe("project");
  });
  it("handles mixed separators", () => {
    expect(basenameOfPath("C:/Users/me\\project")).toBe("project");
  });
  it("strips a trailing separator", () => {
    expect(basenameOfPath("/home/me/project/")).toBe("project");
    expect(basenameOfPath("C:\\Users\\me\\")).toBe("me");
  });
  it("returns the whole string when no separator is present", () => {
    expect(basenameOfPath("project")).toBe("project");
  });
  it("returns null for empty / nullish input", () => {
    expect(basenameOfPath(null)).toBeNull();
    expect(basenameOfPath(undefined)).toBeNull();
    expect(basenameOfPath("")).toBeNull();
  });
});

describe("isAbsolutePath", () => {
  it("accepts windows drive paths with backslashes", () => {
    expect(isAbsolutePath("C:\\Users\\me\\project")).toBe(true);
  });
  it("accepts windows drive paths with forward slashes", () => {
    expect(isAbsolutePath("C:/Users/me/project")).toBe(true);
  });
  it("accepts lowercase drive letters", () => {
    expect(isAbsolutePath("d:\\dev")).toBe(true);
  });
  it("accepts posix absolute paths", () => {
    expect(isAbsolutePath("/home/me/project")).toBe(true);
  });
  it("rejects relative paths", () => {
    expect(isAbsolutePath("project")).toBe(false);
    expect(isAbsolutePath("./project")).toBe(false);
    expect(isAbsolutePath("../project")).toBe(false);
    expect(isAbsolutePath("me/project")).toBe(false);
  });
  it("rejects empty / whitespace-only input", () => {
    expect(isAbsolutePath("")).toBe(false);
    expect(isAbsolutePath("   ")).toBe(false);
  });
  it("rejects a bare drive letter without a path", () => {
    expect(isAbsolutePath("C:")).toBe(false);
    expect(isAbsolutePath("C")).toBe(false);
  });
});
