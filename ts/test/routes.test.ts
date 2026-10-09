// The same cases the Go side's domain matching is tested with, so the two ends
// of the tunnel read a name the same way.

import { describe, expect, test } from "vitest";
import { bestDomainMatch, canonicalHostPort, splitHostPort } from "../src/routes.ts";

describe("bestDomainMatch", () => {
  test("case-insensitive", () => {
    const patterns = ["api.corp:443", "*.wild.corp:443"];
    for (const host of ["API.CORP:443", "Api.Corp:443", "svc.WILD.corp:443"]) {
      expect(bestDomainMatch(patterns, host)).not.toBeNull();
    }
  });

  test("a portless key covers every port; the apex is not under its wildcard", () => {
    const patterns = ["db.corp", "*.wild.corp"];
    for (const host of ["db.corp:443", "db.corp:80", "db.corp:5432", "db.corp:22"]) {
      expect(bestDomainMatch(patterns, host)).toBe("db.corp");
    }
    expect(bestDomainMatch(patterns, "svc.wild.corp:9000")).toBe("*.wild.corp");
    expect(bestDomainMatch(patterns, "wild.corp:9000")).toBeNull();
  });

  test("a ported key covers only that port", () => {
    const patterns = ["db.corp:5432"];
    expect(bestDomainMatch(patterns, "db.corp:5432")).toBe("db.corp:5432");
    expect(bestDomainMatch(patterns, "db.corp:22")).toBeNull();
    expect(bestDomainMatch(patterns, "db.corp:443")).toBeNull();
  });

  test("specificity ladder", () => {
    const ladder = ["db.corp:5432", "db.corp", "*.corp:5432", "*.corp", "db.*"];
    for (let i = 0; i < ladder.length; i++) {
      // Shuffled, so the answer cannot come from the order keys are listed in.
      const patterns = ladder.slice(i).reverse();
      expect(bestDomainMatch(patterns, "db.corp:5432")).toBe(ladder[i]!);
    }
  });

  test("deterministic on a tie, whatever the order", () => {
    expect(bestDomainMatch(["*.a.corp", "*.b.corp"], "x.a.corp:443")).toBe("*.a.corp");
    expect(bestDomainMatch(["*.b.corp", "*.a.corp"], "x.a.corp:443")).toBe("*.a.corp");
    expect(bestDomainMatch(["x.*", "*.corp"], "x.corp:443")).toBe("*.corp");
    // Same kind, same length: the smaller spelling wins.
    expect(bestDomainMatch(["*.a.corp:443", "*.a.corp:080"], "x.a.corp:443")).toBe("*.a.corp:443");
    expect(bestDomainMatch(["ab.*", "a.b.*"], "a.b.x:1")).toBe("a.b.*");
  });

  test("address keys, brackets and all", () => {
    const patterns = ["2001:db8::1", "10.0.0.9", "10.0.0.8:5432"];
    expect(bestDomainMatch(patterns, "[2001:db8::1]:443")).toBe("2001:db8::1");
    expect(bestDomainMatch(patterns, "[2001:db8::1]:5432")).toBe("2001:db8::1");
    expect(bestDomainMatch(patterns, "10.0.0.9:22")).toBe("10.0.0.9");
    expect(bestDomainMatch(patterns, "10.0.0.8:5432")).toBe("10.0.0.8:5432");
    expect(bestDomainMatch(patterns, "10.0.0.8:22")).toBeNull();
  });

  test("folds host spellings", () => {
    const patterns = ["api.corp:443", "*.wild.corp:443", "тест.example:443"];
    const cases: [string, string][] = [
      ["api.corp:443", "api.corp:443"],
      ["API.CORP:443", "api.corp:443"],
      ["api.corp.:443", "api.corp:443"],
      ["API.corp.:443", "api.corp:443"],
      ["api.corp:0443", "api.corp:443"],
      ["api.corp.:0443", "api.corp:443"],
      ["svc.wild.corp:443", "*.wild.corp:443"],
      ["svc.wild.corp.:443", "*.wild.corp:443"],
      ["xn--e1aybc.example:443", "тест.example:443"],
      ["тест.example:443", "тест.example:443"],
    ];
    for (const [hostPort, want] of cases) expect([hostPort, bestDomainMatch(patterns, hostPort)]).toEqual([hostPort, want]);

    for (const hostPort of [
      "api.corp:80",
      "notapi.corp:443",
      "api.corp.evil:443",
      "wild.corp:443",
      "api..corp:443",
      "xn--e1aybc.other:443",
    ]) {
      expect([hostPort, bestDomainMatch(patterns, hostPort)]).toEqual([hostPort, null]);
    }
  });

  test("folds pattern spellings", () => {
    const patterns = ["api.corp.:443", "xn--e1aybc.example:443", "*.wild.corp.:443"];
    for (const hostPort of ["api.corp:443", "тест.example:443", "svc.wild.corp:443"]) {
      expect(bestDomainMatch(patterns, hostPort)).not.toBeNull();
    }
  });

  test("refuses an empty host", () => {
    for (const hostPort of [":443", ".:443", "..:443"]) {
      expect(bestDomainMatch(["api.corp:443", "*.corp:443"], hostPort)).toBeNull();
    }
  });

  test("keys with a bad port match only themselves, verbatim", () => {
    const patterns = ["api.corp:0", "api.corp:99999", "api.corp:"];
    // A key spelled exactly like the query is returned before
    // anything is parsed. Any other spelling goes through parsing, which
    // throws these keys out.
    expect(bestDomainMatch(patterns, "api.corp:0")).toBe("api.corp:0");
    expect(bestDomainMatch(patterns, "API.corp:0")).toBeNull();
    expect(bestDomainMatch(patterns, "API.corp:99999")).toBeNull();
  });
});

describe("splitHostPort", () => {
  test("as net.SplitHostPort", () => {
    expect(splitHostPort("a:1")).toEqual(["a", "1"]);
    expect(splitHostPort("[::1]:443")).toEqual(["::1", "443"]);
    expect(splitHostPort("a:")).toEqual(["a", ""]);
    expect(splitHostPort("a")).toBeNull();
    expect(splitHostPort("::1:443")).toBeNull();
    expect(splitHostPort("[::1]443")).toBeNull();
  });

  test("canonical form", () => {
    expect(canonicalHostPort("API.Corp.:0443")).toBe("api.corp:443");
    expect(canonicalHostPort("[2001:DB8::1]:443")).toBe("[2001:db8::1]:443");
    expect(canonicalHostPort(".:443")).toBeNull();
  });
});
