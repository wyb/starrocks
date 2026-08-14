#!/usr/bin/env python3
"""Self-test for enterprise PR classification / version parsing. Run: python3 scripts/test_parse.py"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pr import (REPOS, parse_sync, clean_base_ref, infer_version, normalize_version,
                derive_version, classify_ent_pr, parse_backport, is_enterprise, _row_skip_kind)

FAILED = []
TOTAL = 0

def check(name, got, want):
    global TOTAL
    TOTAL += 1
    if got != want:
        FAILED.append(f"{name}: got {got!r}, want {want!r}")

# --- REPOS registry ---
check("repos.keys", sorted(REPOS.keys()), ["cd", "ms", "oss"])
check("repos.oss.slug", REPOS["oss"]["slug"], "StarRocks/starrocks")
check("repos.cd.slug", REPOS["cd"]["slug"], "CelerData/celerdata-enterprise")
check("repos.ms.slug", REPOS["ms"]["slug"], "MirrorShipDB/mirrorship-enterprise")
# active/label drive the daemon poll set and UI badges — cd is the current enterprise source
check("repos.oss.active", REPOS["oss"]["active"], True)
check("repos.cd.active", REPOS["cd"]["active"], True)    # CelerData is the live enterprise source
check("repos.ms.active", REPOS["ms"]["active"], False)   # MirrorShip not live yet: daemon skips it
check("repos.labels", [REPOS[r]["label"] for r in ("oss", "cd", "ms")], ["OSS", "CD", "MS"])
# kind drives all enterprise-vs-oss behavior
check("kind.oss", is_enterprise("oss"), False)
check("kind.cd", is_enterprise("cd"), True)
check("kind.ms", is_enterprise("ms"), True)
check("kind.unknown", is_enterprise("zzz"), False)
# derive_version's base_ref fallback applies to any enterprise repo
check("dv.ms_baseref", derive_version("ms", "", "branch-4.1-sync-pr-9"), "4.1")
check("dv.ms_label", derive_version("ms", "version:4.2.0-ee", "main"), "4.2.0")   # -ee stripped

# --- parse_sync ---
check("sync.simple", parse_sync("[Enhancement] Add statistics for RTRIM binary(sync #76698)"), [76698])
check("sync.with_backport",
      parse_sync("[BugFix] Skip JSON subfield pushdown (backport #76594)(sync #76640)"), [76640])
check("sync.case_space", parse_sync("xx (SYNC  #123)"), [123])
check("sync.none", parse_sync("[BugFix] fix cve (backport #59356)"), [])
check("sync.not_paren", parse_sync("Resolve committed merge conflict for sync PR #59134"), [])

# --- clean_base_ref ---
check("base.main", clean_base_ref("main"), "main")
check("base.main_syncpr", clean_base_ref("main-sync-pr-76688"), "main")
check("base.branch_syncpr", clean_base_ref("branch-4.1-sync-pr-76229"), "4.1")
check("base.branch", clean_base_ref("branch-3.5"), "3.5")
check("base.empty", clean_base_ref(""), "main")
check("base.nonbranch", clean_base_ref("release-4.1"), "release-4.1")  # neither main nor branch-*: pass through
# Mergify backport/copy branches must unwrap to their target branch, not leak the whole ref as a version
check("base.mergify_bp_branch", clean_base_ref("mergify/bp/branch-4.0/pr-51951"), "4.0")
check("base.mergify_bp_main", clean_base_ref("mergify/bp/main/pr-123"), "main")
check("base.mergify_copy_branch", clean_base_ref("mergify/copy/branch-3.5/pr-999"), "3.5")
check("base.mergify_bp_cc", clean_base_ref("mergify/bp/branch-4.0-cc/pr-1"), "4.0-cc")

# --- infer_version (raw extractor: keeps -ee; normalize_version strips it downstream) ---
check("ver.oss", infer_version("automerge,version:4.1.1"), "4.1.1")
check("ver.ee", infer_version("automerge,version:3.5.21-ee"), "3.5.21-ee")
check("ver.none", infer_version("automerge,sr"), "main")

# --- normalize_version: drop -ee/-cc to unify version granularity ---
check("nv.ee", normalize_version("4.1.4-ee"), "4.1.4")
check("nv.cc", normalize_version("3.5-cc"), "3.5")
check("nv.plain", normalize_version("4.1.4"), "4.1.4")
check("nv.main", normalize_version("main"), "main")
check("nv.nonrelease", normalize_version("release-4.1"), "release-4.1")
check("nv.combined", normalize_version("4.0-ee-cc"), "4.0")
check("nv.empty", normalize_version(""), "main")

# --- derive_version ---
check("dv.cd_label", derive_version("cd", "version:4.1.4-ee", "branch-4.1"), "4.1.4")   # -ee stripped
check("dv.cd_baseref", derive_version("cd", "", "branch-4.1-sync-pr-76229"), "4.1")
check("dv.cd_mergify_bp", derive_version("cd", "", "mergify/bp/branch-4.0/pr-51951"), "4.0")  # was leaking full ref as version
check("dv.cd_mergify_label_wins", derive_version("cd", "version:4.0.3-ee", "mergify/bp/branch-4.0/pr-51951"), "4.0.3")   # -ee stripped
check("dv.cd_main", derive_version("cd", "", "main"), "main")
check("dv.oss_main", derive_version("oss", "", "main"), "main")
check("dv.oss_baseref_fallback", derive_version("oss", "", "branch-3.5"), "3.5")   # oss also falls back to base branch
check("dv.oss_unlabeled_cc_branch", derive_version("oss", "", "branch-3.5-cc"), "3.5")  # -cc stripped+unified; was dropped as 'main' before the base_ref fallback
check("dv.oss_label_wins", derive_version("oss", "version:3.5.19", "branch-3.5"), "3.5.19")

# --- classify_ent_pr: (pr_kind, sync_source_pr) ---
check("cls.sync",
      classify_ent_pr("[Enhancement] Add statistics for RTRIM binary(sync #76698)", "sync,main,automerge,sr", "main"),
      ("sync", 76698))
check("cls.sync_backport_combo",
      classify_ent_pr("[BugFix] Cancel pipeline fragments (backport #76535)(sync #76696)",
                      "sync,automerge,branch-3.5,version:3.5.21-ee", "branch-3.5"),
      ("sync", 76696))
check("cls.sync_label_only",
      classify_ent_pr("[BugFix] weird title without marker", "sync,main", "main"),
      ("sync", None))
check("cls.conflict_by_baseref",
      classify_ent_pr("fix conflict", "", "main-sync-pr-76688"),
      ("conflict_fix", None))
check("cls.conflict_resolve_title",
      classify_ent_pr("[UT] Resolve committed merge conflict in MetricRepoTest.java for sync PR #59291",
                      "", "branch-4.1-sync-pr-76337"),
      ("conflict_fix", None))
check("cls.conflict_title_only",
      classify_ent_pr("fix conflict", "", "main"),
      ("conflict_fix", None))
check("cls.conflict_beats_backport",
      classify_ent_pr("Resolve committed merge conflict (backport #123)", "", "branch-4.1-sync-pr-76229"),
      ("conflict_fix", None))
check("cls.backport",
      classify_ent_pr("[BugFix] fix cve, bump netty safe version to 4.1.136-Final (backport #59356)",
                      "automerge,version:3.5.21-ee", "branch-3.5"),
      ("backport", None))
check("cls.exclusive",
      classify_ent_pr("[Enhancement] Add block-max WAND top-k scorer for builtin GIN BM25", "", "main"),
      ("exclusive", None))
check("cls.exclusive_sync_word_in_title",
      classify_ent_pr("[Tool] Add sync AI conflict-fix workflow", "", "main"),
      ("exclusive", None))
check("cls.conflict_beats_sync_label",  # conflict base_ref outranks a bare 'sync' label
      classify_ent_pr("internal fix", "sync,main", "branch-4.1-sync-pr-999"),
      ("conflict_fix", None))

# --- _row_skip_kind: rows kept out of enrich (sync/backport/conflict_fix stay in raw only) ---
check("skip.sync", _row_skip_kind({"pr_kind": "sync"}), "sync")
check("skip.backport", _row_skip_kind({"pr_kind": "backport"}), "backport")
check("skip.conflict_fix", _row_skip_kind({"pr_kind": "conflict_fix"}), "conflict_fix")
check("skip.exclusive_none", _row_skip_kind({"pr_kind": "exclusive"}), None)
check("skip.legacy_backport", _row_skip_kind({"title": "x (backport #1)"}), "backport")  # no pr_kind
check("skip.legacy_plain_none", _row_skip_kind({"title": "add feature"}), None)

# --- parse_backport regression ---
check("bp.basic", parse_backport("[BugFix] xxx (backport #71082)"), [71082])

if FAILED:
    print(f"FAIL ({len(FAILED)}/{TOTAL} checks failed)")
    for f in FAILED:
        print(" ", f)
    sys.exit(1)
print(f"PASS ({TOTAL} checks)")
