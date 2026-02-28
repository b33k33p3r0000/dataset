"""Validation report generation — markdown format."""

from datetime import date
from pathlib import Path
from typing import Dict, List

from dataset.validate import Issue, Severity


def generate_report(
    file_issues: Dict[str, List[Issue]],
    total_bars: int,
) -> str:
    """Generate markdown validation report."""
    today = date.today().isoformat()
    n_files = len(file_issues)
    all_issues = [i for issues in file_issues.values() for i in issues]
    n_errors = sum(1 for i in all_issues if i.severity == Severity.ERROR)
    n_warns = sum(1 for i in all_issues if i.severity == Severity.WARN)
    n_infos = sum(1 for i in all_issues if i.severity == Severity.INFO)

    lines = [
        f"# Dataset Validation — {today}",
        "",
        "## Summary",
        f"- **Files:** {n_files}",
        f"- **Total bars:** {total_bars:,}",
        f"- **Errors:** {n_errors}",
        f"- **Warnings:** {n_warns}",
        f"- **Info:** {n_infos}",
        "",
    ]

    if not all_issues:
        lines.append("No issues found.")
        return "\n".join(lines)

    lines.append("## Issues")
    lines.append("")

    for file_key, issues in sorted(file_issues.items()):
        if not issues:
            continue
        lines.append(f"### {file_key}")
        for issue in issues:
            lines.append(f"- **{issue.severity.value}:** {issue.message}")
        lines.append("")

    return "\n".join(lines)


def save_report(report: str, reports_dir: Path) -> Path:
    """Save report to reports directory. Returns path."""
    reports_dir.mkdir(parents=True, exist_ok=True)
    today = date.today().isoformat()
    path = reports_dir / f"{today}-validation.md"
    path.write_text(report)
    return path
