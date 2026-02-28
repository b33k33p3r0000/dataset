"""Tests for validation report generation."""

from pathlib import Path
from datetime import date

import pytest

from dataset.validate import Issue, Severity
from dataset.report import generate_report, save_report


class TestGenerateReport:
    def test_clean_report(self):
        file_issues = {
            "BTCUSDT/1h": [],
            "BTCUSDT/4h": [],
        }
        report = generate_report(file_issues, total_bars=10000)
        assert "0 issues" in report.lower() or "no issues" in report.lower()
        assert "10,000" in report or "10000" in report

    def test_report_with_issues(self):
        file_issues = {
            "BTCUSDT/1h": [
                Issue(Severity.ERROR, "Gap: 5 missing bars"),
                Issue(Severity.WARN, "Gap: 2 missing bars"),
            ],
            "ETHUSDT/15m": [
                Issue(Severity.INFO, "Early listing: 168 bars"),
            ],
        }
        report = generate_report(file_issues, total_bars=50000)
        assert "ERROR" in report
        assert "WARN" in report
        assert "BTCUSDT/1h" in report
        assert "Gap: 5 missing bars" in report

    def test_report_is_markdown(self):
        report = generate_report({}, total_bars=0)
        assert report.startswith("#")


class TestSaveReport:
    def test_saves_to_file(self, tmp_path):
        report = "# Test Report\nAll good."
        path = save_report(report, reports_dir=tmp_path)
        assert path.exists()
        assert path.suffix == ".md"
        assert path.read_text() == report

    def test_filename_contains_date(self, tmp_path):
        report = "# Test"
        path = save_report(report, reports_dir=tmp_path)
        today = date.today().isoformat()
        assert today in path.name
