#!/usr/bin/env python3
"""
Update database handler README with latest test results.

Usage:
    python update_readme.py <junit_xml_path> <readme_path> <workflow_name> <workflow_file>

Example:
    python update_readme.py /tmp/test_results/junit.xml postgres/README.md "PostgreSQL" postgres-ci.yml
"""

import sys
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path


def parse_junit_xml(xml_path):
    """Parse JUnit XML and extract test statistics."""
    try:
        if not os.path.exists(xml_path):
            return None

        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Get test suite stats
        testsuite = root.find(".//testsuite")
        if testsuite is None:
            return None

        total = int(testsuite.get("tests", 0))
        failures = int(testsuite.get("failures", 0))
        errors = int(testsuite.get("errors", 0))
        skipped = int(testsuite.get("skipped", 0))
        passed = total - failures - errors - skipped
        time = float(testsuite.get("time", 0))

        success_rate = (passed / total * 100) if total > 0 else 0

        return {
            "total": total,
            "passed": passed,
            "failed": failures,
            "errors": errors,
            "skipped": skipped,
            "time": time,
            "success_rate": success_rate,
        }
    except Exception as e:
        print(f"Error parsing XML: {e}", file=sys.stderr)
        return None


def get_status_info(success_rate):
    """Get status emoji and badge color based on success rate."""
    if success_rate == 100:
        return "✅ All tests passing", "brightgreen"
    elif success_rate >= 80:
        return "⚠️ Some tests failing", "yellow"
    else:
        return "❌ Tests failing", "red"


def generate_test_results_section(stats, handler_name, workflow_file):
    """Generate the test results markdown section."""
    github_server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    github_repo = os.environ.get("GITHUB_REPOSITORY", "")

    lines = ["<!-- TEST_RESULTS_START -->"]
    lines.append("## 📊 Latest Test Results")
    lines.append("")

    if stats is None:
        lines.append("⚠️ No test data available")
        lines.append("")
        lines.append("<!-- TEST_RESULTS_END -->")
        return "\n".join(lines)

    # Generate badges
    workflow_badge = (
        f"{github_server}/{github_repo}/actions/workflows/{workflow_file}/badge.svg"
    )
    status_text, status_color = get_status_info(stats["success_rate"])

    lines.append(f"![Tests]({workflow_badge})")
    lines.append(
        f"![Status](https://img.shields.io/badge/status-{status_text.split()[1]}-{status_color})"
    )
    lines.append("")

    # Generate statistics table
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| **Status** | {status_text} |")
    lines.append(f"| **Total Tests** | {stats['total']} |")
    lines.append(f"| **✅ Passed** | {stats['passed']} |")
    lines.append(f"| **❌ Failed** | {stats['failed']} |")
    lines.append(f"| **⚠️ Errors** | {stats['errors']} |")
    lines.append(f"| **⏭️ Skipped** | {stats['skipped']} |")
    lines.append(f"| **Success Rate** | {stats['success_rate']:.1f}% |")
    lines.append(f"| **Duration** | {stats['time']:.2f}s |")
    lines.append(
        f"| **Last Updated** | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} |"
    )
    lines.append("")

    # Add link to detailed results
    workflow_url = f"{github_server}/{github_repo}/actions/workflows/{workflow_file}"
    lines.append(f"[📋 View detailed results]({workflow_url})")
    lines.append("")
    lines.append("<!-- TEST_RESULTS_END -->")

    return "\n".join(lines)


def update_readme(readme_path, test_results_section):
    """Update README file with test results section."""
    readme_file = Path(readme_path)

    if not readme_file.exists():
        print(f"Warning: {readme_path} not found", file=sys.stderr)
        return False

    # Read current README
    content = readme_file.read_text()

    # Check if markers exist
    if "<!-- TEST_RESULTS_START -->" not in content:
        # Append to end of file
        print(f"Markers not found, appending to end of {readme_path}")
        with open(readme_path, "a") as f:
            f.write("\n\n")
            f.write(test_results_section)
        return True

    # Replace content between markers
    start_marker = "<!-- TEST_RESULTS_START -->"
    end_marker = "<!-- TEST_RESULTS_END -->"

    start_idx = content.find(start_marker)
    end_idx = content.find(end_marker)

    if start_idx == -1 or end_idx == -1:
        print(f"Error: Markers malformed in {readme_path}", file=sys.stderr)
        return False

    # Include the end marker in the deletion
    end_idx += len(end_marker)

    new_content = content[:start_idx] + test_results_section + content[end_idx:]

    # Write updated content
    readme_file.write_text(new_content)
    print(f"Successfully updated {readme_path}")
    return True


def main():
    if len(sys.argv) < 5:
        print(
            "Usage: python update_readme.py <junit_xml> <readme_path> <handler_name> <workflow_file>"
        )
        print(
            "Example: python update_readme.py /tmp/test_results/junit.xml postgres/README.md PostgreSQL postgres-ci.yml"
        )
        sys.exit(1)

    junit_xml = sys.argv[1]
    readme_path = sys.argv[2]
    handler_name = sys.argv[3]
    workflow_file = sys.argv[4]

    print(f"Parsing test results from {junit_xml}...")
    stats = parse_junit_xml(junit_xml)

    print(f"Generating test results section for {handler_name}...")
    test_results_section = generate_test_results_section(
        stats, handler_name, workflow_file
    )

    print(f"Updating {readme_path}...")
    success = update_readme(readme_path, test_results_section)

    if success:
        print("✅ README update complete")
        sys.exit(0)
    else:
        print("❌ README update failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
