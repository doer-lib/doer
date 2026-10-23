#!/usr/bin/env python3
"""Writes Markdown tables of test results of matrix jobs (for GITHUB_STEP_SUMMARY).

Per matrix: tests run/failed/skipped, wallclock of the matrix jobs, and a table
with a row per test class (time of the class) and a row per test method, a column per matrix value.
Time of a failed test is marked with 🔴 (time of its class is not); a cell is empty when the test did not run there.
When an artifact has container-usage.json (written by the e2e suite, see E2eEnvironment), the table starts with
the CPU, disk and network I/O of the application containers, summed over all of them (node 1, node 2, restarts).

Input:
  --reports DIR      downloaded artifacts: DIR/<artifact-prefix><value>/**/TEST-*.xml (surefire/failsafe reports)
                     and DIR/<artifact-prefix><value>/**/container-usage.json
  --jobs FILE        JSON Lines of the jobs of the run: {"name", "started_at", "completed_at"}
  --matrix TITLE JOB ARTIFACT_PREFIX   one table: jobs "JOB (<value>)", artifacts "<ARTIFACT_PREFIX><value>"
"""
import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

PASSED, FAILED, SKIPPED = "passed", "failed", "skipped"


class Result:
    def __init__(self):
        self.time = 0.0
        self.status = SKIPPED

    def add(self, time, status):
        self.time += time
        if status == FAILED or self.status == FAILED:
            self.status = FAILED
        elif status == PASSED:
            self.status = PASSED


class TestClass:
    def __init__(self, name):
        self.name = name
        self.results = {}  # value -> Result of the class
        self.tests = {}  # test name -> {value -> Result}


def natural_key(text):
    return [int(part) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", text)]


def testcase_status(testcase):
    if testcase.find("failure") is not None or testcase.find("error") is not None:
        return FAILED
    if testcase.find("skipped") is not None:
        return SKIPPED
    return PASSED


def read_reports(directory, value, classes, counts):
    for report in sorted(directory.rglob("TEST-*.xml")):
        suite = ET.parse(report).getroot()
        suite_name = suite.get("name", report.stem.removeprefix("TEST-"))
        test_class = classes.setdefault(suite_name, TestClass(suite_name))
        class_result = test_class.results.setdefault(value, Result())
        statuses = []
        for testcase in suite.iter("testcase"):
            status = testcase_status(testcase)
            statuses.append(status)
            counts[status] += 1
            # A failure of @BeforeAll is reported as a test case without a name
            name = testcase.get("name") or "(class)"
            test_class.tests.setdefault(name, {}).setdefault(value, Result()) \
                .add(float(testcase.get("time") or 0), status)
        if FAILED in statuses or int(suite.get("errors") or 0) + int(suite.get("failures") or 0) > 0:
            class_status = FAILED
        elif PASSED in statuses:
            class_status = PASSED
        else:
            class_status = SKIPPED
        class_result.add(float(suite.get("time") or 0), class_status)


class Usage:
    """Resources used by the application containers of a matrix value, summed over the containers."""

    def __init__(self):
        self.containers = 0
        self.cpu_seconds = 0.0
        self.disk_read = self.disk_write = self.net_in = self.net_out = 0

    def add(self, container):
        self.containers += 1
        self.cpu_seconds += float(container.get("cpuSeconds") or 0)
        self.disk_read += int(container.get("diskReadBytes") or 0)
        self.disk_write += int(container.get("diskWriteBytes") or 0)
        self.net_in += int(container.get("netInBytes") or 0)
        self.net_out += int(container.get("netOutBytes") or 0)


def read_usage(directory):
    """Usage of the containers in container-usage.json files of the artifact, or None without such a file."""
    usage = None
    for file in sorted(directory.rglob("container-usage.json")):
        usage = usage or Usage()
        for container in json.loads(file.read_text()).get("containers", []):
            usage.add(container)
    return usage


def format_bytes(count):
    """Decimal units, as docker stats."""
    value = float(count)
    for unit in ("B", "kB", "MB", "GB"):
        if value < 1000 or unit == "GB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1000


def usage_rows(usages, values):
    """Rows of the usage of the application containers; none when no value has it."""
    if not usages:
        return []

    def row(title, text):
        return (f"| {title} | "
                + " | ".join(text(usages[value]) if value in usages else "" for value in values) + " |")

    return [
        row("**App containers**", lambda u: str(u.containers)),
        row("&emsp;CPU", lambda u: f"{u.cpu_seconds:.1f} s"),
        row("&emsp;Disk read / write",
            lambda u: f"{format_bytes(u.disk_read)} / {format_bytes(u.disk_write)}"),
        row("&emsp;Network in / out",
            lambda u: f"{format_bytes(u.net_in)} / {format_bytes(u.net_out)}"),
    ]


def parse_time(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def matrix_jobs(jobs, job_name):
    """Matrix value -> job, for the jobs named "<job_name> (<value>)"."""
    pattern = re.compile(re.escape(job_name) + r" \((.+)\)")
    return {match.group(1): job for job in jobs if (match := pattern.fullmatch(job["name"]))}


def format_duration(seconds):
    minutes, seconds = divmod(round(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m {seconds}s" if hours else f"{minutes}m {seconds}s"


def escape(text):
    return re.sub(r"([\\`*_{}\[\]<>|#])", r"\\\1", text)


def cell(result, mark_failed=True):
    if result is None or result.status == SKIPPED:
        return ""
    time = f"{result.time:.3f}"
    return f"🔴 {time}" if mark_failed and result.status == FAILED else time


def matrix_summary(title, job_name, artifact_prefix, reports_dir, jobs):
    jobs = matrix_jobs(jobs, job_name)
    classes = {}
    usages = {}  # value -> Usage
    counts = {PASSED: 0, FAILED: 0, SKIPPED: 0}
    values = set(jobs)
    for directory in reports_dir.glob(artifact_prefix + "*"):
        value = directory.name.removeprefix(artifact_prefix)
        values.add(value)
        read_reports(directory, value, classes, counts)
        if (usage := read_usage(directory)) is not None:
            usages[value] = usage
    values = sorted(values, key=natural_key)

    lines = [f"## {title}", ""]
    run = counts[PASSED] + counts[FAILED]
    icon = "❌" if counts[FAILED] else "✅"
    started = [parse_time(job["started_at"]) for job in jobs.values() if job.get("started_at")]
    completed = [parse_time(job["completed_at"]) for job in jobs.values() if job.get("completed_at")]
    wallclock = format_duration((max(completed) - min(started)).total_seconds()) if started and completed else "n/a"
    # HTML: Markdown is not rendered inside <summary>
    header = (f"{icon} <b>{run}</b> tests run, <b>{counts[FAILED]}</b> failed, <b>{counts[SKIPPED]}</b> skipped"
              f" · wallclock <b>{wallclock}</b>")
    if not classes:
        lines += [header, "", "No test reports.", ""]
        return lines

    # The table is collapsed; the blank line after <summary> makes GitHub render the Markdown table
    lines += [f"<details><summary>{header}</summary>", ""]
    lines.append("| Test | " + " | ".join(escape(value) for value in values) + " |")
    lines.append("|:---|" + "---:|" * len(values))
    lines += usage_rows(usages, values)
    for test_class in sorted(classes.values(), key=lambda c: natural_key(c.name)):
        simple_name = test_class.name.rsplit(".", 1)[-1]
        lines.append(f"| **{escape(simple_name)}** | "
                     + " | ".join(cell(test_class.results.get(value), mark_failed=False) for value in values) + " |")
        for test_name in sorted(test_class.tests, key=natural_key):
            results = test_class.tests[test_name]
            lines.append(f"| &emsp;{escape(test_name)} | "
                         + " | ".join(cell(results.get(value)) for value in values) + " |")
    lines += ["", "</details>", ""]
    return lines


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reports", type=Path, required=True)
    parser.add_argument("--jobs", type=Path)
    parser.add_argument("--matrix", nargs=3, action="append", required=True,
                        metavar=("TITLE", "JOB", "ARTIFACT_PREFIX"))
    args = parser.parse_args()

    jobs = []
    if args.jobs and args.jobs.exists():
        jobs = [json.loads(line) for line in args.jobs.read_text().splitlines() if line.strip()]
    lines = []
    for title, job_name, artifact_prefix in args.matrix:
        lines += matrix_summary(title, job_name, artifact_prefix, args.reports, jobs)
    sys.stdout.write("\n".join(lines))


if __name__ == "__main__":
    main()
