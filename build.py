#!/bin/env python3

import os
import sys
import json

REPO = "git@github.com:ymiseddy/eventide.git"
BADGE_DIR = "./badges"
PROJECT_CODE = "x339z"
BADGE_DEPLOY = f"seddy@www.seddy.com:seddydotcom/cicd/{PROJECT_CODE}"

colors = {
    "green": "green",
    "yellow": "yellow",
    "orange": "orange",
    "red": "red",
    "blue": "blue",
}


coverage_colors = [
    {"target": 90, "color":  colors["green"]},
    {"target": 80, "color":  colors["yellow"]},
    {"target": 70, "color":  colors["orange"]},
    {"target": 60, "color":  colors["red"]},
]


# Checkout source code based on branch.
def checkout(branch: str) -> None:

    if not os.path.exists(branch):
        result = os.system(f"git clone --single-branch --branch {branch} {REPO} {branch}")
        if result != 0:
            raise Exception("Failed checkout source code")

    os.chdir(branch)
    result = os.system("git pull")
    if result != 0:
        raise Exception("Failed to pull source code")


# Execute tests with tarpaulin
def test() -> None:
    result = os.system("cargo tarpaulin --skip-clean --target-dir ./target_cov --output-dir ./.metrics/ --out Json -- --test-threads=1")
    if result != 0:
        raise Exception("Tests failed.")


def slocc() -> None:
    result = os.system("tokei --output json > ./.metrics/tokei.json")
    if result != 0:
        raise Exception("Failed to count lines of code.")


def build() -> bool:
    result = os.system("cargo build --release")
    if result != 0:
        return False
    return True


class Stats:
    def __init__(self: "Stats", branch: str) -> None:
        self.branch: str = branch
        self.coverage: float = 0
        self.code: int = 0
        self.comments: int = 0


def get_stats() -> "Stats":
    stats = Stats(branch)

    with open('./.metrics/tokei.json') as json_file:
        data = json.load(json_file)
        stats.code = data["Total"]["code"]
        stats.comments = data["Total"]["comments"]

    with open('./.metrics/tarpaulin-report.json') as json_file:
        data = json.load(json_file)
        covered = 0
        coverable = 0
        for file in data["files"]:
            covered += file["covered"]
            coverable += file["coverable"]

        stats.coverage = round(covered / coverable * 100)
    return stats


def make_badge(filename: str, name: str, value: str, color: str) -> None:
    os.system(f'badge-maker -c "{color}" "{name}" "{value}" > {filename}')
    pass


def clear_badges() -> None:
    # create badge directory if not exist
    if not os.path.exists(BADGE_DIR):
        os.makedirs(BADGE_DIR)


def make_badges(stats: Stats) -> None:

    clear_badges()

    # create badge directory if not exist
    if not os.path.exists(BADGE_DIR):
        os.makedirs(BADGE_DIR)

    # clear existing files from BADGE_DIR
    for file in os.listdir(BADGE_DIR):
        os.remove(os.path.join(BADGE_DIR, file))

    # determine coverage color base on coverage
    coverage_color = colors["blue"]
    for coverage in coverage_colors:
        if stats.coverage >= coverage["target"]:
            coverage_color = coverage["color"]
            break

    make_badge(f"{BADGE_DIR}/coverage.svg", "coverage",
               f"{stats.coverage}%", coverage_color)

    make_badge(f"{BADGE_DIR}/code.svg", "code/comments",
               f"{stats.code}/{stats.comments}", colors["green"])

    make_badge(f"{BADGE_DIR}/build.svg", "build",
               "success", colors["green"])

    make_badge(f"{BADGE_DIR}/awesome.svg", "awesomeness",
               "100%", colors["blue"])


def make_failure_badges() -> None:
    make_badge(f"{BADGE_DIR}/coverage.svg", "coverage",
               "-", "#dddddd")

    make_badge(f"{BADGE_DIR}/code.svg", "code/comments",
               "-", "#dddddd")

    make_badge(f"{BADGE_DIR}/build.svg", "build",
               "fail", colors["red"])

    make_badge(f"{BADGE_DIR}/awesome.svg", "awesomeness",
               "100%", colors["blue"])


def deploy_badges() -> None:
    os.system(f"scp -r {BADGE_DIR}/* {BADGE_DEPLOY}/")


if __name__ == '__main__':
    # if we have a branch (argv1) use that branch otherwise, use develop
    branch = "develop"
    if len(sys.argv) > 1:
        branch = sys.argv[1]

    try:
        checkout(branch)
        test()
        slocc()
        build()
        stats = get_stats()
        make_badges(stats)
    except Exception:
        make_failure_badges()

    deploy_badges()
