load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_foreign_cc",
    sha256 = "2a4d07cd64b0719b39a7c12218a3e507672b82a97b98c6a89d38565894cf7c51",
    strip_prefix = "rules_foreign_cc-0.9.0",
    url = "https://github.com/bazelbuild/rules_foreign_cc/archive/refs/tags/0.9.0.tar.gz",
)

load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")

# This sets up some common toolchains for building targets. For more details, please see
# https://bazelbuild.github.io/rules_foreign_cc/0.9.0/flatten.html#rules_foreign_cc_dependencies
rules_foreign_cc_dependencies()

_ALL_CONTENT = """\
filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
"""

new_git_repository(
    name = "librseq_repo",
    remote = "https://github.com/compudj/librseq",
    branch = "master",
    build_file_content = _ALL_CONTENT,
)

git_repository(
    name = "gtest",
    remote = "https://github.com/google/googletest",
    branch = "main",
)

git_repository(
    name = "com_google_glog",
    remote = "https://github.com/google/glog.git",
    branch = "master",
)

git_repository(
    name = "com_github_gflags_gflags",
    remote = "https://github.com/gflags/gflags.git",
    branch = "master",
)

git_repository(
    name = "benchmark",
    remote = "https://github.com/google/benchmark",
    branch = "main",
)

#git_repository(
#    name = "HyperSharedPointer",
#    remote = "http://github.com/LoganEvans/HyperSharedPointer",
#    branch = "main",
#)
local_repository(
    name = "HyperSharedPointer",
    path = "/home/logan/Repos/HyperSharedPointer",
)
