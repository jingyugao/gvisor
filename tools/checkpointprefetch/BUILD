load("//tools:defs.bzl", "go_library", "go_test")

package(
    default_applicable_licenses = ["//:license"],
    licenses = ["notice"],
)

go_library(
    name = "checkpointprefetch",
    srcs = [
        "prefetch.go",
        "residency_unsafe.go",
    ],
    importpath = "gvisor.dev/gvisor/tools/checkpointprefetch",
    visibility = ["//visibility:public"],
    deps = ["@org_golang_x_sys//unix:go_default_library"],
)

go_test(
    name = "checkpointprefetch_test",
    srcs = ["prefetch_test.go"],
    embed = [":checkpointprefetch"],
    deps = ["@org_golang_x_sys//unix:go_default_library"],
)
