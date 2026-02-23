load("@rules_shell//shell:sh_test.bzl", "sh_test")

genrule(
    name = "greeting",
    outs = ["greeting.txt"],
    cmd = "echo 'Hello from besreceiver example' > $@",
)

sh_test(
    name = "pass_test",
    size = "small",
    srcs = ["pass_test.sh"],
)

sh_test(
    name = "fail_test",
    size = "small",
    srcs = ["fail_test.sh"],
    tags = ["manual"],
)
