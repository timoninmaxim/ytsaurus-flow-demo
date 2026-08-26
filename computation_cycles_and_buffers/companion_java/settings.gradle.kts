// Standalone Gradle build for the Java-companion variant of the computation_cycles_and_buffers
// scenario.
//
// The Flow Java SDK (tech.ytsaurus:flow-*) is not published to Maven Central yet, so this build
// composite-includes a source checkout of github.com/ytsaurus/ytsaurus (clone it next to this
// repo, or repoint with -PytsaurusRoot=/path/to/ytsaurus) and substitutes the SDK coordinates
// with the checkout's Gradle subprojects — the Java equivalent of the Go variant's go.mod
// `replace` directive.
rootProject.name = "computation-cycles-java"

val ytsaurusRoot = providers.gradleProperty("ytsaurusRoot").orNull
    ?: File(rootDir, "../../../ytsaurus").canonicalPath

includeBuild(ytsaurusRoot) {
    dependencySubstitution {
        substitute(module("tech.ytsaurus:flow-core")).using(project(":yt:java:flow:flow-core"))
        substitute(module("tech.ytsaurus:flow-server")).using(project(":yt:java:flow:flow-server"))
        substitute(module("tech.ytsaurus:flow-runner")).using(project(":yt:java:flow:flow-runner"))
        substitute(module("tech.ytsaurus:flow-test-utils")).using(project(":yt:java:flow:flow-test-utils"))
    }
}
