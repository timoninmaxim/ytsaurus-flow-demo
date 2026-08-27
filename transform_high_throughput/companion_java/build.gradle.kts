plugins {
    java
}

repositories {
    mavenCentral()
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
}

dependencies {
    // Substituted with the sibling ytsaurus checkout's subprojects (see settings.gradle.kts);
    // the version is a placeholder the substitution overrides.
    implementation("tech.ytsaurus:flow-runner:1.0.0")
    // @Entity marks the per-key state POJO for the SDK's YSON serializer.
    implementation("javax.persistence:persistence-api:1.0")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.25.1")

    testImplementation("tech.ytsaurus:flow-core:1.0.0")
    testImplementation("tech.ytsaurus:flow-test-utils:1.0.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.10.2")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

// Collects the runnable classpath into one directory. The Flow runner discovers the companion
// jars to ship into the vanilla job from the directories on `java.library.path`, so both the
// launch script and the shipped classpath point at this directory.
tasks.register<Sync>("collectRuntime") {
    dependsOn(tasks.jar)
    from(tasks.jar)
    from(configurations.runtimeClasspath) {
        eachFile {
            // Several checkout subprojects produce identically named jars (proto.jar);
            // disambiguate by the producing project's directory.
            if (name == "proto.jar") {
                name = file.parentFile.parentFile.parentFile.parentFile.name + "-proto.jar"
            }
        }
    }
    into(layout.buildDirectory.dir("companion-libs"))
}
