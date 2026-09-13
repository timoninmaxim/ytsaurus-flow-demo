plugins {
    java
}

repositories {
    mavenCentral()
    // Flow test releases (X.Y.Z-SNAPSHOT) live in the Sonatype snapshot repository.
    maven {
        url = uri("https://central.sonatype.com/repository/maven-snapshots/")
        mavenContent { snapshotsOnly() }
    }
}

// The Flow release version; a test release is published as X.Y.Z-SNAPSHOT.
val flowVersion = providers.gradleProperty("flowVersion").getOrElse("0.1.0-SNAPSHOT")

tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
}

dependencies {
    implementation("tech.ytsaurus:flow-runner:$flowVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.25.1")

    testImplementation("tech.ytsaurus:flow-core:$flowVersion")
    testImplementation("tech.ytsaurus:flow-test-utils:$flowVersion")
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
    from(configurations.runtimeClasspath)
    into(layout.buildDirectory.dir("companion-libs"))
}
